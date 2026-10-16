# Fix: Pin checkpoint below in-memory buffered transactions

## Problem

Distributed (prepared) and large (partialTxn) transactions can be silently
lost across a MongoShake restart.

Timeline that loses data:

```
t1  prepare        ts=100   buffered in memory only (TxnBuffer), NOT dispatched
t2  normal DML     ts=101   dispatched, acked  -> checkpoint persists 101
t3  crash                   TxnBuffer content is gone
t4  restart                 oplog query is {ts: {$gt: 101}} -> prepare@100 never re-fetched
t5  commitTransaction ts=102 arrives for an unknown txn
```

Before this fix, step t5 did **not** fail: `TxnBuffer.AddOp` created a brand
new empty state for the commit, `GetTxnStream` streamed zero ops without any
error, and the transaction completed as an "empty" transaction. Silent data
loss.

A second window exists even without t2: during commit handling the batcher
calls `PurgeTxn` before the inner ops (staged in `barrierOplogs`, memory only)
are dispatched and acked. A crash inside this window loses the same data.

## Root cause

1. `prepare`/`partialTxn` oplogs live only in memory (`oplog/txn_buffer.go`)
   and are never dispatched to workers (`collector/batcher.go`
   `handleTransaction` returns early for non-final entries).
2. `calculateWorkerLowestCheckpoint` (`collector/checkpoint.go`) derives the
   checkpoint purely from worker ack/unack and never consults the transaction
   buffer. The upstream `TxnBuffer.OldestOpTime()` helper existed but was
   never wired into production code.
3. The restart fetch uses a strict `$gt checkpoint` query
   (`collector/reader/oplog_reader.go`), so an over-advanced checkpoint
   permanently skips the buffered entries.

## Design

### 1. Transaction pins (collector/batcher.go)

`Batcher` keeps `txnPins map[oplog.TxnID]*txnPin`:

- `pinTxn` — registered on the first `AddOp` of a transaction;
  `startTs` = packed timestamp of that first buffered entry.
- `unpinTxn` — abort drops the pin immediately (nothing to apply).
- `stageTxnPinRelease` — on commit, after `PurgeTxn`, the pin records
  `releaseTs` = commit timestamp (inner ops carry timestamps up to the commit
  ts, see `ExtractInnerOps`). The pin intentionally survives `PurgeTxn`.
- `OldestUnackedTxnPinTs(ackedTs)` — drops every pin whose `releaseTs > 0 &&
  ackedTs >= releaseTs`, then returns the smallest remaining `startTs`
  (0 when none). This covers both loss windows: buffering and
  staged-but-not-yet-acked inner ops.

### 2. Checkpoint clamp (collector/checkpoint.go)

`clampCheckpointByTxnPin(candidate, ackTs)` caps the value that
`checkpoint()` persists at `oldestPin - 1`. Restarting with a clamped
checkpoint re-fetches the pinned transaction's entries via `$gt`.

`ackTs` is the current worker ack progress (used only to release completed
pins); in the `inputTs > 0` path it is recomputed best-effort from
`calculateWorkerLowestCheckpoint`.

### 3. Two-track barrier wait (collector/syncer.go)

Naively clamping the checkpoint deadlocks the barrier flow: the commit round
waits in `checkCheckpointUpdate(true, newestTs)` for the *persisted*
checkpoint to reach `newestTs`, but the pin holds it below `newestTs` until
the inner ops are acked — which only happens in the next round.

`checkCheckpointUpdate` therefore also checks worker ack progress
(`calculateWorkerLowestCheckpoint() >= newestTs`). Worker ack is the real
barrier condition; the persisted checkpoint was only ever a proxy for it.

### 4. Orphan-commit defense (oplog/txn_buffer.go)

`txnState.sawData` records whether any data-carrying entry was ever ingested.
`GetTxnStream` now fails loudly when the only entry a state ever received is
its final commit — i.e. the prepare/partialTxn oplogs were lost. Legitimately
empty prepared transactions (SERVER-50769) still work because their prepare
entry is a data entry.

## Invariants

- Persisted checkpoint < startTs of every transaction whose data is not yet
  fully acked; restart can always re-fetch the buffered entries.
- Pins are released exactly once: abort, or worker ack reaching the commit ts.
- Barrier ordering is unchanged: DDL/txn inner ops still wait for all prior
  DML acks; only the wait condition gained a second (equivalent) track.

## Tests

- `oplog/txn_buffer_orphan_test.go`
  - `TestGetTxnStreamOrphanCommitReturnsError` — orphan commit errors instead
    of streaming an empty transaction.
  - `TestGetTxnStreamEmptyPreparedTxnStillWorks` — empty-but-known prepared
    txn still streams cleanly.
- `collector/batcher_txn_pin_test.go`
  - `TestTxnPinLifecycle` — pin set at prepare, held across normal-op acks and
    commit staging, released at inner-op ack; abort releases; overlapping
    transactions tracked independently.
  - `TestClampCheckpointByTxnPin` — clamp math and release integration.
- `collector/syncer_txn_pin_test.go`
  - `TestCheckCheckpointUpdateUsesWorkerAckWhenCheckpointPinned` — reproduces
    the pin/barrier deadlock (fails by 3s timeout without the fix).

## Integration test

`integration/txn_pin_test.go` (build tag `integration`) reproduces the loss
window end-to-end with a real 2-shard cluster plus a standalone target:

1. a cross-shard transaction starts committing; shard1 writes its prepare
   oplog entry;
2. `docker pause` on shard2 freezes 2PC with shard1 stuck in prepared state;
3. normal DML on shard1 is synced and acked;
4. assertion 1: shard1's persisted checkpoint stays strictly below the
   prepare ts (fails on unpatched builds: baseline checkpoint reaches the
   prepare ts exactly);
5. collector is killed with SIGKILL and restarted;
6. shard2 is unpaused, 2PC commits;
7. assertion 2: the transaction documents from both shards arrive exactly
   once on the target.

Observed results: patched build pins the checkpoint at `prepare-1` and all
data survives; develop build advances the checkpoint to the prepare ts and
fails assertion 1. Run with:

```
go test -tags integration ./integration -run TestTxnCheckpointPin -v -timeout 10m
```

All new tests were written first and observed failing (TDD). Regression:
`go test ./collector ./oplog` green (pre-existing environment-dependent
failures in `collector/{ckpt,coordinator,docsyncer,reader}` and
`oplog.TestConvertEvent2Oplog` reproduce identically on the unmodified
baseline); `go vet` clean except a pre-existing `transform.go` warning;
`go build ./cmd/collector ./cmd/receiver` OK.
