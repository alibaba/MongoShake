# Sharded integration test (orphan filter)

Reproducible end-to-end test for `full_sync.executor.filter.orphan_document`,
covering the three configurations from issue #978:

| Case | Source config | `orphan_document` | Expected dest |
|---|---|---|---|
| A | `mongo_urls` only (mongod direct) | `true` | 2 docs (orphan filtered) |
| B | `mongo_urls` + `mongo_s_url` (the #978 user's config) | `true` | `<2` docs **and** WARN line in log |
| C | `mongo_urls` only | `false` | baseline failure (dup-key panic) |

## What this catches

- **Regressions on the OrphanFilter range/compound boundary logic** —
  unit tests cover the chunk math, this exercises the full `Find → Filter
  → BulkWrite` pipeline against a real cluster.
- **The mongos-mode silent failure** (issue #978) — Case B asserts that
  the post-fix WARN log line is present, so future refactors can't
  reintroduce the silent fallthrough.
- **Baseline behaviour** — Case C ensures the dup-key path still raises;
  if a future change ever silently swallows it we want the test to fail.

## Running

Prerequisites: Docker + `pymongo` (`pip install pymongo`) + a built
collector binary in `../../../bin/`.

```bash
cd tests/integration/sharded
docker compose up -d
./setup_cluster.sh           # init replsets + addShard + stopBalancer
./inject_orphan.py           # craft orphan scenario
./verify_orphan_filter.py ../../../bin/collector.darwin   # or collector.linux
docker compose down -v       # tear down
```

The whole loop should complete in well under a minute on a laptop.

## Caveats

- Single-node replsets are used for each shard / configsvr / dest to keep
  the cluster small; do not use this layout for performance work.
- `setup_cluster.sh` uses `docker compose exec` (NOT `--network=host`)
  to run the mongo client inside the compose network. `--network=host`
  is a Linux-only Docker Desktop feature and was silently broken on
  macOS / Windows in earlier revisions of this scaffold.
- The shards are started with `--setParameter enableTestCommands=1`
  so `inject_orphan.py` can directly insert into a shardsvr without
  triggering `StaleConfig`. **This is a test-only knob; never enable
  it in production.**
- The script disables the balancer up front so the orphan injected into
  rs2 cannot be silently migrated/cleaned during the test window.
- This is a **scaffold**, not yet wired into CI. The collector binary is
  invoked once per case and waited on synchronously, which works for the
  full-sync mode used here but would need polling/timeout logic for
  long-running incremental tests.

## Where to take it next

- Wire into a `make integration-test` target that builds the collector
  and runs through the three cases on every push (or nightly).
- Extend the matrix to hashed shards (covers `ComputeHash` for
  `ObjectID`/`string`/`int64` end-to-end against the real cluster, not
  just unit tests).
- Add a parallel scenario that triggers the unmarshal-error path in
  `doc_executor.go:194` to confirm the syncer no longer panics on
  malformed payloads (today only checked by unit tests / code review).
