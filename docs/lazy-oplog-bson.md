# Lazy CRUD oplog payloads

The collector's oplog deserializer uses `oplog.ParseRaw` by default. Set
`incr_sync.lazy_oplog_parse = false` and restart to restore eager BSON decoding.
The switch does not affect change streams. It decodes metadata and
keeps ordinary `i/u/d` payloads (`o` and `o2`) as immutable slices of the owned
input BSON. Nested BSON containers are validated without decoding values, with a maximum
nesting depth of 200 (including oplog wrappers). Queueing an oplog therefore does not also retain a decoded document
tree. The reader already clones the cursor buffer before handing it downstream.

- Collection routing and namespace/time-series namespace transforms use metadata.
- ID routing decodes only `_id`, preserving the existing type and hash semantics.
- Unique-index collision checks and duplicate-key resolution read selected paths.
- Bulk, single, and command writers accept the raw insert/delete/replacement
  document or update predicate. V1 updates copy BSON only when removing `$v`.
- V2 diffs use the existing conversion at execution time; their temporary decoded
  tree is not stored back on the queued oplog. Time-series applyOps fallback keeps
  the original diff.
- DBRef transforms explicitly materialize the object before modifying it, so
  enabling `incr_sync.dbref` with namespace transforms disables the raw writer
  benefit. Decode failures return to the executor retry loop before writes or
  acknowledgement; failed decodes are never cached as partial objects.
- BSON serialization preserves raw payload bytes and modified metadata. JSON and
  extended JSON preserve their existing output formats; JSON decoding uses a copy
  so logging does not retain decoded trees in queues.

Commands, transactions, vectored inserts/applyOps, legacy `system.indexes` records,
and change-stream parsing keep their existing eager behavior in this change.
This is not an end-to-end zero-copy guarantee: the MongoDB driver still encodes
write commands, and JSON/DBRef/diff conversion can allocate substantial memory.

## Adding consumers

Use `ObjectValue()` / `QueryValue()` when passing documents to the driver or BSON
encoder. Use `ObjectKey()` / `LookupDocument()` for selected fields. Direct reads
of `Object`/`Query` on a raw CRUD log will see nil. Before mutating `Object`, call
`MaterializeObject()` and handle its error; it releases the raw fallback so a
subsequent nil assignment cannot resurrect an old payload. Raw slices must never
be modified, reused, or returned to a pool while their log is live. These mutable
log objects retain the existing single-owner execution model.

## Validation

Network-free tests capture the MongoDB driver's commands for all three writers,
cover raw namespace transforms, JSON/BSON round trips, ID/type parity, collision
keys, materialization, retry stability, and malformed input. Existing oplog,
transaction, filter, and namespace-transform tests remain regression coverage.

```sh
GOWORK=off go test -race ./oplog -skip '^TestConvertEvent2Oplog$'
GOWORK=off go test -race ./collector/filter ./collector/transform
GOWORK=off go test -race ./executor -run 'Test(Lazy|TransformLog|MergeToGroups|GetFieldValue|SplitDotted|ResolveConflictFilter|ParseDupKey)'
GOWORK=off go test ./collector -run '^TestDeserializer$'
GOWORK=off go test ./oplog -run '^$' -bench '^BenchmarkParseRaw$' -benchmem -count=3
```

The existing `TestConvertEvent2Oplog` integration test requires a live MongoDB
and is excluded only from the network-free commands above. For integration
coverage, provision a replica set and a sharded cluster, set
`MONGOSHAKE_TEST_URL` and `MONGOSHAKE_TEST_URL_SHARDING`, and run:

```sh
GOWORK=off go test -race ./oplog ./executor
```

`TestDeserializer` is listed without `-race` because its existing goroutine
lifecycle/global configuration and `recordLastFetchStats` updates have known
races also reproducible on develop. The isolated parser-switch and configuration
tests can be run with `go test -race ./collector ./collector/configure -run '^TestLazy'`. This change does not claim to fix the pre-existing collector races
or the timestamp-shift warning from `go vet ./executor`.

The benchmark compares eager decoding and raw parsing plus ID extraction on the
same document containing 4,096 nested fields. `B/op` and `allocs/op` describe
parser allocations, not process RSS or a production throughput guarantee.
