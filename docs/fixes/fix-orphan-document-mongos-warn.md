# 修复：mongos 模式下 full_sync.executor.filter.orphan_document 静默失效

## 关联

- [Issue #978](https://github.com/alibaba/MongoShake/issues/978)
- 用户配置同时填了 `mongo_urls`、`mongo_cs_url`、`mongo_s_url`，开了
  `full_sync.executor.filter.orphan_document = true`，但孤儿没被过滤、
  全量阶段撞 E11000 dup key

## 根因

`cmd/collector/collector.go:138-151`：只要 `mongo_s_url` 非空，
`MongoS` 就被赋值，`RealSourceFullSync = [MongoS]`，**全量数据源走 mongos**，
`mongo_urls` 在全量阶段被忽略。

`coordinator/full.go:68`：

```go
if fromIsSharding && coordinator.MongoS == nil {
    shardingChunkMap, err = fetchChunkMap(...)   // ← MongoS != nil 时跳过
}
```

后续 `coordinator/full.go:162`：`shardingChunkMap == nil` →
`orphanFilter` 不创建 → `doc_executor.go:192` 的 `orphanFilter != nil`
判定不通过 → **过滤被静默跳过，没有任何日志告诉用户**。

## 为什么不能简单地"让 mongos 模式也支持 chunk map"

OrphanFilter 的语义是「文档的 shard key 是否落在**当前 shard 持有的**
chunk 范围内」。mongos 模式下 MongoShake 从 mongos 读出的文档不带
「来自哪个 shard」的元信息，把所有 shard 的 chunks 合并起来覆盖整个 key
space → 任何文档都会落在某个 chunk 内 → 永远不会被判为孤儿。**所以加
chunkMap 没用，唯一务实修复就是把"配置无效"的事实暴露出来**。

## 修复内容

### A. `coordinator/full.go`：mongos + orphan_document=true 时 WARN

```go
if fromIsSharding && coordinator.MongoS != nil &&
    conf.Options.FullSyncExecutorFilterOrphanDocument {
    l.Logger.Warnf("full_sync.executor.filter.orphan_document=true is set but "+
        "source is mongos (mongo_s_url configured); the orphan filter will NOT take "+
        "effect because docs read via mongos are not tagged with their origin shard. "+
        "To enable orphan filtering, drop mongo_s_url and use mongo_urls (mongod direct) "+
        "plus mongo_cs_url, or run cleanupOrphaned on the source cluster before full sync.")
}
```

### B. `doc_executor.go`：unmarshal 失败后 continue

修复前：

```go
if err := bson.Unmarshal(*doc, &docData); err != nil {
    l.Logger.Errorf(...)
    // ⚠ 没 continue，空 docData 接着进 Filter
}
if exec.syncer.orphanFilter.Filter(docData, ns) { ... }
```

空 `docData` 进 `Filter` 后，`oplog.GetKey(docD, keyName)` 返回 nil，
触发 `OrphanFilter.Filter` 第 55 行的 `Panicf("OrphanFilter find no
shard key[%v] in doc %v")` 直接挂进程。

修复后用 `if err != nil ... else if Filter(...) ...` 把两条路径分开，
unmarshal 失败时跳过 orphan check 让损坏 payload 在下游 BulkWrite 给出
结构化错误，而不是 panic 整个 syncer。

### C. 用户侧 workaround（不改代码也能让 #978 用户立刻可用）

注释掉 `mongo_s_url`，只留 `mongo_urls`（mongod 直连）+ `mongo_cs_url`：

```
mongo_urls   = mongodb://...;mongodb://...;mongodb://...;mongodb://...
mongo_cs_url = mongodb://...
# mongo_s_url = mongodb://...   <-- 注释掉
```

## 测试

### 单元测试 — 新增 `collector/filter/orphan_filter_test.go`

| 测试 | 覆盖 |
|---|---|
| `TestOrphanFilter_NilChunkMap` | chunkMap 为 nil → 不过滤 |
| `TestOrphanFilter_NamespaceMissing` | namespace 不在 chunkMap → 不过滤 |
| `TestOrphanFilter_RangeSingleKey` | 单 key 9 个边界（in/out/min/max 包含与排他） |
| `TestOrphanFilter_RangeCompoundKey` | **联合 key 10 个边界**（即 #978 用户场景的 shard key 形态） |
| `TestOrphanFilter_HashedAllTypes` | hashed 分片 ObjectID/string/int64 三种 key type |
| `TestOrphanFilter_HashedNoChunks` | 空 Chunks → 任何文档都是孤儿 |
| `TestOrphanFilter_HashedPrecisionBug` | **SKIP，记录 latent bug**（详见下一节） |
| `TestOrphanFilter_HashedUnsupportedTypePanics` | bool 等类型 panic 行为 pin 住 |

### 集成测试雏形 — 新增 `tests/integration/sharded/`

`docker-compose.yml` + `setup_cluster.sh` + `inject_orphan.py` +
`verify_orphan_filter.py`，覆盖三种配置：

| Case | Source | `orphan_document` | 期望 |
|---|---|---|---|
| A | `mongo_urls` only | `true` | 目标端正好 2 docs（孤儿过滤生效） |
| B | `mongo_urls` + `mongo_s_url`（#978 配置） | `true` | 目标端 2 docs（mongos 路由绕过 orphan）, rc=0, **且** 日志含 WARN line |
| C | `mongo_urls` only | `false` | baseline 失败（dup-key panic） |

详见 `tests/integration/sharded/README.md`。雏形阶段，**未接 CI**，
跑一遍要 docker + pymongo，仅作为本次修复的回归基线。

## 顺手发现的几个 latent bug（不在本次修复范围）

记录在此供 follow-up，建议另开 issue：

### latent bug 1：`OrphanFilter` chunkLt/chunkGt 对 int64 hashed bound 丢精度

`collector/filter/orphan_filter.go:219` `getBsonType`:

```go
case int64:
    return BsonTypeNumber, float64(rx)   // ← 把 int64 cast 成 float64
```

后续 `chunkLt/chunkGt` 都是 `float64` 比较。但 hashed 分片的 chunk
min/max 是 int64 hash 值，超出 `[-2^53, 2^53]` 范围会丢精度。
`hashed - 1`、`hashed`、`hashed + 1` 三个相邻 int64 在 float64 表示下
可能折叠成同一个值，造成边界判定错误。

`TestOrphanFilter_HashedPrecisionBug` 用 `t.Skip()` 显式记录此问题，
便于将来扩展时不漏掉。

### latent bug 2：`ComputeHash` 类型支持不完整

`collector/filter/orphan_filter.go:94-135`：当前仅支持
`string` / `int` / `int32` / `int64` / `float64` / `ObjectID`。

对比 MongoDB 内核 `hasher.cpp`（**4.0 与 8.0 的实现完全一致**：
MD5(seed‖canonicalType‖value)，所有数字类型经 `safeNumberLongForHash`
归一化为 int64 再 hash），仍缺：

- `NumberDecimal`（4.4+ 可作 hashed shard key）
- `Date`、`Bool`、`bsonTimestamp`
- `BinData`、`null`、`Symbol`

不支持类型走 default 分支 `Panicf`。MongoShake 主流场景（hashed `_id`
为 ObjectID 或 string）已覆盖，扩展见单独 issue。

`float64` 当前用 `uint64(rd3)` 直接 cast 处理负数 / NaN / Inf 与内核
`safeNumberLongForHash` 行为不完全一致，但实际 hashed shard key 用浮点
极罕见，遗留待修。

### latent bug 3：`mongos` 模式下 OrphanFilter 架构上不可用

如「为什么不能简单地"让 mongos 模式也支持 chunk map"」节所述，需要
重新设计才能在 mongos 模式下做孤儿过滤（例如：让 mongos 把
`$shardName` 注入 cursor metadata，或者让 MongoShake 切换到从每个
shard 直连的辅助通道）。本次仅以 WARN 提示用户当前限制。

## 改动文件清单

| 文件 | 类型 | 改动 |
|---|---|---|
| `collector/coordinator/full.go` | 修改 | mongos + orphan_document=true 时 WARN |
| `collector/docsyncer/doc_executor.go` | 修改 | unmarshal 失败时 skip orphan check |
| `collector/filter/orphan_filter_test.go` | 新增 | 8 个 OrphanFilter 单元测试 |
| `tests/integration/sharded/docker-compose.yml` | 新增 | sharded cluster 集成测试拓扑 |
| `tests/integration/sharded/setup_cluster.sh` | 新增 | replset / addShard / stopBalancer |
| `tests/integration/sharded/inject_orphan.py` | 新增 | 构造孤儿场景 |
| `tests/integration/sharded/verify_orphan_filter.py` | 新增 | 三种配置自动化校验 |
| `tests/integration/sharded/README.md` | 新增 | 集成测试使用说明 |
| `docs/fix-orphan-document-mongos-warn.md` | 新增 | 本 spec |
