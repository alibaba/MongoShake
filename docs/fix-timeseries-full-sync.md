# 修复：全量同步对时序集合的适配（#993）

## 需求描述

**现象：** 全量同步时序集合时源端出现 `(OutOfDiskSpace) PlanExecutor error during aggregation :: Available space ... less than the required minimum of 524288000 bytes`，并伴随大量 `E11000 duplicate key error ... index:_id_`。关联 #992、#897、#784。

**GitHub issue：** https://github.com/alibaba/MongoShake/issues/993

## 根因分析

全量链路把时序集合当普通集合处理，共四处不适配。以下均在 MongoDB 7.0.37 实测确认。

### 1. #897 的修复条件是反的（本轮新发现，issue 原文未提及）

`doc_reader.go` 的 `ensureNetwork` 里，#784 的 `noCursorTimeout` 门控和 #897 后加的 `SetHint(nil)` 挤在同一个 `IsTimeSeriesCollection(...) == false` 分支里：

```go
if reader.client.IsTimeSeriesCollection(db, coll) == false {
    findOptions.SetNoCursorTimeout(true)   // #784：正确，TS 不支持
    findOptions.SetHint(nil)               // #897：注释写"TS 不能用 hint"，但只在非 TS 时执行
}
```

`noCursorTimeout` 的门控是对的（`0b688f1`），但 `12baf3c` 把 `SetHint(nil)` 加进了同一个分支，方向正好相反。后果是双向的：

- **时序集合保留了 `hint({_id:1})`** —— 逻辑视图没有 `_id` 索引，服务端直接拒绝：
  `(BadValue) error processing query: ns=<db>.system.buckets.<coll> Tree: $and`
  **即时序集合的全量同步在 develop 上是直接 panic 失败的**，不只是 issue 描述的落盘问题。
- **普通集合反而丢掉了 `_id` hint**（第 398 行设的 hint 被第 406 行清掉，成了死代码）。

`IsTimeSeriesCollection` 靠 `collStats` 返回里有没有 `timeseries` 字段判断，实测**对逻辑名和 `system.buckets.*` 都返回 true**；而 `GetListCollectionQueryCondition` 显式包含 `type: "timeseries"`、`GetDbNamespace` 又跳过 `system.*` 前缀，所以全量读的确实是逻辑视图，必然踩中上面第一条。

### 2. 强制 `_id` 排序导致源端阻塞排序落盘

`SetSort({_id:1})` 对所有集合无条件生效。时序逻辑视图没有 `_id` 索引，服务端把 find 改写为对 `system.buckets` 的解包聚合，再对解包后的全部测量值做阻塞排序，触发 `allowDiskUse` 落盘。集合越大、并发越高越容易跌破 MongoDB 500MB 空闲底线。

### 3. 分片读依赖索引，时序集合不适用

`DocumentSplitter.Run` 在并发读时会先 `checkIndexKeyTypeConsistency`（对 `parallel_index` 做两次 `FindOne` + `SetSort`，在 TS 上同样是阻塞排序），再执行 `splitVector`（TS 无 `_id` 索引，必然失败后回退单 reader）。阻塞排序的开销在回退前就已经付掉了。

### 4. 目标端不按时序属性建表

全量写入直接对逻辑名 `BulkWrite`，没有任何携带 `timeseries:{...}` 的建表动作。对 oplog 窗口之前就已存在的时序集合（`sync_mode=all`），没有 create DDL 可回放，目标端首次 insert 会自动建成普通集合，丢失时序属性并带上 `_id_` 唯一索引 —— 这正是 E11000 的来源。

实测佐证：对不存在的集合执行 `createIndex` 会**隐式建出普通集合**（`type=collection`），所以建表必须排在索引同步之前；而在已存在的时序集合上重建同规格索引是**幂等**的，所以建表之后再走索引同步没有问题。

### 5. dup 兜底对时序集合无效

`doc_executor.go` 在 `insert_on_dup_update=true` 时把重复 insert 转成 `UpdateOne({_id}, {$set: doc})`。实测时序集合直接拒绝：`Cannot perform a non-multi update on a time-series collection`。

## 修复方案

### 1. `ensureNetwork` 按时序与否分流

```go
if !reader.client.IsTimeSeriesCollection(reader.ns.Database, reader.ns.Collection) {
    findOptions.SetSort(map[string]interface{}{"_id": 1})
    findOptions.SetNoCursorTimeout(true)   // #451
}
// 时序分支：不排序、不 hint、不设 noCursorTimeout（#897 #993 #784）
```

顺带消掉了原先"设 hint 再清 hint"的死代码。**普通集合的行为与改动前完全一致**（hint 本来就被清成 nil），只是时序集合不再拿到非法 hint、也不再排序。

### 2. 时序集合强制单片读

`DocumentSplitter` 在 `collStats` 里顺带取出 `timeseries` 字段缓存为 `isTimeSeries`（不额外增加一次往返），`Run` 在并发分支之前直接回退单 reader，跳过 min/max 探测和 `splitVector`。

### 3. 全量前置建表

新增 `StartCreateDestTimeSeriesCollection`，在 `full.go` 中排在 `StartDropDestCollection` 之后、索引同步与数据同步之前。按源端 `listCollections({type:"timeseries"})` 的 `options` **原样透传**建表（含 `timeField`/`metaField`/`granularity`/派生的 `bucketMaxSpanSeconds`/`expireAfterSeconds`），实测往返完全一致。

目标端已存在同名集合时分两种情况：已是时序集合则保留；是普通集合则打 **ERROR** 日志说明时序属性无法就地补加、会带 `_id_` 唯一索引导致重跑 E11000，并给出处理建议（删表重跑或开 `full_sync.collection_exist_drop`），但不中断同步——数据仍能同步进去，硬失败会是 `collection_exist_drop=false` 用户的行为回退。

### 4. 时序目标跳过 dup 兜底

命中重复键且目标是时序集合时，直接返回明确错误（提示清理目标端并重启全量阶段），而不是发一个必然失败的 `UpdateOne`。

## 影响范围

- **文件：** `collector/docsyncer/doc_reader.go`、`collector/docsyncer/doc_syncer.go`、`collector/docsyncer/doc_executor.go`、`collector/coordinator/full.go`
- **集成测试：** `integration/timeseries_full_sync_test.go`（`TestTimeSeriesFullSync`）——源端一个时序集合（12 条测量值、带 `expireAfterSeconds`）+ 一个普通集合作对照，跑真实 `sync_mode=all`，断言目标端类型为 `timeseries`、options 逐字段一致、索引集合一致且无 `_id_`、测量值全部到位、普通集合精确一致（守住非时序路径未被 reader 改动带坏）、日志无 `E11000`/`BadValue`/`non-multi update`。
- **红绿对照：** develop 基线上该测试 3s 内失败，collector panic 于 `(BadValue) ... ns=tsfullsync.system.buckets.metrics`；修复后 1.9s 通过。

### 已知边界

1. **`_id` 不保留（设计如此）**。实测把源端合成 `_id` 的测量值插入目标时序集合会成功，但 `_id` 由目标端重新分桶生成、与源端不同（源 `...88ba` vs 目标 `...88bf`）。因此任何逐字节比对都必须排除 `_id`；字段顺序同样会变（解包顺序由目标端决定），比对需按语义规范化而非 BSON 字节。
2. **目标端版本低于源端时可能建表失败**：透传的 `timeseries` 子字段（如 `bucketMaxSpanSeconds`）旧版本不认。这是真实的版本不兼容，选择带完整 options 的报错失败，而不是静默降级建表。
3. **`collection_exist_drop=false` 且目标端已有数据时重跑会产生重复测量值**：时序集合没有 `_id_` 唯一索引可拦，重跑前应清理目标端。
4. **未实现 issue Phase 2 的 `system.buckets.*` 直读直写**：那属于架构级改造，issue 原文也只要求"评估"。当前实现走逻辑视图，忠实复制测量值但不保证 bucket 物理布局与源端一致。

## 验证建议（issue 原文三项的落实情况）

- 大体量时序集合全量的落盘峰值对比：本轮未做（需要 200GB 级数据），但已消除阻塞排序这一落盘来源，且时序集合不再并发分片读。
- 预存时序集合 `sync_mode=all`：已由集成测试覆盖（目标端 `type=timeseries`、无 `_id_` 索引、无 E11000）。
- 崩溃重启重跑：`insert_on_dup_update` 兜底对时序目标改为明确报错，不再发出必然失败的 update；见已知边界第 3 条。
