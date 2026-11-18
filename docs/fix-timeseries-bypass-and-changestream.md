# 修复：时序集合的 bypassDocumentValidation 报错与 change_stream 静默不同步（#1000）

## 需求描述

**现象：** 用户同步时序集合（`operation.gpsevents_20260X`）时，两种 `incr_sync.mongo_fetch_method` 都不可用：

- `change_stream` 模式下日志长期 `get=754333, filter=754314, write_success=0, tps=0`，但 **checkpoint 一直正常推进**，没有任何报错；
- 切到 `oplog` 模式则直接失败：
  `[CRITICAL] Replay-3, executor-3, oplog for namespace[operation.system.buckets.gpsevents_202608] op[i] failed. error type[mongo.BulkWriteException] error[bulk write exception: write errors: [bypassDocumentValidation is not permitted on timeseries collections]]`

源端 MongoDB 7.0.28，目标端阿里云 MongoDB 小版本 8.0.18（按内核映射为 MongoDB 7.0.x，baseline 约 7.0.39）。配置中 `incr_sync.executor.bypass_document_validation = true`、`incr_sync.shard_key = collection`、白名单为一批 `operation.gpsevents_*` / `dry_operation.gpsevents_*` 逻辑集合名。

**GitHub issue：** https://github.com/alibaba/MongoShake/issues/1000

这是**两个互相独立的问题**，分别修复。

## 根因分析

### 1. change_stream 根本收不到时序写入（上游限制，非本仓库缺陷）

MongoDB 官方文档明确说明：

> Time series collections do not support change streams because time series collections use an optimized storage format instead of tracking changes at the document level.
> —— https://www.mongodb.com/docs/manual/changestreams/

> Time series collections do not support change streams.
> —— https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/

对照实验（同一副本集、同一时间窗内，向一个时序集合和一个普通集合各写一批数据）证实了这一点，并穷尽了所有取数姿势：

| 取数方式 | 时序 bucket 写入是否可见 |
|---|---|
| `local.oplog.rs` | 可见（1 条 insert + 9 条 update 全在） |
| cluster-wide `$changeStream` | **0 条**（同窗内普通集合的 3 条 insert 正常返回，构成有效对照） |
| cluster-wide + `showExpandedEvents: true` | 只多出一个 `create` DDL，bucket 仍 0 条 |
| 直接 watch 时序逻辑集合 | 服务端直接拒绝：`Cannot run aggregation on timeseries with namespace ...` |

**7.0.37、7.0.43、8.0.4、8.2.12 四个版本表现完全一致**，所以没有可以等待的版本，也不存在回放链路上的可修之处。时序集合的增量同步只能走 `oplog` 模式。

用户日志里的数字与此吻合：`filter` 计的是**被丢弃**的条数（`batcher.go` 中 `AddFilter(1)` 均在过滤命中分支调用），`get=754333 / filter=754314` 意味着集群里其他 namespace 的事件被正常过滤，而白名单内的时序写入根本没有进入 change stream。checkpoint 照常推进是因为 change stream 本身仍在正常返回 resume token。

**本仓库真正的缺陷是"静默"**：这条链路没有任何报错，唯一的症状是 `write_success` 恒为 0，运维无从自查，最终被当成 MongoShake 的 bug 报上来。

### 2. bypassDocumentValidation 的拒绝是版本相关的服务端行为

用**真实 bucket 文档**（从真实时序集合抓取，保留原 `_id`）向目标端 `system.buckets.<ts collection>` 插入，只差 `bypassDocumentValidation` 一个字段：

| 服务端 | 不传该字段 | `false` | `true` |
|---|---|---|---|
| 7.0.37 | `n=1` 成功 | `n=1` 成功 | 成功（bypass 生效，错误前进到 `6067608`/`6781401` 一类桶内校验） |
| **7.0.43** | `n=1` 成功 | `n=1` 成功 | **`n=0`，`code=2 bypassDocumentValidation is not permitted on timeseries collections`** |
| 8.0.4 | `n=1` 成功 | `n=1` 成功 | 成功 |
| 8.2.12 | `n=1` 成功 | `n=1` 成功 | 成功 |

即该限制只在部分 7.0.x 小版本上存在。用户目标端是阿里云小版本 8.0.18（内核为 MongoDB 7.0.x，baseline 约 7.0.39），正落在有该限制的区间。

`incr_sync.executor.bypass_document_validation = true` 时，三个 writer 会把该选项无差别地带到**所有**写入上，包括 `system.buckets.*`，于是在这些小版本上时序集合的增量同步必然失败。

**踩坑记录（探测方法）：** `db.runCommand()` 对写错误**不抛异常**，错误在返回体的 `writeErrors` 里，`ok` 仍为 1。第一版探测只看了 `ok`/异常，得到"四个版本都允许"的错误结论；改为打印 `writeErrors` 后才看到真实的 `code=2`。另外，时序 bucket 的 `_id` **内嵌开桶时间戳**，服务端会校验它与 `control.min.<timeField>` 一致；给抓来的 bucket 换一个新 `_id` 会先撞上 `code=121 Mismatch between the embedded timestamp ... and the timestamp ... in 'control.min' field`，把真正要测的错误盖掉。

## 修复方案

### 1. bucket 写入不下发 bypassDocumentValidation

`executor/db_writer.go` 新增单一谓词，三个 writer 原先直接读配置的 **13 处**（bulk 3、command 5、single 5）全部改为调用它：

```go
func bypassDocumentValidationFor(collection string) bool {
	return conf.Options.IncrSyncBypassDocumentValidation &&
		!strings.HasPrefix(collection, utils.VarSystemBucketsPrefix)
}
```

收敛到一处后，后续新增写入站点不可能漏掉这条规则。用**前缀判断**而非 `IsTimeSeriesCollection()`：后者每次写入都要发一次 `listCollections`，而 bucket 命名空间必然带 `system.buckets.` 前缀。

`command` writer 的 `else` 分支（显式下发 `false`）**无需改动**——上表已证实 `false` 在所有版本都被接受，被拒的只有显式 `true`。

### 2. change_stream + 时序集合时显式告警

- `common/db_opertion.go` 新增 `ListTimeSeriesNamespaces(conn, filterFunc)`，按 `listCollections({type:"timeseries"})` 枚举，返回**逻辑名**（不含 `system.buckets.*`），`filterFunc` 语义与 `GetDbNamespace` 一致（true 保留）。
- `collector/coordinator/timeseries_change_stream.go` 新增 `timeSeriesUnsupportedByChangeStream(sources)`：仅当 `incr_sync.mongo_fetch_method = change_stream` 时工作，用与同步链路**同一个** `NamespaceFilter` 判定范围，因此不在同步范围内的时序集合不会产生噪音；跨 source 去重；名单上限 10 条（时序集合常按月分表，源端可能有上百个）。
- `warnChangeStreamCannotSyncTimeSeries` 只负责格式化与输出，在 `startOplogReplication` 开头调用一次。

把"判定"与"输出"分开，是为了让门控条件和 namespace 匹配可以在不断言日志文本的前提下被单测覆盖。

源端连不上或查询失败时**跳过而非失败**：这里只关系到一条告警，而真正为启动把关的检查在此之前已经校验过每一个连接。

实际输出：

```
[WARNING] incr_sync.mongo_fetch_method is change_stream, which cannot sync time-series collections:
MongoDB change streams do not surface time-series writes on any version, the bucket inserts and
updates only ever reach the oplog. These 1 in-scope collection(s) will NOT be synced incrementally:
cswarn_ts.gps. The run will still look healthy - the checkpoint keeps advancing while write_success
stays 0. Set incr_sync.mongo_fetch_method = oplog to sync them.
See https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/
```

## 影响范围

- **文件：** `executor/db_writer.go`、`executor/db_writer_bulk.go`、`executor/db_writer_command.go`、`executor/db_writer_single.go`、`common/db_opertion.go`、`collector/coordinator/timeseries_change_stream.go`、`collector/coordinator/incr.go`
- **未新增任何配置项**：1 复用 `incr_sync.executor.bypass_document_validation`，2 复用 `incr_sync.mongo_fetch_method` 与既有的 `filter.namespace.white/black`，`conf/collector.conf` 与 `configure.go` 无需同步。
- **对非时序集合零影响**：谓词只在 namespace 带 `system.buckets.` 前缀时改变行为。

### 已知边界

1. **CommandWriter 未做端到端覆盖。** 选中它需要 metadata `{g: <gid>}`，只有配置了 `filter.oplog.gids` 时 `buildMetadata` 才会产生；而它的 6 条命令路径都会把该字段原样拼进服务端命令，上游内核会以 `40415 BSON field 'insert.g' is an unknown field` 拒绝。**这是符合预期的**：gid 是阿里云内核适配的能力，只有阿里云实例才会开启 `filter.oplog.gids`，上游或自建内核本就不带该字段。因此其 5 处 bypass 站点改由版本无关的谓词单测覆盖，不做真机端到端断言。
2. **告警只覆盖增量阶段。** 全量同步对时序集合的支持见 `docs/fix-timeseries-full-sync.md`（#993），不受此限制影响；`sync_mode = all` 时全量正常、增量开始前告警。
3. **`ListTimeSeriesNamespaces` 依赖 `listCollections` 的 `type` 过滤**，该取值自 5.0 起存在；更低版本的源端匹配不到任何集合，返回空名单，不会误报也不会报错。

## 验证

- **1 的红绿对照：** 在 7.0.43（会拒绝的版本）上，`TestTimeSeriesBucketInsertIgnoresBypassDocumentValidation` 修复前 SingleWriter 与 BulkWriter 均报 `Code:2 bypassDocumentValidation is not permitted on timeseries collections`；把谓词改成无条件返回配置值后，两个测试重新变红；修复后在 **7.0.43 与 7.0.37 上均通过**，且断言的是"真实 bucket 落地并能从逻辑视图读回 5 条测量值"，而不是"服务端报错"——因此在不接受该限制的版本上同样有意义，不会退化成假绿。
- **2 的红绿对照：** 同时去掉 fetch-method 门控与白名单过滤后，`TestTimeSeriesUnsupportedByChangeStream` 的两个反向断言（oplog 模式应为空、范围外应为空）均变红；`TestWarnChangeStreamCannotSyncTimeSeriesIsLogged` 从日志文件读回告警，要求其中出现集合名、`incr_sync.mongo_fetch_method = oplog` 与官方文档链接。
- **接线覆盖：** `integration/changestream_timeseries_warn_test.go` 起真实 collector（`change_stream` 模式、白名单覆盖时序集合），断言告警出现在其日志中。删掉 `incr.go` 里那一行调用后该测试变红（1.51s 失败），补回后 0.97s 通过——这是唯一能守住"接线存在"的测试，单元测试直接调用的是被调函数本身。
- **回归：** `go test -p 1 ./...` 18 个包全绿；`go test -tags integration ./integration` 6 个用例全绿。
