# 修复：applyOps 回放携带源端集合 uuid，导致时序 bucket 更新与 system.views 插入永远到不了目标端（#1009 / #1010）

## 需求描述

**现象：** executor 中三处 `applyOps` 回放把**源端**集合的 uuid（`ui`）一并转发，而写入的目标命名空间属于**目标端**自行创建、uuid 各不相同的集合。`applyOps` 要求两者一致，因此每一次这样的回放都被服务端拒绝：

```
code 3318200, Location3318200:
Namespace 'db.system.views' and UUID 'd548a9c6-8cf7-4bec-9526-8cfe73923efe'
point to different collections
```

命令响应本身只说 `(UnknownError) applyOps had one or more errors applying ops`，**真实原因只在目标端 mongod 日志里**（`applyOps error applying`），这是排查成本的主要来源。

**与版本无关**：7.0.37 和 8.2.12 均可复现。

**GitHub issue：**
- https://github.com/alibaba/MongoShake/issues/1010 （uuid 转发本身）
- https://github.com/alibaba/MongoShake/issues/1009 （暴露它的测试覆盖缺口）

## 根因分析

### 受影响的三条路径

| 位置 | 回放内容 | 后果 |
|---|---|---|
| `executor/db_writer.go` `replayUpdateViaApplyOps` | `$v:2` diff 中含列存 `b` 段的时序 bucket 更新 | MongoDB 8.x 上**任何**扩展现有 bucket 的插入都会产生一条，因此 8.x 时序集合的增量同步完全无法推进 |
| `executor/db_writer_single.go` | `system.views` 插入 | 视图定义永远落不到目标端 |
| `executor/db_writer_bulk.go` | `system.views` 插入 | 同上 |

三个 writer（`SingleWriter`、`BulkWriter`、`CommandWriter`）都会走到 bucket 路径，因为它们都在同一个 `strings.HasPrefix(collection, utils.VarSystemBucketsPrefix)` 判断之后调用 `replayUpdateViaApplyOps`。

后两条合起来的净效果是：**一个复制到目标端的时序集合只有 bucket、没有覆盖其上的逻辑视图**，从用户视角看集合是空的。且这一条**不限于 8.x**，7.0.37 同样复现。

### `ui` 在所有抓取路径上都会被填充

`oplog.ParseRaw` 的 metadata 白名单、非 lazy 的 `bson.Unmarshal`、`oplog/change_stream_event.go` 中的七处赋值、以及 `oplog/txn_buffer.go`，下游没有任何一处清理它。所以这不是某种特定配置下才出现的问题。

### 仓库其余部分其实早已在剥离 `ui`

`utils.ApplyOpsFilter` 对 `"ui"` 返回 true，并在 `db_writer.go` 的两处生效；嵌套 op 走 `oplog.RemoveFiled(doc, uuidMark)`，其中 `uuidMark = "ui"`。**上述三处只是没有使用这套既有机制。**

### 为什么测试没有发现（#1009）

`TestTimeSeriesIntegration` 有一个 `system.views` 用例，但它手工构造 `PartialLog` 字面量、**没有设置 `UI`**。`ui` 缺失时 `applyOps` 恰好能成功，于是生产链路是坏的、测试却是绿的。

它的 `sdata.b` 用例同样无效：二进制块是凭空编造的字节，任何服务端都会拒绝，而断言只检查

```go
assert.NotContains(t, err.Error(), "unknow Key", ...)
```

即"错误不是来自 MongoShake 自己的解析器"。**任何**服务端拒绝都满足这个断言，所以由 `ui` 引起的拒绝与"预期结果"看起来一模一样。各版本的实际拒绝原因：

| 服务端 | 对伪造 `sdata.b` 的反应 |
|---|---|
| 7.0.x | `4770503 Unexpected section: b in document diff` —— `b` 根本不是 7.0 的 diff 段（`kBinarySectionFieldName` 只从 8.0 起存在，5.0/6.0/7.0 内核树中均无） |
| 8.2.12 | `6067608 Invalid BSON Column encoding` —— 段能解析，但字节不是合法的 BSON Column |

而 `b` 在 8.x 上并不冷门：8.0 的 `timeseries_write_util.cpp` 中，`b` 段承载一个写批次里**每一个被更新的既有列**，所以任何扩展现有 bucket 的插入都会产生一条。已在真实 8.2.12 副本集上确认：两批写入同一个 bucket，得到的 oplog 条目为 `op:"u"`、`o.$v = 2`、`o.diff` 键为 `scontrol, sdata`，且 **`sdata` 的键恰好是 `b`**；bucket 为 `control.version = 2`，每个 `data` 列以 `BinData(subtype 7)`（`BinDataType::Column`）存储。

### 排查方法论上的教训

最初的判断是"8.x 上 applyOps 回退路径是好的"，依据是手工执行 `applyOps` 回放真实 8.x bucket oplog 条目**成功**了。但为了让那条命令跑起来，事先从捕获的条目上剥掉了一组字段：`_id`、`ts`、`t`、`v`、`wall`、`prevOpTime`、`clusterTime`、**`ui`**、`lsid`、`txnNumber`、`stmtId`，并把它们当作记账信息。`ui` 不是：它正是导致失败的那一个。

**手工简化过的命令无法证明生产链路可用。**剥字段用于定位机制是可以的，但"能不能work"的结论必须来自真实代码路径，否则简化本身会悄悄变成结论。改用真实链路（真实 oplog 条目经 `oplog.ParseRaw` 进入 writer）后立刻失败。

## 修复方案

在 `executor/db_writer.go` 中显式构造 applyOps 文档，三处回放统一使用：

```go
func applyOpsDoc(pLog *oplog.PartialLog) bson.D {
	doc := bson.D{
		{Key: "op", Value: pLog.Operation},
		{Key: "ns", Value: pLog.Namespace},
		{Key: "o", Value: pLog.ObjectValue()},
	}
	// 仅在非空时补 o2
	...
	return doc
}
```

只发出 `op`、`ns`、`o` 和非空的 `o2`，`ui` 自然不在其中。

**不能用 `PartialLog.Dump` 代替**：它会序列化每一个带 tag 的字段，包括 `lsid`、`prevOpTime` 这类空的 `bson.Raw`，驱动会以 `cannot marshal type primitive.D to a BSON Document: couldn't read length from src` 拒绝。

`RunCommand` 的 `collMod`/`drop`/`dropIndex`/`dropIndexes` 分支是**故意**带上 `ui` 的（注释为 `ensure every shard collection's uuid is same`，门控在 `Subtype == 3 || 4`），与数据 op 情形不同，本次不动。

## 影响范围

- **文件：** `executor/db_writer.go`、`executor/db_writer_single.go`、`executor/db_writer_bulk.go`、`executor/db_writer_test.go`、`executor/timeseries_bucket_replay_test.go`（新增）、`integration/lazy_oplog_e2e_test.go`
- **测试：**
  - `TestTimeSeriesBucketBinaryColumnReplay`（8.0+ 副本集）——两批写入同一个 bucket，从 `local.oplog.rs` 取出 bucket insert 与携带 `b` 的 update，经 **`oplog.ParseRaw`**（生产入口，因此 lazy decode 路径也被覆盖）解码、改写命名空间后通过 writer 回放到独立的目标时序集合，同时比对 bucket 文档与**从逻辑视图读回的测量值**（两侧各 5 条，含 `_id`）。并正向断言至少有一条捕获到的 op 确实带 `diff.sdata.b`，使其无法退化成只回放了 bucket insert。
  - `TestSystemViewsApplyOpsReplay`（任意支持时序的副本集）——捕获真实 `system.views` 插入并回放，断言视图定义到达目标端。**若捕获到的 op 不带 `ui` 则测试失败**，因此不会退化成当初漏掉这个问题的手工 fixture。
  - 保留但收紧了 `TestTimeSeriesIntegration` 中两个伪造 `sdata.b` 用例：由"接受 nil 错误"改为**要求必须有错误**，并注明它们覆盖的只是**路由**（`ConvertV2Oplog` 是否拒绝 `b` 并把 op 交给 applyOps），结果正确性由上面的新测试负责。不删是因为 7.x 上新测试会 skip，删掉就丢了路由覆盖。
- **两个新测试都会清理自己的数据库。**它们写入的是共享源端，而遗留的视图定义会成为悬空定义，导致服务端对**整个数据库**的 `listCollections` 都拒绝。`TestLazyOplogEndToEnd` 同时改为清理自己的 checkpoint：已存储的 checkpoint 优先于 `checkpoint.start_position`，否则它会从旧位置续读、回放无关历史。

### 已知边界

1. **`b` 段仅自 8.0 起存在。**7.x 上 `ConvertV2Oplog` 返回 `unknow Key[b]` 并走 applyOps 回退，但 7.x 的时序更新本来就不产生 `b`，所以 7.x 上该回退只会被伪造输入触发。
2. **DDL 路径仍转发 `ui`**（见上），未验证目标端 uuid 不一致时 `collMod`/`drop` 等是否也会撞上 3318200，值得单独评估。
3. **未做完整 collector 端到端验证**：真实 8.x `b` diff 经完整 collector 运行的链路，是"服务端接受此类 diff"与"回退分支被执行"两半分别验证的，没有串成一条。
4. **时序 bucket 校验分三档**，回放构造测试数据时必须满足最严的一档：7.0.37 完全不校验；7.0.43 要求 `control.min`、`control.max` 与 `data` 列携带相同字段集（否则 `code 121`，报 `Control had N min fields and M max fields, but observed data had K fields`）；8.x 在此之上还要求 `control.min`/`control.max` 与列数据**逐值相等**（否则 `code 121 Incorrect column data min for field ...`）。详见 #1006。

## 验证

- **红绿对照（两个方向都验过）：**
  - 把 `ui` 转发加回 `applyOpsDoc`：两个新测试均变红，目标端 mongod 日志出现 `Location3318200`，而 `TestTimeSeriesIntegration` 仍旧全绿——正好复现"旧测试为什么看不见这个问题"。
  - 让 `ConvertV2Oplog` 吞掉 `b` 而不再退出（使 op 走普通 `$set` 路径）：`TestTimeSeriesBucketBinaryColumnReplay` 变红，`Plan executor error during update :: caused by :: BadValue`。
- **skip 分支逐一实测**（要求"响亮地跳过"而不是静默通过）：

  | 服务端 | 结果 |
  |---|---|
  | 7.0.37 / 7.0.43 standalone | `server is MongoDB 7.0.43, older than 8.0, where time-series bucket diffs carry no binary column ('b') section at all` |
  | 8.2.12 standalone | `server is not a replica set, so there is no local.oplog.rs to capture a real bucket diff from` |
  | 7.0.37 replica set | bucket 测试按版本跳过；`TestSystemViewsApplyOpsReplay` 运行 |
  | 8.2.12 replica set | 两个都运行，捕获到 2 条 bucket op |

  版本判定用 `GetDBVersion` 而非 `GetAndCompareVersion`，因为后者通过 error 返回值表达"低于阈值"，会把一台普通的旧服务器变成 `Fatalf`。
- **回归：** `./executor` 在 7.0.37（standalone 与 replica set）、7.0.43、8.2.12 上均通过。
- **环境说明：** `mongo:8.0` 镜像在 Linux 内核 6.19 及以上无法启动（`MongoDB cannot start: Linux kernel versions 6.19 and newer has a known incompatibility with this version of MongoDB`，SERVER-121912）；8.x 侧验证使用 `mongo:8.2`（8.2.12）。副本集是必需的，不是可选项：捕获真实 diff 需要读 `local.oplog.rs`，standalone 没有。
