# 修复：change stream update 事件中 truncatedArrays 导致文档被错误替换

## 需求描述

**现象：** 使用 change stream 增量同步时，聚合管道更新（如 `$slice`）只截断数组而不修改其他字段时，目标端文档丢失除 `_id` 以外的所有字段。

**预期行为：** 数组截断应在目标端正确回放，其他字段保持不变。

**GitHub issue：** https://github.com/alibaba/MongoShake/issues/986

## 根因分析

MongoDB change stream 的 update 事件包含 `updateDescription`，其中有三个字段：
- `updatedFields`：修改的字段
- `removedFields`：删除的字段
- `truncatedArrays`：截断的数组（含 `field` 路径和 `newSize`）

当聚合管道更新只截断数组时，`updatedFields` 和 `removedFields` 均为空，只有 `truncatedArrays` 有内容。

`oplog/change_stream_event.go` 的 `ConvertEvent2Oplog` 函数只处理前两个字段，忽略了 `truncatedArrays`，导致生成的 `oplog.Object` 是空 `bson.D{}`。

executor 在判断更新类型时调用 `ObjectHasPrefix("$")`：
- 有 `$` 前缀 → `UpdateOne`（增量更新）
- 无 `$` 前缀 → `ReplaceOne`（全量替换）

空 `bson.D{}` 没有 `$` 前缀，被当作 replacement 执行，用空文档替换了目标文档，导致字段丢失。

## 修复方案

### 1. 解析 `truncatedArrays` 为 `$push.$each.$slice`

在 `ConvertEvent2Oplog` 的 update case 中解析 `truncatedArrays`，将每个截断条目转换为 `$push` + `$each: []` + `$slice` 经典 update operator：

```go
truncatedArrays: [
  { field: "arr", newSize: 1 }
]
```

转换为：

```json
{
  "$push": {
    "arr": { "$each": [], "$slice": 1 }
  }
}
```

`$push.$each.$slice` 自 MongoDB 2.6 起支持，是常规 update operator（非聚合表达式），在单文档模式下正确执行，兼容所有目标端版本。

**为什么不用 `$set` + `$slice` 聚合表达式：**
- `$slice` 聚合表达式在 `$set` 内仅当 MongoDB server 把整个 update 当作 aggregation pipeline 解释时才正确执行
- change stream 路径生成 `bson.D`（单文档），executor 直接传给 Go driver → MongoDB server 按普通 update operator 解释
- MongoDB 4.x 目标端会把 `{"$slice": ["$arr", 1]}` 当作字面量写入，导致数组字段被替换为嵌套文档（静默数据损坏）
- `$push.$each.$slice` 避免了 pipeline-vs-document 歧义

### 2. 防御性类型断言

将 `removedFields` 的类型断言改为 comma-ok 风格，与新增 `truncatedArrays` 代码一致，避免类型不匹配时 panic。

### 3. 同字段冲突降级（`$set` 与 `$push` 路径互斥）

**一个事件可以同时修改并截断同一个数组。** `$v:2` delta 允许同一个数组子 diff 里 `u<N>` 与 `l` 并存，MongoDB 7.0.37 实测：200 元素数组执行

```javascript
db.items.updateOne({_id:1}, [{$set:{arr:{$concatArrays:[["X"],{$slice:["$arr",1,99]}]}}}])
```

产生 `{"$v":2,"diff":{"sarr":{"a":true,"l":100,"u0":"X"}}}`，对应 change stream 事件为
`updatedFields={"arr.0":"X"}` + `truncatedArrays=[{field:"arr",newSize:100}]`。

此时若同时生成 `$set:{"arr.0":"X"}` 和 `$push:{arr:...}`，服务端直接拒绝：

```
Updating the path 'arr' would create a conflict at 'arr'
```

executor 对失败的写入无限重试，**排在它后面的所有 namespace 一起卡死**。实测目标端停在 200 元素不动、日志每秒刷 `[CRITICAL] ... mongo.BulkWriteException`。

文档模式下无解：`$push` 改不了指定下标，`$set` 整个数组又拿不到数组内容（`watch_full_document = false`），`$unset` 数组下标只会置 null 而不会缩短数组；而 `PartialLog.Object` 是 `bson.D`，装不下 aggregation pipeline（`bson.A`）。

因此检测到冲突时**丢弃该条截断并打 WARNING**，保留 `$set`。目标端数组维持截断前长度——这与未修复前的行为一致（同样丢截断），但**同步不会中断**。冲突判定按点分路径分量而非裸字符串前缀，所以 `arr` 与 `arr.0` 冲突、与 `arrayish` 不冲突。

### 4. 空对象防护与不可表示事件的分级

`ConvertEvent2Oplog` 区分两种情况：

| 情况 | 返回 | syncer 行为 |
|---|---|---|
| `updateDescription` 三者本来就空 | `ErrEmptyChangeStreamUpdate`（sentinel） | log warn 后跳过 |
| `truncatedArrays` 有条目但无法表示（`field` 缺失/为空、`newSize` 缺失/非 int32/int64/负数、条目或外层类型不符） | 普通 error | `Panicf`，同步停止 |

第二级不能走 sentinel：那会把"我们读不懂这个事件"降级成一条 WARNING 后静默丢掉一次真实截断。

只有"本来就空"能走到 sentinel 分支——不可表示的条目已在上面报错返回，因冲突被丢弃的截断必然留下了对应的 `$set`，`oplog.Object` 不会为空。

## 影响范围

- **文件：** `oplog/change_stream_event.go`、`collector/syncer.go`
- **单元测试：** `oplog/change_stream_event_test.go`（`TestConvertEvent2Oplog_TruncatedArrays`，14 个用例：纯截断、与 `updatedFields`/`removedFields` 组合、嵌套点分路径、多条目、全空 sentinel、同字段冲突的 4 种形态、部分冲突保留兄弟条目、5 种不可表示条目、外层类型错误）
- **端到端测试：** `integration/changestream_truncated_arrays_test.go`（`TestChangeStreamTruncatedArrays`，两个子测试共用一次 collector 启动：`truncate_only` 验证 #986 原始场景收敛；`modify_and_truncate_same_array` 验证冲突降级后 `$set` 仍落地、WARNING 打出、没有冲突写入到达服务端、且随后的无关更新仍能同步）
- **场景：** 聚合管道更新仅截断数组时，目标端正确回放 `$push.$each.$slice` 而非替换文档

跑端到端测试有两个换环境时容易踩的前提（都已内置在测试里）：

1. **必须带 `-count=1`。** 被测代码由 `go run` 子进程编译、不被 `integration` 包 import，Go 的测试缓存感知不到它变化，改完实现重跑会命中缓存直接返回旧的 PASS（本轮实测踩到，红绿对照差点被误判成"改不动"）。
2. **collector 默认 `log.flush = false`**，日志有最长 1s 缓冲。测试要断言日志里的 WARNING，所以 conf 中显式设为 `true`，否则该断言偶发失败。

### 已知边界与风险

1. **同字段既改又截断时丢截断**（见第 3 节）。表现为目标端数组比源端长，同步不中断，日志有 WARNING。这是文档模式下的固有限制，彻底解决需要 executor 支持 pipeline update。
2. **`$push` 对目标端字段缺失会凭空造出空数组**：目标文档没有 `arr` 时，`{$push:{arr:{$each:[],$slice:2}}}` 会写入 `arr: []`。仅在源目已不一致时发生。
3. **`$push` 对目标端非数组字段硬报错**：`The field 'arr' must be an array but is of type string`，会导致该条写入无限重试。同样仅在源目已不一致时发生。
4. `$push.$each.$slice` 语义已实测：正数 N 保留前 N 个；目标数组短于 N 时为 no-op；重复回放幂等。
5. `newSize` 只接受 int32/int64。真实 change stream 恒为 int32，出现其它类型按不可表示处理（报错而非猜测）。

> 注：GitHub issue #986 的回复曾预告用 `$set` + `$slice` 聚合表达式实现，最终改用 `$push.$each.$slice`，原因见上文"为什么不用 `$set` + `$slice` 聚合表达式"。
