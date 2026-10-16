# 修复：DDL 与 DML 未按 oplog 顺序同步

## 需求描述

**来源：** [GitHub Issue #934](https://github.com/alibaba/MongoShake/issues/934)

**现象：** 目标端 DDL 与 DML 的执行顺序与源端不一致。

源端执行顺序为 DML1→DDL→DML2 时，目标库实际执行顺序可能为 DDL→DML1→DML2，或 DML1→DML2→DDL。

**预期行为：** DDL 与 DML 之间严格按照 oplog 顺序同步。DML 内部的顺序不保证。

## 根因分析

### 1. 读出序不是问题

数据通路：

```
reader.Next() → Persister.Buffer → PendingQueue[i] → deserializer[i] → logsQueue[i]
              → Batcher.getBatch() → dispatchBatches() → worker[i] → tunnel(direct)
```

`Persister.dispatchBuffer()` 用 `nextQueuePosition % len(PendingQueue)` **轮转投放**，`getBatch()` 按**同一个轮转顺序**取回：
它阻塞读 `logsQueue[currentQueue()]`，每取走一个批次指针前进一格；只有当"下一个队列非空"时才继续机会式合并，遇到空队列就停在它上面、下一轮从这个队列开始阻塞。

投放规则是确定性的——第 k 个批次必进 `queue[k%n]`，而 channel 是 FIFO ⇒ 每个队列内部按下标递增——所以 **getBatch 的读出序恒等于投放序，也就是 ts 序**。某个队列的 deserializer 慢，只会让本轮停下来等它，不会让更新的批次超车；"跳过滞后队列"那种写法（`select` + `default`）不在代码里。

这一点已由 #1014 的探针独立排除：真实 `getBatch` + 人为延迟，含队列容量 1 / 慢通道 2–5ms / `batching_max_size=4096` 的对抗配置，6000 条递增 ts 的 oplog，`-race` 下 0 违例。

### 2. 真正的问题：barrier 轮被判成"空轮"，整段等待分支被跳过

`dispatchBatches()` 按 `shard_key` 把一批 op 拆给多个 worker **并行**执行，所以"**已读出、已派发**"与"**已应用**"之间没有对应关系。DDL 要保证顺序就必须在 barrier 轮**等**——那正是 `startBatcher` 里这段分支的职责。

`BatchMore()` 遇到 DDL 时（`processMergeBatch` 返回停止下标那条路径），把 DDL 放进 `batcher.barrierOplogs`、结束本轮并返回：

```go
return batcher.batchGroup, true, batcher.setLastOplog(), false
```

第三个返回值 `allEmpty` 来自 `setLastOplog()`，而它当时返回的就是"`batchGroup` 里有没有 op"。于是：

- DDL 是 `mergeBatch[0]`，或它前面的 op 全被 filter 掉 ⇒ `batchGroup` 为空 ⇒ `allEmpty = true`；
- 事务 commit（`handleTransaction` 的 `mustIndividual`）那条路径更极端：原代码直接 `nimo.AssertTrue(allEmpty == true, ...)`，batchGroup 必定为空 ⇒ `allEmpty = true`。

而 `startBatcher` 的分支条件是：

```go
} else if log, filterLog := batcher.getLastOplog(); log != nil && !allEmpty {
```

`!allEmpty` 为假 ⇒ **整段 barrier 分支被跳过**：不 `dispatchBatches`、不 `checkpoint`、`checkCheckpointUpdate` 一次都没有执行。DDL 留在 `barrierOplogs` 里，下一轮才被转进 `batchGroup` 交给 `worker[0]`；而更早的轮次已经派发到其它 worker 的 DML 可能还在队列里或在途 ⇒ **DDL 越过它之前的写入执行**。

Issue #981 把这里的等待从有界改成无界，方向是对的，但解决不了这个 bug：分支被跳过时，等待根本不会开始。

**为什么只在部分场景出现：** 这个跳过与并发无关，只取决于批次划分——DDL 是否恰好是本轮 `mergeBatch` 的第一个（或第一个未被过滤的）op。落到这个形状必定乱序，落到别的形状就没事，所以现象看起来是概率性的。

一旦这条分支能被执行，它后面的等待本身是够用的：`sync.checkpoint(barrier, 0)` 把各 worker ack 的聚合值写进 checkpoint（`calculateWorkerLowestCheckpoint()`，`checkpoint.go:290`：有批次在飞取最小值，全部追平取最大值），`checkCheckpointUpdate(true, newestTs)`（`syncer.go:415`）再等这个持久化值追上 `newestTs`，等待期间每轮都会重新 flush（`barrierCkptFlushFunc`，`syncer.go:50`）。direct 隧道的 `Send()` 是同步写，所以"checkpoint 追平"就等于"在 DDL 之前派发的 op 都已经应用"。

## 修复方案

**barrier 轮恒 `allEmpty = false`**（`collector/batcher.go`），保证 barrier 轮的等待一定会被执行。

两条 barrier 路径都改——DDL 那条，以及事务 commit 的 `mustIndividual` 那条。同时抽出 `processMergeBatch()`（`batcher.go:345`），让"停在哪条 barrier 上"有单一来源。

隧道/开关不满足 `incrOrderingGuarantee()`（`batcher.go:59`）时，逐语句等于原实现。

另外补一条启动 warn：开关打开但不会生效的两种组合（`tunnel != direct`、分片源）此前完全静默，用户付了吞吐代价却看不到任何提示。`cmd/collector/sanitize.go` 的 `checkIneffectiveOptions()` 对这两种组合各产生一条 warn，由 `cmd/collector/collector.go` 在日志初始化后打出（sanitize 阶段 `l.Logger` 还是 no-op，不能在那里打）。

## 影响范围与生效条件

本修复需要**同时满足**以下两个条件才会生效（`incrOrderingGuarantee()`，`collector/batcher.go:59`）：

- **`tunnel = direct`**：本方案的现场是 mongo2mongo；其它 tunnel 的 ack 语义不同（见[已知限制](#已知限制)第 2 条）。
- **`incr_sync.barrier.ordering_enable`**（`collector/configure/configure.go:94`、`conf/collector.conf:451`，**默认 `false`**）：**回滚杠杆**——修复让 barrier 轮必定执行等待与强制 checkpoint，出问题时必须能不改代码切回原行为。默认 `false` 与 nimo"只设置文件中出现的键"的加载方式自洽，升级本身不改变行为，也无需 bump `conf.version`。

其余情况（非 direct、开关关闭、配置文件没写 `tunnel` 键）逐语句等于原实现，每个受影响的判断点只多出一层分支。

## 测试

| 用例 | 覆盖 |
|------|------|
| `TestBatchMore`（`allEmpty` 断言） | barrier 轮恒 `allEmpty=false`；并按 tunnel × 开关分列断言（`direct+开` → `false`；`direct+关` / 未配置 `tunnel` / `kafka+开` → `true`，即维持原行为）。用例形状正是根因第 2 点：18 条 op 里 DDL 是 `mergeBatch[0]`，`batchGroup` 为空、17 条进 `remainLogs` |
| `TestBatchMore`（既有 barrier 轮断言） | 该用例配置为 `direct` + 开关打开，所有 barrier 轮断言由 `allEmpty=true` 改为 `false` |
| `TestCheckIneffectiveOptionsBarrierOrdering`（`cmd/collector`） | warn 矩阵：`direct+副本集` → 0 条；`kafka+副本集` → 1 条；`direct+分片` → 1 条；`kafka+分片` → 2 条；开关关闭 → 0 条 |

### 真实环境对照实验

除单元用例外，还在真实集群上做了对照组：持续对多张表压测，同时每 5s 在压测表上执行一次 `drop` + `createCollection`，制造"DML 持续写入 + DDL 周期性穿插"的负载。源库与目标库均为 MongoDB 5.0 三节点副本集（2c4g）。

| `incr_sync.barrier.ordering_enable` | 运行时长 | 目标端临时集合（`tmp*.create`）出现次数 |
|------|------|------|
| `false`（历史行为） | 3 小时 | 3 |
| `true`（本修复） | 24 小时 | 0 |

每次乱序都会在目标端留下一个临时集合（名字形如 `tmpADIWS.create`），里面正是在 `drop` 执行之后、`createCollection` 执行之前被乱序写入目标库的数据，所以它的出现次数就等于乱序次数。

## 已知限制

1. **分片源（多 `MongoUrls`）**：`RealSourceIncrSync` 每个 shard 一个 syncer，各有独立的 PendingQueue / logsQueue / batcher / worker，barrier 是 **per-syncer** 的，因此顺序保证只对 per-syncer 成立：**分片源下 DDL 与 DML 的顺序不作保证**。另外分片源上同一个 DDL 在各 shard 的 ts 并不一致（`drop` 在每个 shard 上是不同 ts），跨 shard 的 "ts < B" 没有统一含义。`conf/collector.conf` 中该键的注释已写明这一点，分片源打开开关时 collector 启动会打 warn。
2. **非 direct tunnel 下 "ack" 不等于"目标端已执行"**：`kafka` / `file` 的 `AckRequired()` 为 `false`（ack 只是"已交给 tunnel"），`rpc` / `tcp` 为 `true` 但那是 receiver 回执。barrier 轮的等待依赖"ack 即已应用"，所以本方案只在 direct 下声明顺序保证；receiver 侧的顺序保证不在本次范围。
3. **开关在 `tunnel != direct` 或分片源时不生效**：用户打开它不会报错也不会有任何效果。这两种组合现在各有一条启动 warn（`cmd/collector/sanitize.go` 的 `checkIneffectiveOptions()`，由 `cmd/collector/collector.go` 打出），不再静默；该组合只是无效，不算用户错误，所以不阻塞启动。
4. **`incr_sync.target_delay`**：等待只覆盖"已进入内存管道并被派发"的 op；尚未从源端拉到的 op 的 ts 必然 ≥ barrier（入口 FIFO + oplog ts 单调），不破坏顺序，只推迟 DDL 的执行时点。
5. **吞吐**：DDL 密集的负载下，每个 barrier 轮都要等它之前的 oplog 落定（历史行为里这些轮次有一部分被整段跳过），吞吐低于历史行为；这是顺序保证的代价，也是开关默认关闭的原因之一。
6. **既有断言的消息文本与判据相反**：`batcher.go:367` 的 `nimo.AssertTrue(allEmpty == true, "batcher.batchGroup don't be empty")` 中 `allEmpty` 取自 `setLastOplog()`、含义就是 "batchGroup 为空"，即判据要求 `batchGroup` 为空，消息却写成 "don't be empty"。该断言只在 `-tags debug` 构建下 panic（`runtime_release.go` 中 `AssertTrue` 是空函数），release 构建无影响，但 debug 构建触发时提示会把排查方向带反。属既有代码的文案问题，建议上游单独修正。

## 改动文件清单

| 文件 | 改动 |
|------|------|
| `collector/batcher.go` | `BatchMore()` 两条 barrier 路径恒返回 `allEmpty=false`（`stopAt >= 0` 那条）、`processMergeBatch()` 抽取、`incrOrderingGuarantee()` |
| `collector/configure/configure.go` | `IncrSyncBarrierOrderingEnable` |
| `conf/collector.conf` | `incr_sync.barrier.ordering_enable`（双语说明，默认 false；含"仅在 direct + 单副本集生效"的 caveat） |
| `collector/batcher_test.go` | `TestBatchMore` 的 `allEmpty` 断言（含 tunnel × 开关分列） |
| `cmd/collector/sanitize.go` | 新增 `checkIneffectiveOptions()`：无效配置的启动 warn（`SanitizeOptions()` 未改动） |
| `cmd/collector/collector.go` | 日志初始化后打印上述 warn |
| `cmd/collector/sanitize_test.go` | `TestCheckIneffectiveOptionsBarrierOrdering` |
| `docs/fixes/fix-ddl-dml-ordering-barrier-round.md` | 本文件 |
