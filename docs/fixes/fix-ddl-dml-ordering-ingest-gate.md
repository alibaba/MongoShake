# 修复：DDL 与 DML 未按 oplog 顺序同步

## 需求描述

**来源：** [GitHub Issue #934](https://github.com/alibaba/MongoShake/issues/934)

**现象：** 目标端 DDL 与 DML 的执行顺序与源端不一致。

源端执行顺序为 DML1→DDL→DML2 时，目标库实际执行顺序可能为 DDL→DML1→DML2，或 DML1→DML2→DDL。

概率性发生，写入越密集、队列越满越容易复现。

**预期行为：** DDL 与 DML 之间严格按照 oplog 顺序同步。DML 内部的顺序不保证。

## 根因分析

### 1. 队列本身是乱序的

oplog 要经过 `Buffer → PendingQueue[i] → deserializer[i] → logsQueue[i]` 多段缓冲才到达 `Batcher`。
`Persister.dispatchBuffer()` 用 `nextQueuePosition % len(PendingQueue)` **轮转投放**，`shard_key` 只决定
DML 落到哪个 **worker**，不影响落到哪个 **queue**，所以各 `logsQueue[i]` 持有**互相重叠的 ts 区间**——
队列之间没有"谁更早"的关系（这是并行的前提）。`getBatch()` 又按**队列下标**拼接（"下一个队列非空就继续
append"，直到达到 `IncrSyncAdaptiveBatchingMaxSize`），拼接顺序与 ts 无关。

于是 **op 的 ts 与"它是否已被派发、是否已落库"之间没有对应关系**：ts 更大的批次可能先派发、先落库，
而 ts 更小的 op 还压在其他队列里。这是下一条成立的前提。

### 2. `checkCheckpointUpdate` 不能保证顺序一致

`Batcher` 遇到 DDL 时，`checkCheckpointUpdate` 的判据是 **checkpoint ≥ 本批 DML 的最大 ts**。

但 checkpoint 是各 worker ack 的汇总值（`calculateWorkerLowestCheckpoint()`：仍有批次在飞时取最小值，
worker 全部空闲时取最大值），并且**只增不减**；而 ack 只随**已落库**的批次前进，不会因新批次入队而回退。

配合第 1 点——跨批次 ts 不严格单调——就有：DML1 还压在某个 `logsQueue` 里、从未派发给任何 worker，
而 ts 更高的其它批次已经落库；此时所有 worker 空闲（`ack == unack`），checkpoint 已经是那些更高的 ts，
**判据提前成立，DDL 抢在 DML1 前面执行**。

#981 把这里的等待从有界改为无界并不能解决：判据本身不成立，进入循环时往往已经满足，等多久都一样。

## 修复方案

### 入口闸门

闸门是本次修复的核心。reader 是管道唯一的生产者，`next()` 读到 DDL 时按这个时序走：
**关闸 → 把 Buffer 里残余的 raw 刷出去 → 等管道真的安静 → 才 `Inject` 这条 DDL**。

- **关闸之后 reader 不再从源端取任何 op**，所以"ts 晚于 DDL 的 op"一条都没有机会抢先进入管道；
  DDL 注入时，它前面的 op 全都已经在管道里了——顺序保证由此成为**构造性**的事实。
- **安静判据**：`rawInFlight == 0` 且所有 `logsQueue` 为空。`rawInFlight` 从唯一的入口
  （`bufferInput`）一直数到 deserializer 把批推进 `logsQueue` 之后，归零即"没有 op 还在管道里飞"。
- **放行**同样由 poll 协程按管道状态判定：`rawInFlight` 归零且所有 `logsQueue` 为空即开闸。
  关闸与放行读的是同一份管道状态。
- DDL 的识别沿用 batcher 同一个谓词 `ddlFilter.Filter`（共用 `parseRawOplog()` 解析），两侧判定结果
  一致。前置的 `mayBeDDL()` 只是零解析快筛，只跳过**可证明**不是 DDL 的 op——漏判只是多等一轮，
  误判才会丢掉整个保证。事务 commit 需要 `txnBuffer` 的累积状态，不在入口分类范围内
  （见[已知限制](#已知限制)第 1 条）。
- Buffer 由**属主**（poll 协程；disk replay 期间是 `retrieve()`）在入闸前刷出，所以关闸瞬间仍在
  Buffer 里的 raw 都已经在管道里，`rawInFlight` 不会停在 1。回归用例
  `TestNextFlushesBufferBeforeParkingOnGate`。

### barrier 轮等真实 worker ack

`startBatcher` 在 barrier 轮的等待判据是 `waitForAllWorkersIdle()`（`batcher.go:609`）：每个 worker 的
`queue` 排空且 `unack == ack`。direct 隧道的 `Send()` 内部是同步写，**ack 就等于"已落到目标端"**，
所以它读到的是真实的落库状态。

barrier 轮固定返回 `allEmpty = false`，让这一轮的等待真正生效。DDL 本身也是一个批次：
`processMergeBatch` 把它放进 `barrierOplogs`，下一轮转进 `batchGroup` 交给 `worker[0]` 经 tunnel 写入。

### reader 游标不回退

`EnsureNetwork()` 重建游标时把查询基准抬到取数水位之上：`query[QueryTs]` 由 batcher 的"最近一次**已分发**"
时间戳驱动，它落后于取数水位，用它重建游标会**重投已经进入管道的 op**——包括比某个 barrier 更早的 op。
水位只约束**运行中**的游标重建；进程重启从已落盘 checkpoint 恢复，本来就允许重复交付。

## 影响范围与生效条件

本修复需要**同时满足**以下两个条件才会生效：

- **`tunnel = direct`**：本方案的现场是 mongo2mongo；其它 tunnel 未经验证，且 `waitForAllWorkersIdle()`
  的 ack 语义只在 direct 成立（见[已知限制](#已知限制)第 3 条）。
- **`incr_sync.barrier.ordering_enable`**（`collector/configure/configure.go:92`、`conf/collector.conf:414`，
  **默认 `false`**）：**回滚杠杆**——入口排空会让管道在 DDL 处停顿，出问题时必须能不改代码切回原行为。
  默认 `false` 与 nimo"只设置文件中出现的键"的加载方式自洽，升级本身不改变行为，也无需 bump `conf.version`。

其余情况（非 direct、开关关闭、配置文件没写 `tunnel` 键）**逐语句等于原实现**，每个受影响的判断点
都只多出一层分支。

## 测试

| 用例 | 覆盖 |
|------|------|
| `TestIsEntryBarrierClassifiesDDLOnly` | 入口分类：`op:"c"` 命令、`system.indexes` 写 → true；`applyOps`、普通写、空 raw → false；change stream 的 `insert/update` 不到解析阶段、`drop`/`invalidate` 必须解析；非 direct / `FilterDDLEnable=false` / `ordering_enable=false` 一律 false |
| `TestNextGatesDDLUntilPipelineQuiesced` | 入口闸门时序：两条 DML 不限流进入管道，第三条是 DDL 时 `next()` 必须先关闸、把两条 DML 交给 batcher 才 `Inject`；随后闸门按管道状态自动重开，batcher 看到的顺序是 `50, 51, 100` |
| `TestNextDoesNotGateOutsideDirect` | 两个条件必须同时满足：`kafka + 开` / `未配置 tunnel + 开` / `direct + 关` 三例都是 DDL 直接穿透、闸门恒为 nil |
| `TestNextFlushesBufferBeforeParkingOnGate` | 属主刷写纪律的回归：关闸时 Buffer 里的 op 必须由属主在入闸前推出 |
| `TestBatchMore`（`allEmpty` 断言） | barrier 轮恒 `allEmpty=false`，并按 tunnel × 开关分列断言（`direct+开` / `direct+关` / `未配置+开` / `kafka+开`） |
| `TestTrackFetchedTsRaisesWatermark` | reader 水位只升不降；读不出 ts 的文档被忽略，水位不动 |
| `TestAdvanceQueryBaseToFetchWatermark` | 水位领先时基准被抬升；重复调用幂等；checkpoint 领先时不把基准往回拉 |
| `TestWatermarkIsInertWithoutOrderingGuarantee` | 水位在非 direct **以及 `direct + 开关关闭`** 下完全不动 |

以上用例均已通过（`go test ./collector/ -count=1` 整包通过，含 `TestBatchMore`；
`go test ./collector/reader/ -run '<三个水位用例>' -count=1` 通过）。

## 已知限制

1. **事务 commit（`mustIndividual`）**：需要 `txnBuffer` 的累积状态才能判定，入口处识别不出来，
   因此不在闸门的覆盖范围内。要覆盖它必须把 `txnBuffer` 的状态跟踪一并前移，属后续独立工作。
2. **分片源（多 `MongoUrls`）**：`RealSourceIncrSync` 每个 shard 一个 syncer，各有独立的
   reader / PendingQueue / logsQueue / batcher，**入口闸门是 per-syncer 的**。因此顺序保证只对
   per-syncer 成立：**分片源下 DDL 与 DML 的顺序不作保证**。另需注意分片源上 DDL 本身在各 shard 的 ts
   并不一致（`drop` 在每个 shard 上是不同 ts），一个 shard 上的 "ts < B" 跨 shard 没有统一含义。
3. **非 direct tunnel 下 "ack" 不等于"目标端已执行"**：`kafka` / `file` 的 `AckRequired()` 为 `false`
   （ack 只是"已交给 tunnel"），`rpc` / `tcp` 为 `true` 但那是 receiver 回执。本方案只在 direct 下调用
   `waitForAllWorkersIdle()`，因此这一条不影响本次的顺序保证；receiver 侧的顺序保证不在本次范围。
4. **`incr_sync.target_delay`**：入口等待只覆盖"已进入内存管道"的 op，尚未从源端拉到的 op 不在等待范围内；
   但入口 FIFO + oplog ts 单调 ⇒ 这些 op 的 ts 必然 ≥ barrier，不破坏顺序，只推迟 DDL 的执行时点。
5. **开关在 `tunnel != direct` 时静默失效**：用户把它打开不会报错也不会有任何效果，日志里也没有提示，
   现象就是"开关打开了但 DDL 还是乱序"。`cmd/collector/sanitize.go` 已有"配置项之间互相约束"的校验先例，
   可以加一条 warn（该组合只是无效，不算用户错误）。未做，属易用性改进。
6. **吞吐**：barrier 期间停止拉取是顺序保证的代价，DDL 密集负载下单连接吞吐会下降。

## 改动文件清单

| 文件 | 改动 |
|------|------|
| `collector/syncer.go` | 入口分类（`isEntryBarrier` / `mayBeDDL` / `parseRawOplog`，与 deserializer 共用同一份解析）、安静判据与等待（`pipelineQuiesced` / `waitPipelineQuiesced`）、`rawInFlight` 字段与判据、`next()` 两侧闸门、`startBatcher` 三个判断点 |
| `collector/batcher.go` | `ingestGate` 及 `setIngestGate()` / `clearIngestGate()`、`processMergeBatch()` / `batchGroupEmpty()` 抽出、`BatchMore()` 在 barrier 轮返回 `allEmpty=false`、`waitForAllWorkersIdle()` 作为 barrier 已落地的判据 |
| `collector/persister.go` | `rawInFlight` 计数、`FlushBuffer()` / `flushOwnedBuffer()`（属主纪律）、disk replay 侧的属主刷写 |
| `collector/reader/oplog_reader.go` | `lastFetchedTs` 水位、`trackFetchedTs()`、`advanceQueryBaseToFetchWatermark()`、谓词副本 |
| `collector/configure/configure.go` | `IncrSyncBarrierOrderingEnable` |
| `conf/collector.conf` | `incr_sync.barrier.ordering_enable`（双语说明，默认 false） |
| `collector/batcher_test.go` | 新增 `stubReader` / `rawOplogRaw` 辅助与 4 个入口闸门、分类用例；`TestBatchMore` 增加按 tunnel × 开关分列的 `allEmpty` 断言 |
| `collector/reader/oplog_reader_test.go` | 水位用例（新增文件） |
| `docs/fixes/fix-ddl-dml-ordering-ingest-gate.md` | 本文件 |
