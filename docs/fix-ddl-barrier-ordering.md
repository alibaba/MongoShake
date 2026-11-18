# 修复：DDL 和 DML 操作没有按 oplog 顺序同步

## 需求描述

**来源：** [GitHub Issue #934](https://github.com/alibaba/MongoShake/issues/934)

**现象：** 使用 YCSB 做压测时，高并发 insert 导致同步延迟。insert 完成后立刻执行 `dropCollection`。目标端先同步了 drop 操作，再同步剩余的 insert，导致源库 collection 为空而目标库遗留部分数据。

**预期行为：** DDL（如 drop）操作应在所有前序 DML（如 insert）操作完成后再执行，严格保证 oplog 顺序。

## 根因分析

问题出在 `collector/syncer.go` 的 `checkCheckpointUpdate()` 方法。

增量同步的数据流为：`Batcher.BatchMore()` → `dispatchBatches()` → Workers 并行处理。当 Batcher 遇到 DDL 时，会设置 `barrier=true` 并将 DDL 暂存到 `barrierOplogs`，先返回 DDL 之前的 DML batch。`startBatcher()` 在 dispatch DML batch 后调用 `checkCheckpointUpdate(barrier=true, newestTs)` 来等待所有 worker 完成。

**但是** `checkCheckpointUpdate` 使用了**有界循环**：

```go
for i := 0; i < CheckCheckpointUpdateTimes; i++ { // 最多 10 次 × 300ms = ~3 秒
    // 轮询 remote checkpoint...
}
// 超时后放弃等待，继续执行
l.Logger.Warnf("check checkpoint[%v] update to ts[%v] failed, but don't worry", ...)
```

当积压量很大时（百万级 insert），3 秒不足以让所有 worker 完成。超时后 batcher 继续取出 DDL 并 dispatch 给 worker 0 执行，但此时其他 worker 可能仍在写入 insert，导致：

1. Worker 0 执行 `drop` → collection 被删除
2. Workers 1..N 继续写入剩余 insert → MongoDB 自动创建 collection → 数据残留

## 修复方案

将 `checkCheckpointUpdate` 中 `barrier=true` 时的循环从有界改为**无界 + Pause/Shutdown 感知**，确保 checkpoint 到达 `newestTs`（即所有 worker 的 ACK 值都 >= DML batch 的最后时间戳）后才返回，同时在运维发出 Pause/Shutdown 信号时能中断等待。

**具体改动：**

1. `for i := 0; i < CheckCheckpointUpdateTimes; i++` → `for i := 0; ; i++`
2. 循环开头增加 `IncrSentinelOptions.Pause` 和 `IncrSentinelOptions.Shutdown` 检查
3. 降低日志频率（每 10 次迭代输出一次比较日志），避免长时间等待时日志过多
4. 删除超时后的 "don't worry" 警告块（不再可达）
5. 引入 `barrierCkptGetFunc` / `barrierCkptFlushFunc` 函数变量，方便测试注入
6. `DDLCheckpointInterval` 从 `const` 改为 `var`，方便测试覆写加速

**改动文件：** `collector/syncer.go`、`collector/worker.go`（类型转换适配）、`collector/syncer_test.go`（新增测试）

## 退出循环的方式

| 方式 | 触发条件 | 返回值 | 含义 |
|------|----------|--------|------|
| 正常退出 | `checkpointTs >= newestTs` | `true` | 所有 worker 完成 DML，DDL 可以安全执行 |
| Pause 退出 | `IncrSentinelOptions.Pause == true` | `false` | 运维通过 sentinel API 中断等待 |
| Shutdown 退出 | `IncrSentinelOptions.Shutdown == true` | `false` | 运维通过 sentinel API 触发关闭 |

运维操作方式：`POST http://<host>:<incr_sync.http_port>/sentinel/options`，body 为 `{"Pause":true}` 或 `{"Shutdown":true}`。

## 修复 Pause/Resume 后 barrier ordering 漏洞

**问题：** `checkCheckpointUpdate` 返回 false（Pause/Shutdown 中断）时，调用方未检查返回值，下一轮 loop 直接处理新 ops，可能导致 DDL 未确认完成就处理后续操作，破坏 ordering。

**修复（方案A）：** 在 `startBatcher` 闭包中新增 `pendingBarrierTs` 变量：
1. 若 `checkCheckpointUpdate(barrier=true, newestTs)` 返回 false，记录 `pendingBarrierTs = newestTs` 并 return
2. 下次循环开头检查 `pendingBarrierTs > 0`，重新调用 `checkCheckpointUpdate` 确认
3. 确认成功后才清零 `pendingBarrierTs`，允许处理新 ops
4. 确认失败继续 return，不处理任何新 batch

## 边界场景分析

| 场景 | 行为 | 说明 |
|------|------|------|
| 目标端永久不可达 | Worker.transfer() 本身无限重试，checkpoint 不推进，barrier 等待持续 | 整个 pipeline 已经卡住；barrier 等待不是新增问题。运维可通过 Pause/Shutdown 中断 |
| Checkpoint 存储不可达 | ckptManager.Get() 报错，循环 continue 重试 | 每次重试等待 900ms，持续输出 Error 日志。运维可通过 Pause/Shutdown 中断 |
| Worker 异常状态 | calculateWorkerLowestCheckpoint() 返回 (0, err)，checkpoint() 不更新 | checkpoint 不推进，循环继续等待。运维可通过 Pause/Shutdown 中断 |
| 正常大量积压 | Worker 逐步完成，checkpoint 逐步推进，最终 >= newestTs | 正常退出，DDL 在所有 DML 完成后执行 |

## 测试

新增 `TestCheckCheckpointUpdate`（5 个子用例）：

1. `barrier=false` → 立即返回 `false`
2. checkpoint 已达到 newestTs → 立即返回 `true`
3. checkpoint 逐步推进到 newestTs → 多次迭代后返回 `true`
4. Pause 中断 → 返回 `false`
5. Shutdown 中断 → 返回 `false`

## 影响范围

- 仅影响 `barrier=true`（DDL 或大事务）时的等待逻辑
- 非 barrier 场景不受影响
- 正常情况下 worker 会在合理时间内完成，等待不会显著增加延迟
- 极端情况下（如目标端写入极慢），等待时间会延长，但这是正确行为——不应在 DML 未完成时执行 DDL
- 新增的 Pause/Shutdown 感知确保运维始终能够中断等待并退出进程

## 优化：移除 barrier 的固定 300ms 等待

原来的 `DDLCheckpointInterval` 同时控制 barrier 检查和 worker 空闲 probe。
当 prepared 事务只有一条业务操作时，每笔事务也会独立下发；如果首次刷新时
worker 还未 ACK，batcher 会先等待 300ms 才重新刷新 checkpoint。
连续小事务因此可能降到约 3 笔/秒，即使目标端已经完成写入。

现在将两个间隔分开：

| 用途 | 行为 |
|------|------|
| Barrier 等待 | Worker ACK 改变时立即唤醒，300ms 定时检查兜底 |
| Checkpoint I/O | 独立 300ms 重试期限；写失败后 ACK 通知不绕过退避 |
| Worker 空闲 probe | 保持 300ms，不随 barrier 检查加快 |
| Checkpoint 读取失败 | 保持 900ms 重试间隔 |
| 长时间 barrier 等待的比较日志 | 按时间限频，每 3 秒输出一次 |

每个 syncer 使用容量为 1 的 channel 合并 ACK 通知；worker 用非阻塞发送，
batcher 暂停或处理较慢时不会阻塞 worker。通知只是重新检查的提示，不能直接
放行 barrier。batcher 重新计算所有相关 worker 的安全 ACK 水位，保留 develop
的两条顺序确认路径：持久化 checkpoint 达到 `newestTs`，或者真实 worker ACK
达到 `newestTs`。ACK 完成不代表 checkpoint 已持久化。

仍有未完成事务 pin 阻挡当前位置时，已满足的 ACK barrier 不发起无关写入。
没有该 pin 时，按 I/O 期限尽力刷新；健康情况下立即读回。checkpoint 写入失败、
读回失败或滞后不能阻止已经完成的 ACK barrier，正常 batcher checkpoint 路径在
存储恢复后继续持久化。没有完成 ACK 时，不得因通知或成功写入但滞后的读回而
提前放行。事务 pin 的注册、释放和持久化保护保持原样。

中间 ACK 只按独立 I/O 期限刷新。仅安全候选水位已达到本次 barrier 目标时，新的
ACK 才能绕过成功写入后的期限，避免多 worker 交错推进放大远端读写；失败写入
的退避不被绕过。没有新 ACK 时，定时检查仍会重试并观察 Pause/Shutdown。
最终一致性验收和重启测试仍须单独确认持久化 checkpoint 覆盖结束 fence，不能
把 ACK barrier 返回成功当成落盘证明。

事务分类、DDL 前后 barrier、Pause/Shutdown 后的 `pendingBarrierTs` 恢复确认
均保留。此优化不改变事务在目标端的执行语义，也不跳过未确认的操作。

回归测试位于 `collector/barrier_test.go`、`collector/worker_test.go`，覆盖：

- 刷新成功后立即确认，无需等待轮询计时器。
- ACK 在进入等待前或刷新后到达，都能唤醒 barrier。
- ACK 通知合并、重复 ACK 不通知、worker 不被通知 channel 阻塞。
- Checkpoint 写入失败、读取失败或读回位置落后时，不提前放行。
- 没有新 ACK 时仍通过定时检查重试。
- Pause/Shutdown 中断后，恢复时重新确认原来的目标位置。
- DDL 和单条 insert 的 prepared 事务保持前序 DML、独立操作、后序 DML 的分批顺序，慢 worker 未确认时 barrier 不放行。
- 将 barrier 兜底间隔改成 1ms，worker probe 仍保持 300ms。

新增 checkpoint I/O 回归位于 `collector/barrier_checkpoint_io_test.go`：

- 本机 HTTP checkpoint 服务实际返回 503，验证无进展和持续新 ACK 下的读写重试限速。
- 阻塞 checkpoint POST，验证真正仍被未提交事务 pin 住的已完成 ACK barrier 不等待写入。
- 成功刷新后的新 ACK 不等待 I/O 重试期限。
- 已完成 pin 仍须清理，健康存储下持久化并读回最终 checkpoint。
- 无 pin、已完成 pin、位于目标之后的 pin：真实 HTTP 写失败不阻塞已完成 ACK。
- 成功写入后的滞后/失败读回不阻塞已完成 ACK，且不伪造持久化状态。
- 存储恢复后，正常 checkpoint 路径能够补齐持久化。
- 8 个 worker 分阶段推进时，中间 ACK 不越过 I/O 期限；最终 ACK 可立即刷新。
- helper cleanup 恢复 barrier 与 I/O 间隔，随机执行顺序不泄漏配置。

这些用例使用真实 HTTP 存储和模拟 worker ACK；不能替代真实 MongoDB、持久化耐久性和故障恢复的完整回归。

### 验证口径与兼容性

CI 显式运行无需 MongoDB 的 ACK/barrier race 回归；真实 MongoDB 测试仍通过
环境变量选择隔离实例，不能以 CI 单测代替真实数据、持久化和故障恢复验收。
这两种启用机制分别服务单元和端到端测试，不要求把已有 live 入口改成 build tag。

`TestBarrierLive` 的可选 3 倍阈值是 60 笔小事务、同样单 worker 的特定旧等待
逻辑对照，计时包含启动与末尾 checkpoint；保留原断言。PR 正文约 2.40 倍来自
另一套至少 60 秒的 develop 混合负载稳态对照，两者不能互相替代或混报。

旧导出名称保留为 deprecated 兼容声明，避免仅因重命名而导致外部源码编译失败。
旧 `DDLCheckpointInterval` 变量仅保留名称与类型；调节新 barrier 兜底间隔应使用
`BarrierCheckpointInterval`，worker probe 不再共享该变量。

相关顺序与 pin 背景：[#990](https://github.com/alibaba/MongoShake/pull/990)、
[#997](https://github.com/alibaba/MongoShake/issues/997)。本次不包含独立事务回放修复。
