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

运维操作方式：`POST http://<host>:<system_profile_port>/sentinel/options`，body 为 `{"Pause":true}` 或 `{"Shutdown":true}`。

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
