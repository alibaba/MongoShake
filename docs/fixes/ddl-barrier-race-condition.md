# 修复：DDL 与 DML 执行顺序竞争条件

## 关联

- 继承自：[fix-ddl-barrier-ordering.md](../fix-ddl-barrier-ordering.md)
- Issue：DDL 和 DML 操作没有按 oplog 顺序同步（部分修复后仍有乱序风险）

## 问题概述

[fix-ddl-barrier-ordering.md](../fix-ddl-barrier-ordering.md) 将 `checkCheckpointUpdate` 的有界循环改为无界循环，解决了"等待超时后继续执行"的问题。但修复不完整，仍存在两种 DDL-DML 乱序场景：

| 场景 | 正常顺序 | Bug 后实际顺序 |
|------|---------|---------------|
| **DDL 先于前序 DML 执行** | `DML_1 → DML_2 → DDL → DML_3` | `DDL → DML_1 → DML_2 → DML_3` |
| **DDL 后于后序 DML 执行** | `DML_1 → DML_2 → DDL → DML_3 → DML_4` | `DML_1 → DML_2 → DML_3 → DML_4 → DDL` |

---

## 根因分析

### 根因 1：`checkCheckpointUpdate` 等待不精确

`checkCheckpointUpdate` 通过比较 `checkpointTs >= newestTs` 来判断 DML 是否执行完成，但这是一种**间接推断**，不精确：

```
时间线：
T1: Batcher 解析 DML_1, DML_2
    → dispatch 到 worker[0], worker[1]
    → worker.unack 更新（入队即更新）
    → DML 尚未执行！

T2: Batcher 检测到 DDL
    → sync.checkCheckpointUpdate(true, newestTs=DML_2.ts)

T3: checkCheckpointUpdate 检查
    → checkpoint.ts 可能来自之前已完成的操作
    → 若历史 checkpoint.ts >= newestTs，立即返回 true
    → ⚠️ 此时 DML_1, DML_2 可能仍在 worker 队列中
```

**问题本质**：Checkpoint 反映的是**历史已完成操作**的时间戳，而非**当前批次 DML** 是否真正执行完成。存在时间戳乱序场景（如延迟到达的 oplog、历史批次已完成）时，等待会错误地立即成功。

---

### 根因 2：DDL 和 DML 在不同 Worker 中并行执行

DDL 固定发送到 `worker[0]`，DML 按 hash 分布到多个 Worker，各 Worker **并行执行**，执行顺序不确定：

```
┌─────────────────────────────────────────────────────┐
│                    Batcher                          │
│                                                     │
│  DML_1, DML_2 ──┬──► worker[0] ──► Executor        │
│                 ├──► worker[1] ──► Executor        │
│                 └──► worker[N] ──► Executor        │
│                                                     │
│  DDL ────────────► worker[0] ──► Executor           │
│                                                     │
│  DML_3, DML_4 ──┬──► worker[1] ──► Executor        │
│                 └──► worker[N] ──► Executor        │
└─────────────────────────────────────────────────────┘

并行执行，顺序不确定：
- worker[0] 可能先执行 DDL
- worker[1] 可能后执行 DML_1
- 无真正的串行保证
```

**问题本质**：DDL 和 DML 在不同执行通道中竞争，无法保证 DDL 在"它前面的所有 DML 之后"且"它后面的所有 DML 之前"执行。

---

### 两个根因的关系

| 场景 | 根因 1 的贡献 | 根因 2 的贡献 |
|------|-------------|-------------|
| **DDL 先于前序 DML 执行** | 等待立即成功，误判 DML 已完成 | worker[0] 执行 DDL 时，其他 worker 的 DML 还在队列 |
| **DDL 后于后序 DML 执行** | 等待期间后续 DML 已被 dispatch | DDL dispatch 时机晚于后续 DML，不同 worker 并行 |

---

## 修复方案

### 核心原则

**DDL 不进 Worker 管道，由 Batcher goroutine 直接同步执行。**

Batcher 是单线程的，DDL 在 Batcher 中同步执行即天然串行，永远不会和 DML 并行。

### 修复步骤

```
1. 检测到 DDL 时，DDL 通过返回值传给 startBatcher（不加入 batchGroup，不 dispatch）

2. startBatcher 收到 DDL 后：
   a. 先 dispatch 当前 batchGroup 中的 DML 到 workers
   b. 等待所有 workers 完成（waitForAllWorkersIdle）
   c. DDL 由 batcher 直接同步执行到目标库（DDLExecutor）
   d. 执行完后更新 checkpoint
   e. 继续处理后续 oplog
```

### 如何解决两个根因

| 根因 | 原始问题 | 修复方案 |
|------|---------|---------|
| `checkCheckpointUpdate` 等待不精确 | 比较 checkpoint 时间戳，可能立即成功 | `waitForAllWorkersIdle()` 直接检查 worker 队列状态，等待真正空闲 |
| DDL/DML 并行执行 | DDL 走 worker[0]，DML 走其他 worker，并行竞争 | DDL 由 batcher 直接执行，天然串行，无并行竞争 |

---

## DML dispatch 与 DDL 处理解耦

原代码将 DML dispatch 和 DDL barrier 耦合在同一个 `else if` 分支：

```go
} else if log, filterLog := batcher.getLastOplog(); log != nil && !allEmpty {
    // DML dispatch 和 DDL 处理耦合
    batcher.dispatchBatches(batchedOplog)
    if barrier {
        // DDL 处理...
    }
}
```

**问题**：如果 DDL 没有前置 DML（`allEmpty=true`），整个分支跳过，DDL 不会被处理。

修复后将两者解耦为独立步骤：

```
Step 1: DML dispatch（独立，不受 allEmpty 影响）
Step 2: DDL barrier（独立，检查 ddlOplogs 非空）
```

---

## 修复前后对比

### 修复前

```
DDL → addIntoBatchGroup → worker[0] ──► Executor ──► Target
                                        ↑
                                    并行执行
                                        ↓
DML → addIntoBatchGroup → worker[N] ──► Executor ──► Target

⚠️ DDL 和 DML 在不同 worker 中并行，顺序不可控
⚠️ checkCheckpointUpdate 等待 checkpoint 时间戳，不精确
```

### 修复后

```
DML → dispatch 到 workers → waitForAllWorkersIdle → 等待完成
                                                          │
                                                          ▼
                                          DDL → DDLExecutor → Target（batcher 直接执行）
                                                          │
                                                          ▼
                                              checkpoint(true, ddlTs)
                                                          │
                                                          ▼
                                              继续处理后续 oplog

✅ DDL 不进 worker 管道，batcher 直接同步执行
✅ waitForAllWorkersIdle 等待所有 DML 真正完成
✅ 天然串行，无竞争条件
```

---

## 非 direct tunnel 降级

对于 `kafka` / `tcp` / `rpc` / `file` tunnel，DDL 仍走 `worker[0]` 执行，保持原有行为。原因：

- 非 direct tunnel 场景下，DDL 在 receiver 端执行，不存在同样的竞争条件
- Receiver 是单点消费，天然串行

---

## 测试

### 单元测试

| 测试 | 覆盖 |
|------|------|
| `TestBatchMore_ReturnsDDLOplogs` | DDL 通过返回值传递，不加入 batchGroup |
| `TestWaitForAllWorkersIdle_AllIdle` | 所有 worker 队列为空且 ack==unack |
| `TestWaitForAllWorkersIdle_SomeBusy` | 部分 worker 队列非空，持续等待 |
| `TestWaitForAllWorkersIdle_PauseInterrupt` | Pause 信号中断等待 |
| `TestExecuteDDLDirectly_DirectTunnel` | Direct tunnel 时调用 DDLExecutor |
| `TestExecuteDDLDirectly_NonDirectTunnel` | Non-direct tunnel 时降级到 worker[0] |

### 集成测试

| Case | 操作序列 | 验证 |
|------|---------|------|
| A | 大量 insert → drop collection | 目标端 collection 应不存在（无残留数据） |
| B | create collection → insert → drop | 无 `Collection already exists` 错误 |
| C | insert → rename → insert | 数据写入正确集合，无丢失 |

---

## 影响范围

- **仅影响 direct tunnel**：Non-direct tunnel 保持原有行为
- **DDL 延迟增加**：DDL 必须等待所有前置 DML 完成，这是正确行为
- **运维可中断**：保留 Pause/Shutdown 感知，运维始终可中断长时间等待

---

## 改动文件清单

| 文件 | 改动 |
|------|------|
| `collector/batcher.go` | 新增 DDLExecutor、waitForAllWorkersIdle、executeDDLDirectly；修改 BatchMore 返回值 |
| `collector/syncer.go` | 解耦 DML dispatch 与 DDL 处理 |
| `collector/batcher_test.go` | 适配 BatchMore 新签名 |
