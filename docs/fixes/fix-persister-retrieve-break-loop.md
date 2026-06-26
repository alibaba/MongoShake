# 修复：oplog_store_disk=true 时增量阶段永远不 apply

## 需求描述

**来源：** [GitHub Issue #976](https://github.com/alibaba/MongoShake/issues/976)

**现象：** 配置 `full_sync.reader.oplog_store_disk = true` 后，全量同步完成、切到增量阶段，oplog 被持续 `get` 并写入磁盘队列，但完全不 apply：

```
[INFO] ------------------------full sync done!------------------------
[INFO] finish document replication, change oplog replication to store disk and apply
[INFO] persister replset[mongos] update fetch status to: store disk and apply
[INFO] [name=mongos, stage=incr, get=167326550, filter=0, write_success=0, tps=0,
       ckpt_times=0, lsn_ckpt={0[0, 0], 1970-01-01 08:00:00}, lsn_ack={0[0, 0], 1970-01-01 08:00:00}]
```

`get` 数持续增长但 `write_success` 始终为 0，`lsn_ckpt`/`lsn_ack` 始终为零值。

**预期行为：** 增量阶段进入 `FetchStageStoreDiskApply` 后，应该从磁盘队列读出 oplog 并 push 到 pending queue 让 worker apply。

## 根因分析

问题在 `collector/persister.go` 的 `retrieve()` 函数（issue 报告者已正确指出）。

`Persister.Start()` 在启用 disk persist 时会启动 `retrieve()` goroutine，其设计意图是：

1. **等待阶段**：fetchStage 为 `FetchStageStoreUnknown` 或 `FetchStageStoreDiskNoApply` 时空转等待
2. **触发阶段**：fetchStage 变为 `FetchStageStoreDiskApply` 时跳出 wait 循环
3. **执行阶段**：跳出后从 `DiskQueue` 读 oplog 推到 pending queue，最后切到 `FetchStageStoreMemoryApply`

但 wait 循环写成了：

```go
for range time.NewTicker(3 * time.Second).C {
    stage := atomic.LoadInt32(&p.fetchStage)
    switch stage {
    case utils.FetchStageStoreDiskApply:
        break                   // ← Go 中 switch 里的 break 只跳出 switch，不跳出 for
    case utils.FetchStageStoreUnknown:
    case utils.FetchStageStoreDiskNoApply:
    default:
        l.Logger.Panicf(...)
    }
}
// ↓ 下面的"读盘 + apply"代码永远不可达
l.Logger.Infof("persister retrieve for replset[%v] begin to read from disk queue ...", ...)
```

Go 的语义里，`switch` 内的 `break` 只跳出 `switch` 块。所以 `for range time.NewTicker(...).C` 永远空转，下面的执行阶段代码完全不可达 → 磁盘里的 oplog 没人读 → `write_success` 永远 0。

## 修复方案

最小改动：给 wait 循环加 `Wait:` label，把 `break` 改为 `break Wait`。与同一个文件第 257 行 `Loop:` + `break Loop` 风格一致。

```go
func (p *Persister) retrieve() {
Wait:
    for range time.NewTicker(3 * time.Second).C {
        stage := atomic.LoadInt32(&p.fetchStage)
        switch stage {
        case utils.FetchStageStoreDiskApply:
            break Wait          // 跳出整个 for
        case utils.FetchStageStoreUnknown:
        case utils.FetchStageStoreDiskNoApply:
        default:
            l.Logger.Panicf("invalid fetch stage[%v]", utils.LogFetchStage(stage))
        }
    }
    // ↓ 现在可达
    l.Logger.Infof("persister retrieve for replset[%v] begin to read from disk queue ...", ...)
    ...
}
```

**改动文件：** `collector/persister.go`（1 处，新增 `Wait:` label + 把 `break` 改为 `break Wait`）

## 影响范围

- 仅影响 `full_sync.reader.oplog_store_disk = true` 配置下的增量阶段启动逻辑
- 默认配置（`oplog_store_disk = false`）不走该路径，不受影响
- 修复后行为与代码注释和原始设计意图一致

## 测试

`go build ./...` 三平台（linux/darwin/windows）通过。`TestBatchMoreApplyOpsInheritsSourceTime`、`TestInject` 通过。

未新增针对 `retrieve()` 的单测：bug 本质是一处 `break` typo，加上 `Wait:` label 后 Go 语法保证 `break Wait` 必然跳出 for（编译器层面拒绝歧义）；为单测一个 break 语句去 mock 整个 `DiskQueue` 收益与成本不匹配，违反 surgical changes 原则。

`go test ./collector/` 整体 timeout 与本修复无关——baseline（移除本修复）同样在 180s 时因 `common/metric.go:121` 的 goroutine 不退出而超时，是预先存在的测试基础设施问题。
