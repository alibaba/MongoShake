# 修复：oplog_store_disk=true 重启场景下 persister.retrieve goroutine panic

## 关联

- 本次为 [#976](https://github.com/alibaba/MongoShake/issues/976) 的 follow-up 修复，与上一个 spec [`fix-persister-retrieve-break-loop.md`](./fix-persister-retrieve-break-loop.md) 一并解决 `full_sync.reader.oplog_store_disk = true` 配置下增量同步的可靠性问题。
- 隐藏 bug 在 review 上一个修复（`b590dec`）时通过追状态机调用方发现，并非由该修复引入。

## 需求描述

**现象：** 开启 `full_sync.reader.oplog_store_disk = true`，MongoShake 完成一轮全量+增量同步后**重启**，且重启时 checkpoint 已经追上 `OplogDiskQueueFinishTs`（也就是磁盘里残留的 oplog 全部应用过了）。

启动后 `Persister.retrieve()` goroutine 在第一次 3 秒 ticker fire 时通过 `l.Logger.Panicf("invalid fetch stage[%v]", ...)` 直接 panic，导致进程退出无法启动。

## 根因分析

调用顺序（`collector/syncer.go:211-235`）：

```
OplogSyncer.Start()
├─ sync.persister.Start()       // line 218 → 启动 retrieve goroutine
│                               //   只看 enableDiskPersist (构造时算定，依赖 conf.Options.FullSyncReaderOplogStoreDisk)
└─ sync.loadCheckpoint()        // line 233 → 之后才设置 fetchStage 和 (可选) InitDiskQueue
```

`loadCheckpoint()` 在 `collector/checkpoint.go:62-66` 有一条分支：

```go
if checkpoint.OplogDiskQueueFinishTs > 0 && checkpoint.Timestamp >= checkpoint.OplogDiskQueueFinishTs {
    // no need to init disk queue again
    sync.persister.SetFetchStage(utils.FetchStageStoreMemoryApply)
    return nil
    // ⚠ 没有 InitDiskQueue，DiskQueue 仍为 nil
}
```

但 `enableDiskPersist` 在 `NewPersister()` 时就已经算出 `true`（仅看 `FullSyncReaderOplogStoreDisk` 配置，不看运行时状态），所以 `Persister.Start()` 依然启动了 `retrieve` goroutine。3 秒后 goroutine 第一次 `atomic.LoadInt32(&p.fetchStage)` 读到 **`FetchStageStoreMemoryApply`**：

```go
switch stage {
case utils.FetchStageStoreDiskApply:    // 不匹配
case utils.FetchStageStoreUnknown:      // 不匹配
case utils.FetchStageStoreDiskNoApply:  // 不匹配
default:
    l.Logger.Panicf("invalid fetch stage[%v]", ...)   // ← 直接 panic
}
```

这正是 `checkpoint.go:68` 上 `// TODO, there is a bug if MongoShake restarts` 注释所暗示场景之一。

**修复前 #976 的关系：** 上一个修复（`b590dec`）解决的是 `case FetchStageStoreDiskApply: break` 跳不出 for-range 的死循环 bug；本次的 panic 在该修复前后行为一致（default 分支始终 Panicf），并非由 `break Wait` 改动引入，是一个独立但同函数内的设计漏洞。

## 修复方案

在 `retrieve()` 的 wait 循环中显式处理 `FetchStageStoreMemoryApply`：直接 `return` 让 goroutine 安全退出，避免落入下方依赖 `p.DiskQueue` 非 nil 的代码段。

```go
Wait:
    for range time.NewTicker(3 * time.Second).C {
        stage := atomic.LoadInt32(&p.fetchStage)
        switch stage {
        case utils.FetchStageStoreDiskApply:
            break Wait
        case utils.FetchStageStoreMemoryApply:
            // loadCheckpoint() may set MemoryApply directly (without InitDiskQueue)
            // when restart and checkpoint has already caught up to disk last ts.
            // In that path DiskQueue is nil, so we must exit instead of falling
            // through to the disk-read stage below.
            l.Logger.Infof("persister retrieve for replset[%v] skip disk replay: fetchStage is MemoryApply",
                p.replset)
            return
        case utils.FetchStageStoreUnknown:
        case utils.FetchStageStoreDiskNoApply:
        default:
            l.Logger.Panicf("invalid fetch stage[%v]", utils.LogFetchStage(stage))
        }
    }
```

**改动文件：** `collector/persister.go`（仅在 wait 循环中新增一个 case 分支）

## 影响范围

- 仅影响 `full_sync.reader.oplog_store_disk = true` 且 **进程重启** 后 checkpoint 已追上 disk queue 的场景
- 正常启动路径（无 checkpoint → DiskNoApply → DiskApply）行为不变
- 不修改外部 API、不修改状态机定义，仅让 retrieve goroutine 对一个本就合法但未处理的状态做正确响应

## 测试

- `go build ./...` 三平台通过
- 未新增针对 `retrieve()` 的单测；理由同上一个 spec：mock 整个 DiskQueue/状态机来覆盖一个 case 分支收益与成本不匹配。若后续要在 `retrieve()` 之上叠加更多逻辑，建议先抽出 `waitForDiskApply()` helper 再补测

## 遗留问题与建议的新 Issue 草稿

本修复消除了重启 panic，但 `retrieve()` 还有几处**与本次改动无关、但同函数内可优化**的点，建议另开 issue 跟进：

1. `for range time.NewTicker(3*time.Second).C` 与下方 `ticker := time.NewTicker(time.Second)` 均未 `Stop()`，break/return 后会在 timer heap 中残留至函数返回
2. wait 循环采用 3 秒 ticker 轮询而非 channel 通知，状态变更后最长需等 3 秒才感知；可考虑由 `SetFetchStage` 通过 `chan struct{}` 主动唤醒
