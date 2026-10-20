# Barrier 本机端到端测试

这个测试运行真实 collector，使用 MongoDB 中真实的 prepared 事务和持久化
checkpoint，验证 barrier 性能和顺序保障。未设置环境变量时跳过。

准备两套隔离的本机 MongoDB，源端必须是名为 `barrier_src` 的副本集。
测试 collector 配置显式启用 `incr_sync.barrier.ordering_enable = true`。
两套服务都需要以 `--setParameter enableTestCommands=1` 启动；源端用于
`prepareTransaction`，目标端用于 `failCommand`。只在测试实例启用这些选项。
测试拒绝非回环地址，为每个场景创建独立数据库，并在结束时清理。

```sh
export MONGOSHAKE_BARRIER_SOURCE='mongodb://127.0.0.1:SOURCE_PORT/?directConnection=true'
export MONGOSHAKE_BARRIER_TARGET='mongodb://127.0.0.1:TARGET_PORT/?directConnection=true'
export MONGOSHAKE_TEST_COLLECTOR_BINARY='/absolute/path/to/native/collector'
export MONGOSHAKE_BARRIER_OUTPUT='/absolute/path/to/test-results'

# 可选：只恢复旧 barrier 等待逻辑的对照程序。
export MONGOSHAKE_BARRIER_BASELINE='/absolute/path/to/baseline/collector'

go test ./tests/integration/barrier -run '^TestBarrierLive$' -count=1 -v -timeout=180s
```

测试场景：

- 60 笔单条 insert 的 prepared 事务，每笔先 prepare 再 commit；源端 oplog
  必须有 60 条 prepare 和 60 条 commit。验证目标端每条文档的内容。
- 可选旧逻辑单 worker 对照，以及新逻辑的单 worker、双 worker 回放。
  只有对照存在时，才要求新逻辑在同样单 worker 下至少快 3 倍。
- 双 worker 回放 1000 条 insert、drop、再插入标记文档，并用 failpoint
  阻塞一次目标端 insert 1000ms。最终只能留下 drop 后的标记文档。
- 目标 insert 阻塞 1500ms 时暂停 batcher，观察 barrier 中断日志，恢复后
  观察 pending barrier 重新确认日志，验证所有事务完成。
- 目标端第一次 checkpoint update 返回错误，验证错误日志存在，后续重试
  能完成所有事务。

所有场景末尾都创建一个 DDL fence，必须等持久化 checkpoint 到达该 fence
才验收目标端数据，避免把“最后一条文档出现”误当成整个 pipeline 已完成。
耗时包含 collector 启动、读取、目标写入和 checkpoint IO，不含造数。
原始日志、配置和 JSON 报告写入 `MONGOSHAKE_BARRIER_OUTPUT`。

测试程序与 collector 必须能在执行主机上运行。macOS 原生执行通过，不代表
Linux 二进制已经在 Linux 或 Docker 中运行通过。
