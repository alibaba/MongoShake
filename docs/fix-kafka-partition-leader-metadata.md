# 修复：Kafka 分区 leader 分布不同节点时增量同步报错

## 需求描述

**来源：** GitHub Issue #907

**现象：** 当 Kafka topic 的分区 leader 分布在不同 broker 节点时，MongoShake 增量同步写入 Kafka 报错：
```
kafka server: Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date.
```

当所有分区 leader 在同一个 broker 时正常同步。

**版本：** mongoshake 2.6.4/2.8.2，Kafka 3.2.1

## 根因分析

`tunnel/kafka/common.go` 中 Kafka 客户端配置存在三个问题：

1. **协议版本过旧**：`config.Version = sarama.V0_10_0_0` 硬编码了 Kafka 0.10.0 的协议版本。与现代 Kafka 3.x 集群交互时，使用旧版本协议获取的 metadata 可能不完整或无法正确路由到不同 broker 上的分区 leader。

2. **无 metadata 刷新机制**：未配置 `Metadata.RefreshFrequency`，默认 10 分钟。当 metadata 初始获取不完整或分区 leader 切换时，无法及时更新。

3. **Producer 重试不足**：未配置 `Producer.Retry.Max`（默认 3 次）和 `Producer.Retry.Backoff`（默认 100ms），在 metadata 刷新完成前重试次数就已耗尽。

## 修复方案

1. **新增配置项 `tunnel.kafka.version`**：允许用户指定 Kafka 协议版本。默认保留 `0.10.0.0`（向后兼容），未配置时输出 WARN 日志提醒用户显式设置。Issue #907 用户只需配置 `tunnel.kafka.version = 2.1.0`（或更高）即可解决问题。
2. **添加 metadata 刷新频率**：`config.Metadata.RefreshFrequency = 3 * time.Minute`
3. **增强 producer retry**：`config.Producer.Retry.Max = 10`，`config.Producer.Retry.Backoff = 500 * time.Millisecond`

### 为什么默认值保留 0.10.0.0 而非升级到 2.1.0

存量用户如果使用 Kafka 0.10.x/1.x 集群且未配置 `tunnel.kafka.version`，升级 MongoShake 后协议版本从 0.10.0 跳到 2.1.0 可能导致连接失败。保留旧默认 + WARN 日志是最安全的向后兼容策略。

## 修改文件

- `collector/configure/configure.go` — 新增 `TunnelKafkaVersion` 配置字段
- `tunnel/kafka/common.go` — 修改 `NewConfig()` 实现
- `conf/collector.conf` — 新增配置项文档

## 验证

1. `go build ./...` 编译通过
2. `go test ./tunnel/...` 测试通过
3. 连接多 broker Kafka 集群（分区 leader 分布在不同节点）验证同步正常
