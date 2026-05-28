# MongoShake Prometheus / Grafana 部署指南

本文档说明如何在 Linux 和 macOS（测试环境）中部署 Prometheus 与
Grafana，复用仓库现有的两个监控产物：

- `docs/monitor/prometheus.yml`
- `docs/monitor/mongoshake-dashboard.json`

文档只覆盖 `collector` 侧监控，不扩展到 Alertmanager、`receiver` 或新
dashboard 设计。

## 1. 监控组件与仓库现状

| 组件 | 仓库位置 | 作用 | 备注 |
| --- | --- | --- | --- |
| MongoShake Prometheus 端口 | `conf/collector.conf:24-38` | 默认暴露 `prom.http_port = 9102` | `<= 0` 表示关闭 |
| Prometheus 配置样例 | `docs/monitor/prometheus.yml` | 抓取 MongoShake `/metrics` | 默认抓取 `127.0.0.1:9102` |
| Grafana dashboard | `docs/monitor/mongoshake-dashboard.json` | 展示复制状态、oplog delay、全量进度、队列等 | 依赖 Prometheus 数据源 |
| 配置结构体 | `collector/configure/configure.go:14-19` | `prom.http_port` 是正式配置项 | 和 full / incr 监控端口并列 |
| Prometheus 启动链路 | `cmd/collector/collector.go:105-129` | collector 单独启动 Prometheus HTTP 服务 | `/metrics` 与 REST 监控分离 |
| Prometheus HTTP provider | `common/http.go:24-45` | 实际监听 `:prom.http_port` | 端口 `> 0` 才启动 |
| 指标注册 | `common/metric_prom.go:97-214` | 注册 dashboard 依赖的核心指标 | 包含 Go runtime 指标 |

当前 collector 的监控端口分工如下：

| 配置项 | 默认值 | 用途 |
| --- | --- | --- |
| `full_sync.http_port` | `9101` | 全量阶段 REST 监控接口 |
| `incr_sync.http_port` | `9100` | 增量阶段 REST 监控接口 |
| `prom.http_port` | `9102` | Prometheus `/metrics` |
| `system_profile_port` | `9200` | Go profiling，不给 Prometheus 抓取 |

Prometheus 和 Grafana 只依赖 `prom.http_port`，与 `full_sync.http_port` /
`incr_sync.http_port` 是否开启无关。

## 2. 前置条件

部署前至少确认以下几点：

1. `collector` 已能正常启动，例如：

   ```bash
   ./bin/collector -conf=conf/collector.conf
   ```

2. `conf/collector.conf` 中 `prom.http_port` 为正数。仓库默认值是 `9102`，
   可直接使用。
3. Prometheus 到 MongoShake 的网络可达。若 Prometheus 与 collector 不在
   同一台机器，后续只需要修改 `docs/monitor/prometheus.yml` 里的
   `targets`。
4. 现有 dashboard 依赖 `job="mongoshake"`。`Instance` 变量来自：

   ```promql
   label_values(up{job="mongoshake"}, instance)
   ```

   因此复用 `docs/monitor/prometheus.yml` 时必须保留
   `job_name: "mongoshake"`，只能改 `targets`，不要改 job 名。

## 3. Linux 部署 Prometheus 和 Grafana

本节假设目标环境是通用的 `systemd` Linux 主机，并使用官方二进制目录部署。
以下目录仅为示例，可按运维规范调整。

### 3.1 部署 Prometheus

1. 创建用户和目录：

   ```bash
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin prometheus
   sudo mkdir -p /opt/prometheus /etc/prometheus /var/lib/prometheus
   sudo chown -R prometheus:prometheus /opt/prometheus /etc/prometheus /var/lib/prometheus
   ```

2. 从官方发布页下载与你机器架构匹配的 Prometheus tar 包，解压后把二进制
   和控制台文件拷到目标目录。下面命令中的 `<prometheus-version>` 需要替换
   成实际版本；如果解压目录名与示例不同，请替换成实际目录名：

   ```bash
   tar -xzf prometheus-<prometheus-version>.linux-amd64.tar.gz
   sudo cp prometheus-<prometheus-version>.linux-amd64/prometheus /opt/prometheus/
   sudo cp prometheus-<prometheus-version>.linux-amd64/promtool /opt/prometheus/
   sudo cp -R prometheus-<prometheus-version>.linux-amd64/consoles /opt/prometheus/
   sudo cp -R prometheus-<prometheus-version>.linux-amd64/console_libraries /opt/prometheus/
   sudo chown -R prometheus:prometheus /opt/prometheus
   ```

3. 复制仓库里的 Prometheus 配置：

   ```bash
   sudo cp docs/monitor/prometheus.yml /etc/prometheus/prometheus.yml
   sudo chown prometheus:prometheus /etc/prometheus/prometheus.yml
   ```

4. 按实际 collector 地址修改 `/etc/prometheus/prometheus.yml`。只改
   `targets`，不要改 `job_name`：

   ```yaml
   global:
     scrape_interval: 15s

   scrape_configs:
     - job_name: "mongoshake"
       metrics_path: /metrics
       static_configs:
         - targets:
             - "127.0.0.1:9102"
             - "192.168.1.10:9102"
   ```

5. 创建 `systemd` 服务 `/etc/systemd/system/prometheus.service`：

   ```ini
   [Unit]
   Description=Prometheus
   After=network-online.target
   Wants=network-online.target

   [Service]
   User=prometheus
   Group=prometheus
   Type=simple
   ExecStart=/opt/prometheus/prometheus \
     --config.file=/etc/prometheus/prometheus.yml \
     --storage.tsdb.path=/var/lib/prometheus \
     --web.listen-address=0.0.0.0:9090
   Restart=on-failure
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

6. 启动并设置开机自启：

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now prometheus
   sudo systemctl status prometheus
   ```

### 3.2 部署 Grafana

1. 创建用户和目录：

   ```bash
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin grafana
   sudo mkdir -p /opt/grafana /etc/grafana/provisioning /etc/grafana/dashboards /var/lib/grafana /var/log/grafana
   sudo chown -R grafana:grafana /opt/grafana /etc/grafana /var/lib/grafana /var/log/grafana
   ```

2. 从官方发布页下载与你机器架构匹配的 Grafana OSS 或 Enterprise tar 包，
   解压后放到 `/opt/grafana`。下面命令中的 `<grafana-version>` 需要替换成
   实际版本；如果解压目录名与示例不同，请替换成实际目录名：

   ```bash
   tar -xzf grafana-<grafana-version>.linux-amd64.tar.gz
   sudo cp -R <extracted-grafana-dir>/* /opt/grafana/
   sudo chown -R grafana:grafana /opt/grafana
   ```

3. 创建最小配置文件 `/etc/grafana/grafana.ini`：

   ```ini
   [server]
   http_addr = 0.0.0.0
   http_port = 3000

   [paths]
   data = /var/lib/grafana
   logs = /var/log/grafana
   plugins = /var/lib/grafana/plugins
   provisioning = /etc/grafana/provisioning
   ```

4. 创建 `systemd` 服务 `/etc/systemd/system/grafana.service`：

   ```ini
   [Unit]
   Description=Grafana
   After=network-online.target
   Wants=network-online.target

   [Service]
   User=grafana
   Group=grafana
   Type=simple
   ExecStart=/opt/grafana/bin/grafana server \
     --homepath=/opt/grafana \
     --config=/etc/grafana/grafana.ini
   Restart=on-failure
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

5. 启动并设置开机自启：

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now grafana
   sudo systemctl status grafana
   ```

6. 浏览器访问 `http://127.0.0.1:3000`，按 Grafana 首次启动提示完成管理员账
   号初始化。

## 4. macOS（测试环境）部署 Prometheus 和 Grafana

macOS 只建议用于本地联调和 dashboard 验证，不建议作为长期生产部署方式。

### 4.1 安装并启动

```bash
brew install prometheus grafana
brew services start prometheus
brew services start grafana
```

如果服务已经存在，后续可以用 `restart`：

```bash
brew services restart prometheus
brew services restart grafana
```

### 4.2 替换 Prometheus 配置

Homebrew 安装后的配置目录通常位于：

```bash
BREW_PREFIX="$(brew --prefix)"
echo "${BREW_PREFIX}/etc/prometheus.yml"
echo "${BREW_PREFIX}/etc/grafana"
```

把仓库里的 Prometheus 配置复制到 Homebrew 配置路径：

```bash
BREW_PREFIX="$(brew --prefix)"
cp docs/monitor/prometheus.yml "${BREW_PREFIX}/etc/prometheus.yml"
```

如果 MongoShake 不在本机，编辑 `${BREW_PREFIX}/etc/prometheus.yml` 的
`targets`，保留 `job_name: "mongoshake"` 不变。修改后重启服务：

```bash
brew services restart prometheus
```

Grafana 启动后默认访问地址仍是 `http://127.0.0.1:3000`。

## 5. MongoShake 配置与 Prometheus 抓取配置

### 5.1 确认 MongoShake 暴露 `/metrics`

仓库默认配置已经包含：

```ini
full_sync.http_port = 9101
incr_sync.http_port = 9100
prom.http_port = 9102
system_profile_port = 9200
```

对应说明位于 `conf/collector.conf:24-38`。如果你只想保留 Prometheus 监控，
可以把 `full_sync.http_port` 和 `incr_sync.http_port` 设为 `<= 0`，但
`prom.http_port` 必须保持 `> 0`。

启动 collector 后，以下地址应能返回指标文本：

```bash
curl http://127.0.0.1:9102/metrics | head
```

### 5.2 复用仓库现有 Prometheus 配置

仓库自带的 `docs/monitor/prometheus.yml` 内容很小，默认已经指向：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "mongoshake"
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:9102"]
```

使用时遵循以下规则：

1. 保留 `job_name: "mongoshake"`。
2. 只按实际 collector 地址修改 `targets`。
3. collector 多实例时，在同一个 `targets` 数组里追加多个地址。
4. 若 collector 与 Prometheus 之间有防火墙，确保放行 `prom.http_port`。

### 5.3 当前 dashboard 依赖的核心指标

`common/metric_prom.go:97-214` 已注册 dashboard 依赖的核心指标，常用的有：

| 指标 | 说明 |
| --- | --- |
| `repl_status_code` | 复制状态 |
| `oplog_get_delay` | 最新抓取位置延迟，单位毫秒 |
| `oplog_put_delay` | 最新成功写入或 ACK 位置延迟，单位毫秒 |
| `oplog_get_total` / `oplog_apply_total` / `oplog_success_total` | 增量流量与处理计数 |
| `lsn_ack_lag_seconds` / `lsn_checkpoint_lag_seconds` | ACK / checkpoint 落后秒数 |
| `pending_queue_used` / `logs_queue_used` / `worker_jobs_queued` / `worker_unack_buffer_used` / `persister_buffer_used` | 队列与缓冲区使用情况 |
| `full_sync_collections_total` / `finished` / `processing` / `waiting` | 全量同步进度 |
| `executor_op_total` | 下游直写执行操作数 |
| `go_info` / `go_goroutines` / `go_threads` / `go_memstats_sys_bytes` | Go runtime 指标 |

## 6. Grafana 数据源配置与 dashboard 导入

### 6.1 手工创建 Prometheus 数据源

1. 打开 `http://127.0.0.1:3000`
2. 进入 `Connections` -> `Data sources`
3. 新建 `Prometheus`
4. `URL` 填 `http://127.0.0.1:9090`
5. 保存并测试

### 6.2 手工导入仓库现有 dashboard

1. 进入 `Dashboards` -> `New` -> `Import`
2. 上传 `docs/monitor/mongoshake-dashboard.json`
3. 导入时 Grafana 会要求为 `DS_PROMETHEUS` 选择一个 Prometheus 数据源
4. 把它绑定到上一步创建的 Prometheus 数据源
5. 导入完成后，在顶部 `Instance` 下拉框里选择目标实例

说明：

- 当前 JSON 自带 `__inputs`，名字是 `DS_PROMETHEUS`
- `Instance` 变量查询依赖：

  ```promql
  label_values(up{job="mongoshake"}, instance)
  ```

- 如果你改了 Prometheus 的 `job_name`，这里会直接查不到实例

### 6.3 可选：Grafana provisioning 最小示例

如果你希望自动创建 datasource 和 dashboard，可以使用 provisioning。但由于
仓库里的 dashboard JSON 使用了 `${DS_PROMETHEUS}` 输入变量，做文件
provisioning 时需要先把它替换成固定 datasource UID。

1. 约定 datasource UID 为 `mongoshake-prom`
2. 复制 dashboard JSON 到 Grafana 的 dashboard 目录
3. 把 JSON 中所有 `${DS_PROMETHEUS}` 替换成 `mongoshake-prom`

示例 datasource provisioning：

```yaml
apiVersion: 1

datasources:
  - name: MongoShake Prometheus
    uid: mongoshake-prom
    type: prometheus
    access: proxy
    url: http://127.0.0.1:9090
    isDefault: false
    editable: true
```

示例 dashboard provider：

```yaml
apiVersion: 1

providers:
  - name: MongoShake
    orgId: 1
    folder: MongoShake
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/dashboards/mongoshake
```

Linux 常见目录可放在：

- `/etc/grafana/provisioning/datasources/mongoshake-prometheus.yml`
- `/etc/grafana/provisioning/dashboards/mongoshake-dashboard.yml`
- `/etc/grafana/dashboards/mongoshake/mongoshake-dashboard.json`

macOS Homebrew 环境可按 `$(brew --prefix)/etc/grafana/...` 组织同样的目录结构。

## 7. 验证步骤

### 7.1 验证 MongoShake `/metrics`

```bash
curl http://127.0.0.1:9102/metrics | head
```

预期：

- HTTP 状态码为 `200`
- 返回的是 Prometheus 文本格式
- `head` 输出里通常能看到 `# HELP`、`# TYPE`、`go_` 等开头内容
- 完整输出里应包含 `oplog_`、`repl_status_code`、
  `full_sync_collections_total` 等指标定义或样本

### 7.2 验证 Prometheus 已抓到 MongoShake

```bash
curl 'http://127.0.0.1:9090/api/v1/query?query=up{job="mongoshake"}'
```

如果当前 `curl` 因 URL globbing 报错，可以改为：

```bash
curl -g 'http://127.0.0.1:9090/api/v1/query?query=up{job="mongoshake"}'
```

预期：

- 返回 JSON
- `status` 为 `success`
- `data.result` 非空
- 目标实例正常时，样本值通常为 `"1"`

### 7.3 验证 Grafana dashboard

导入 `docs/monitor/mongoshake-dashboard.json` 后，选择 `Instance`，至少应能看
到以下面板有数据或状态变化：

- 复制状态
- `oplog_get_delay` / `oplog_put_delay`
- `oplog tps`
- Go runtime 指标
- 如果当前运行 `full` 或 `all`，还能看到全量同步进度面板

## 8. 常见故障排查

| 现象 | 排查方向 |
| --- | --- |
| `curl http://127.0.0.1:9102/metrics` 返回连接失败 | 检查 collector 是否启动、`prom.http_port` 是否 `> 0`、端口是否被占用 |
| Prometheus `up{job="mongoshake"}` 为空 | 检查 `prometheus.yml` 的 `targets`、网络连通性、是否误改 `job_name` |
| Grafana 导入后 `Instance` 下拉为空 | 通常是 Prometheus 里没有 `job="mongoshake"` 的 `up` 指标 |
| Grafana 有运行时图表，但全量面板为空 | 若 `sync_mode = incr`，全量同步相关指标为空是正常现象 |
| Linux 服务启动失败 | 用 `systemctl status prometheus` / `systemctl status grafana` 查看错误；重点检查二进制路径、目录权限、端口冲突 |
| macOS `brew services` 启动失败 | 先执行 `brew services list`，再检查 `brew info prometheus` 或 `brew info grafana` 的配置路径和日志提示 |

## 9. 建议的最小落地顺序

1. 启动 MongoShake collector，确认 `http://127.0.0.1:9102/metrics` 可访问
2. 复制并调整 `docs/monitor/prometheus.yml`
3. 启动 Prometheus，确认 `up{job="mongoshake"}` 返回结果
4. 启动 Grafana，创建 Prometheus datasource
5. 导入 `docs/monitor/mongoshake-dashboard.json`
6. 在 dashboard 顶部选择 `Instance`，检查复制状态和 delay 面板是否出数

## 10. 增强指标与常用 PromQL

### 10.1 当前同步阶段

`mongoshake_sync_stage{name,stage}` 用于直接判断当前任务处于全量还是增量阶段：

```promql
# 当前正在全量同步
mongoshake_sync_stage{stage="full"} == 1

# 当前正在增量同步
mongoshake_sync_stage{stage="incr"} == 1
```

`mongoshake_sync_stage_start_time_seconds{name,stage}` 表示阶段开始时间，可计算阶段持续时间：

```promql
# 当前全量阶段持续秒数
time() - mongoshake_sync_stage_start_time_seconds{stage="full"}
```

`mongoshake_sync_state_code{name,stage}` 表示阶段生命周期状态：

| code | 状态 |
| --- | --- |
| 0 | init |
| 1 | running |
| 2 | done |
| 3 | error |
| 4 | stopped |

### 10.2 全量同步表级进度

以下指标可用于确认具体库表的全量同步进度：

| 指标 | 说明 |
| --- | --- |
| `full_sync_collection_status{name,stage,db,collection}` | 表状态：0 等待、1 同步中、2 完成 |
| `full_sync_collection_docs_total{name,stage,db,collection}` | 表总文档数 |
| `full_sync_collection_docs_finished{name,stage,db,collection}` | 已完成文档数 |
| `full_sync_collection_progress_ratio{name,stage,db,collection}` | 表同步进度，范围 0~1 |

常用查询：

```promql
# 正在同步的表
full_sync_collection_status{stage="full"} == 1

# 每张表同步百分比
full_sync_collection_progress_ratio{stage="full"} * 100

# 某个库下每张表同步百分比
full_sync_collection_progress_ratio{stage="full",db="db1"} * 100

# 未完成的表
full_sync_collection_progress_ratio{stage="full"} < 1
```

整体集合进度可用：

```promql
full_sync_collections_progress_ratio{stage="full"} * 100
```

### 10.3 队列利用率

新增 capacity 和 ratio 指标，便于观察堆积：

```promql
pending_queue_used_ratio{stage="incr"}
logs_queue_used_ratio{stage="incr"}
worker_jobs_queue_used_ratio{stage="incr"}
persister_buffer_used_ratio{stage="incr"}
```

示例告警条件：

```promql
pending_queue_used_ratio{stage="incr"} > 0.8
```

### 10.4 TPS 与运行信息

`oplog_success_tps{name,stage}` 暴露内部每秒成功量，和日志中的 `tps` 语义一致。也可以继续使用 counter 计算平滑速率：

```promql
rate(oplog_success_total[1m])
```

运行信息：

```promql
mongoshake_info
mongoshake_uptime_seconds
```

进程级指标由 Prometheus process collector 暴露，常见指标包括：

```promql
process_resident_memory_bytes
process_virtual_memory_bytes
process_start_time_seconds
```
