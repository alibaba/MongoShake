# 字段级（顶层字段）过滤同步 —— 使用与配置指南

本特性给 MongoShake 增加"集合顶层字段白名单"，用于把源端宽集合**只同步选定的若干顶层字段**到目标端（集合字段级拆分）。全量用服务端 projection，增量（仅 change stream）对事件做字段投影，索引按白名单 all-or-nothing 过滤。

- 设计文档（SDD）：`docs/superpowers/specs/2026-09-22-field-level-sync-design.md`
- 实现计划（TDD）：`docs/superpowers/plans/2026-09-22-field-level-sync.md`
- 引入版本：develop（commits `282e882`..`bdf82c0`）

---

## 1. 配置项

两个**独立**参数，分别控制全量与增量阶段。格式与 `incr_sync.executor.dup_key_skip_rules` 一致：不同 ns 用 `;` 分隔，ns 与字段用 `:` 分隔，字段之间用 `,` 分隔。

```ini
# 全量阶段顶层字段白名单；留空 = 不过滤（同步全部字段）
full_sync.field.whitelist = db1.orders:userId,amount,status;db1.users:userId,name

# 增量阶段顶层字段白名单；留空 = 不过滤
incr_sync.field.whitelist = db1.orders:userId,amount,status;db1.users:userId,name
```

### 字段语义（务必理解）

- **只支持顶层字段名**，字段名**不能含 `.`**（含点会在启动校验时报错）。
- 选中一个顶层字段 → 同步它的**整个值**：即使该字段是对象或数组，也整体保留（服务端 inclusion projection 返回整棵子树）。
- **不支持只挑子字段**（如只要 `profile.city` 而丢掉 `profile.phone`）。增量 update 按"路径**首段** ∈ 白名单"保留，所以选中 `profile` 时，源端 `$set:{"profile.city":x}` 也会正确同步。
- `_id` 始终保留，不要写进白名单。

### 强制约束（启动期 sanitize 校验，违反即拒绝启动）

1. **必须先配置 `filter.namespace.white`**（白名单模式）。字段白名单里的每个 ns 都必须被 `filter.namespace.white` 覆盖（white 可写 `db` 级或 `db.coll` 级）。换句话说：**namespace 过滤的集合范围必须是字段过滤规则集合的超集**。未覆盖会报错并列出具体 ns。
   - 仅配置 `filter.namespace.black` 或两者都不配 → 报错。
2. **增量字段白名单要求 `incr_sync.mongo_fetch_method = change_stream`**。oplog 模式不支持字段过滤，配了会报错。
3. **同一 ns 若同时出现在全量与增量白名单，字段集必须完全一致**，否则报错（避免全量/增量投影不一致导致目标残留或缺字段）。允许某 ns 只在全量或只在增量出现。

### 索引过滤（all-or-nothing）

一个索引只有当其 key 中**所有非 `_id` 字段的首段都在白名单内**时才会同步到目标，否则整条索引跳过。复合索引"既有白名单字段又有非白名单字段" → **不保留**。

原因：若复合索引含被过滤掉的字段，目标端该字段恒缺失；对 **unique** 复合索引会退化成对剩余字段唯一，导致源端合法文档在目标端触发**假重复键冲突**。这是数据正确性问题，故 v1 采取最保守的整条跳过。

### 完整 conf 片段示例

```ini
sync_mode = all
mongo_urls = mongodb://src-host:27017
tunnel = direct
tunnel.address = mongodb://dst-host:27017

# 字段过滤依赖 white 名单先圈定集合范围
filter.namespace.white = db1.orders;db1.users

# 增量字段过滤只支持 change stream
incr_sync.mongo_fetch_method = change_stream
incr_sync.change_stream.watch_full_document = false

# 全量 / 增量各自的字段白名单（同一 ns 字段集必须一致）
full_sync.field.whitelist = db1.orders:userId,amount,status;db1.users:userId,name
incr_sync.field.whitelist = db1.orders:userId,amount,status;db1.users:userId,name
```

效果：目标端 `db1.orders` 文档只含 `{_id, userId, amount, status}`；只覆盖这些字段的索引会建到目标，涉及其它字段的索引被跳过。

---

## 2. 行为速查

| 源端操作 | 目标端结果（白名单 = {a, profile}） |
|---|---|
| insert `{_id,a,secret,profile}` | 写入 `{_id,a,profile}`，丢弃 `secret` |
| update `$set:{a:1}` | 应用 `a=1` |
| update `$set:{secret:1}` | **无变化**（事件被丢弃） |
| update `$set:{"profile.city":x}` | 应用（首段 `profile` 命中），整 `profile` 子树保持最新 |
| replace 整文档 | 目标变为投影后的整文档 |
| delete | 目标文档删除 |
| createIndex（含非白名单字段） | 跳过该索引 |

> 目标端写入采用整文档/`$set` 覆盖语义，**假定目标集合由同步任务独占**。若目标已存在非白名单的历史字段，本特性不会主动清理（见 SDD 第 10 节"已知限制"）。建议配合 `full_sync.collection_exist_drop = true` 或全新目标集合使用。

---

## 3. 运行测试

### 3.1 单元 / live-mongo 单测（推荐先跑）

纯逻辑单测无需 Mongo：

```bash
go test ./collector/filter ./cmd/collector
```

全量投影 / 索引过滤的 live-mongo 单测需要一个可连的 MongoDB（用本地 docker 源副本集即可，见 `AGENTS.md`）：

```bash
MONGOSHAKE_TEST_URL=mongodb://127.0.0.1:27030 \
  go test ./collector/docsyncer -run 'TestDocumentReaderFieldProjection|TestStartIndexSyncFieldWhitelist' -v
```

覆盖：
- `cmd/collector`：`TestParseFieldWhitelists`（格式、含点拒绝、ns 未覆盖、全量↔增量一致性、change_stream 强制）。
- `collector/filter`：`TestFirstComponent`/`TestBuildInclusionProjection`/`TestProjectDocument`/`TestFilterModifiers`/`IndexSpecCovered`/`TestFieldFilter`/`TestFieldFilterCreateIndexes`。
- `collector/docsyncer`：`TestDocumentReaderFieldProjection`（全量投影）、`TestStartIndexSyncFieldWhitelist`（索引 all-or-nothing）。

### 3.2 端到端集成测试

依赖本地 docker 拓扑（`AGENTS.md`）：源副本集 `mshake-cs-src`（27030）+ 目标 `mshake-txn-tgt`（27019）。

```bash
docker start mshake-cs-src mshake-txn-tgt
go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m
```

可用环境变量覆盖：`MSHAKE_CS_SRC_URL`、`MSHAKE_TGT_URL`、`MSHAKE_COLLECTOR_BIN`。

collector 的启动方式（`ffStartCollector`）：
- 设了 `MSHAKE_COLLECTOR_BIN` → 直接 exec 该预构建二进制（CI/正常机器推荐）。
- 未设 → 用 `go run ./cmd/collector` 启动，并以独立进程组拉起，`kill()` 时连同子进程一起回收。

---

## 4. 本机环境注意事项（重要）

在某些开启了端点安全/网络管控的开发机（例如带 AliLang Network Extension 的 Mac）上，集成测试可能无法在 shell 内跑绿，原因有二，均**与本特性代码无关**：

1. **直接执行新构建的二进制被 SIGKILL**：`go build -o collector ./cmd/collector && ./collector` 会被立即 kill（连 hello-world 都 137）。因此默认的 `MSHAKE_COLLECTOR_BIN=go build` 路径在本机不可用——这正是 harness 增加 **`go run` 兜底**的原因（`go run` 由 go 工具作为父进程拉起子进程，可正常运行）。
2. **`go test` 深层嵌套 `go run` 的 change stream 长轮询会 stall**：collector 作为 `go test → go run → collector` 的孙进程时，change stream 能投递起始事件，但随后新的增量事件不再送达（疑似长轮询 getMore 受本机网络策略影响）。普通 find（全量扫描）不受影响，所以全量与索引过滤在测试里能通过，增量步骤超时。

### 在本机如何验证增量行为（绕过办法）

直接从 **shell** 用 `go run` 启动 collector（不要嵌在 `go test` 里），再手动改源端观察目标端：

```bash
# 1) 准备 conf（关键字段）：sync_mode=all 或 incr、change_stream、filter.namespace.white、
#    full_sync.field.whitelist / incr_sync.field.whitelist、tunnel.kafka.producer.max_message_bytes 用纯数字
# 2) 启动（前台或后台均可）
go run ./cmd/collector -conf=/path/to/collector.conf -verbose=2

# 3) 另开终端，对源端做写操作，观察目标端
docker exec mshake-cs-src mongosh --quiet --port 27030 \
  --eval 'db.getSiblingDB("db1").orders.updateOne({_id:1},{$set:{amount:99}})'
docker exec mshake-txn-tgt mongosh --quiet \
  --eval 'db.getSiblingDB("db1").orders.findOne({_id:1})'
```

> 提示：`go run` 以仓库根为 CWD，会在仓库根生成 `diagnostic/` 运行日志目录，验证完可 `rm -rf diagnostic`。

### CI / 正常机器

不存在上述两条限制：`go build -o` 的二进制可正常 exec，change stream 长轮询正常。设置 `MSHAKE_COLLECTOR_BIN` 指向预构建二进制（或留空走 `go run`）即可，`TestFieldFilterSync` 应跑绿。

---

## 5. 已知限制 / 后续优化

详见 SDD 第 10 节。摘要：v1 不支持子字段挑选（`profile.city`）、数组内嵌字段（`items.sku`）、字段名含点、复合索引改写、目标残留字段清理、白名单热变更、oplog 模式、通配 ns。这些列为后续优化点。
