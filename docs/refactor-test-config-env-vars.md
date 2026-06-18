# Refactor: 测试连接信息改用环境变量注入

## Why

仓库里两个治理点：

1. `unit_test_common/include.go` 中以 `const` 形式硬编码了公司内网测试集群的真实 URI（含明文用户名 / 密码 / 内网 IP / 外网 IP），随源码分发到任何 fork / 镜像仓库都会泄漏。

2. `common/common_test.go` 的 SSL 用例把作者本地的 CA pem 绝对路径写死成字面量，其他开发者跑 `TestMongoConn` 必然 file-not-found。

## What

- `unit_test_common/include.go`：`const` → `var`，统一从环境变量读取；加 `envOr(key, fallback)` helper；新增 `TestCaPem` 作为 CA 路径的统一出口。
- `common/common_test.go`：删掉个人本地路径字面量，改读 `unit_test_common.TestCaPem`；SSL/CA 缺失时 `t.Skip` 而不是假装失败。

### 环境变量列表

| 变量 | 默认 fallback | 说明 |
|---|---|---|
| `MONGOSHAKE_TEST_URL` | `mongodb://localhost:27017` | 普通副本集 URI |
| `MONGOSHAKE_TEST_URL_SSL` | `""` | TLS URI，未设置时 SSL 测试 Skip |
| `MONGOSHAKE_TEST_URL_CONFIG_SERVER` | `""` | 分片集群 config server URI |
| `MONGOSHAKE_TEST_URL_SERVERLESS` | `""` | serverless tenant URI |
| `MONGOSHAKE_TEST_URL_SHARDING` | `""` | mongos URI |
| `MONGOSHAKE_TEST_CA_PEM` | `""` | CA chain pem 文件路径 |

只有 `MONGOSHAKE_TEST_URL` 给了 `localhost:27017` fallback —— 这是开发者本地最常见的形态。其余项空值，连接型用例自检并 `t.Skip`，避免在 CI / 无环境的机器上爆出无意义失败。

### 本地开发 / CI 使用方式

本地使用 `.env.test`（仓库根目录，已加入 `.gitignore`，不入仓）保存自己的连接信息：

```bash
source .env.test && go test ./...
```

或临时手动导：

```bash
export MONGOSHAKE_TEST_URL='mongodb://...'
export MONGOSHAKE_TEST_URL_SSL='mongodb://...'
export MONGOSHAKE_TEST_CA_PEM='/path/to/ca.pem'
go test ./...
```

CI（不依赖外部 mongo）：不导任何 env var，连接型用例自动 Skip。

## Scope（本次不做）

- **git history 脱敏**：相关内网 / 外网 IP 已经存在于历史 commit 中。`git filter-repo` 重写历史需要强推 + 所有协作者重新 clone，风险大于收益。本次只阻断后续泄漏；如果安全团队后续要求清理历史，单独立项。
- **其他 `mongodb://...` 字面量**：`community_client_test.go` 是 `BlockMongoUrlPassword` 的输入数据（不是连接串），`sanitize_test.go` / `collector_test.go` / `prom_metrics_test.go` 是占位字符串走单元逻辑分支 —— 这些不是"硬编码环境信息"，本次不动。

## Verification

```bash
go build ./...
go vet ./...
# 未导 env var 时，连接型 SSL 用例应输出 SKIP 而非 FAIL
go test ./common -run TestMongoConn -v
```
