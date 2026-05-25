# 修复：增量同步过程中阻塞在 E11000 duplicate key error

## 需求描述

**来源：** 使用版本 2.8.7。

**现象：** 增量同步过程中，同步一直卡在同一条记录上，不断报 `E11000 duplicate key error`，且报错的索引不是 `_id`，而是业务唯一索引（如 `account_1`）。用户已经开启了 `incr_sync.executor.upsert = true` 和 `incr_sync.executor.insert_on_dup_update = true`，但问题仍然存在。

**Workaround：** 在目标端手动删除冲突文档可以临时解决阻塞，但会引发其他报错，不是合理方案。

## 根因分析

问题出在 `executor/db_writer_single.go` 的 `doUpdateOnInsert()` 方法中的 upsert 分支：

1. insert oplog 写入目标端 → 因唯一索引（如 `account_1`）冲突报 E11000
2. MongoShake 将 insert 转为 upsert（按 `_id` 做 filter）
3. 但目标端存在一条**不同 `_id`、相同 `account` 值**的文档 → upsert 尝试插入新文档时，再次触发 `account_1` 索引冲突
4. 代码中对这种「非 `_id` 唯一索引冲突」没有处理逻辑，直接返回错误 → 上层不断重试 → 同步阻塞

## 修复方案

**核心思路：通过统一策略配置处理重复键冲突**

新增配置项 `incr_sync.executor.dup_key_strategy`（默认 `error`），可选值：

- `error`：保持原有行为，阻塞报错。
- `delete_and_retry`：源端为准，删除冲突文档并重试 upsert。
- `skip`：目标端为准，按 `incr_sync.executor.dup_key_skip_rules` 白名单跳过并记录重复 oplog。

`skip` 策略需要配置白名单规则，格式为 `db.collection:index1,index2;db.collection:*`，用于限定可跳过的 namespace 和 index，避免扩大影响范围。

当配置为 `delete_and_retry` 时：

1. **解析 E11000 错误的 `dup key: { ... }` 部分**，提取冲突字段名（如 `account`、`a, b`）
2. **判断**：如果是 `_id_` 索引，走已有逻辑；如果是非 `_id` 唯一索引，进入新逻辑
3. **从源文档中提取对应字段值**，构造精确的冲突 filter（支持复合索引、dotted path）
4. **删除冲突文档** → **重试原来的 upsert**

### 为什么从 dup key 错误信息解析字段名

E11000 错误格式为：
```
E11000 duplicate key error collection: db.coll index: a_1_b_1 dup key: { a: "x", b: "y" }
```

`dup key: { ... }` 部分直接包含冲突的字段名，无需从索引名推断。相比其他方案：

- **vs 正则解析索引名**：索引名有歧义（复合索引 `a_1_b_1`、自定义命名、字段名含数字后缀）
- **vs listIndexes**：需要额外 DB 查询，增加延迟和权限依赖

从错误信息直接解析字段名是最简单可靠的方式，天然支持复合索引和自定义索引名。

### 为什么默认关闭

删除目标端文档是不可逆操作。在双端有流量的场景下（如灰度迁移、双写），目标端的冲突文档可能是目标端应用主动写入的有效数据，自动删除会导致线上故障。

仅在「以源端为准」的单向同步场景下安全。

## 改动文件

| 文件 | 改动 |
|------|------|
| `collector/configure/configure.go` | 新增 `IncrSyncExecutorDupKeyStrategy` 配置字段 |
| `conf/collector.conf` | 新增配置项及说明 |
| `executor/dup_key_resolver.go` | 新增：`parseDupKeyIndexName()`、`parseDupKeyFields()`、`resolveConflictFilter()`、`deleteConflictAndRetry()` |
| `executor/db_writer_single.go` | 修改 `doUpdateOnInsert()` 的 upsert 分支，配置开启时调用 resolver |
| `executor/db_writer_command.go` | 新增 `retryUpdateOnInsertIndividually()` 和 `runSingleUpdateCmd()`，gid 场景逐条重试并保留 metadata |
| `executor/db_writer_test.go` | 新增测试：parseDupKeyIndexName、getFieldValue、splitDotted、集成测试（开启/关闭） |

## 配置说明

```ini
# 当 insert_on_dup_update 的 upsert 仍因重复键冲突失败时的处理策略。
# error            默认，保持原有行为，阻塞报错。
# delete_and_retry 源端为准，删除目标端冲突文档后重试；双端有流量时请勿开启。
# skip             目标端为准，按白名单跳过 oplog 并记录重复日志。
incr_sync.executor.dup_key_strategy = error
# 仅当 dup_key_strategy = skip 时生效。
# 规则格式：db.collection:index1,index2;db.collection:*
incr_sync.executor.dup_key_skip_rules =
```

## 验证

1. `go build ./...` 编译通过
2. `go test ./executor -run TestParseDupKeyIndexName` 等单元测试通过
3. 集成测试需要 MongoDB 实例：`go test ./executor -run TestSingleWriterDeleteOnNonIdDupKey`
