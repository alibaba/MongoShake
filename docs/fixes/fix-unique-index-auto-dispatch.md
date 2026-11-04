# 修复：`shard_key=auto` 改为按 namespace 判定唯一索引，并跟随 DDL 切换

## 需求描述

**来源：** [GitHub Issue #1015](https://github.com/alibaba/MongoShake/issues/1015)（自 #1002 拆出）

**现象：** `incr_sync.shard_key = id` 时按 `_id` 哈希分发，同一唯一键值上的跨文档操作没有全序，目标端可能把"占用唯一键的插入"排在"释放该键的更新"之前，得到 `E11000`。默认 `incr_sync.executor.dup_key_strategy = ignore` 把这条写入错误吞掉，**文档就此丢失**，偏差随时间累积。

**最小复现**（字段 `a` 上有唯一索引）：

```
1) insert {_id: 1, a: 1, b: 1}
2) update {_id: 1}, {$set: {a: 2}}
3) insert {_id: 2, a: 1, b: 2}
```

期望目标端有 `{a:1,b:2}` 与 `{a:2,b:1}` 两条；`id` 模式下第 3 步可能在目标端先于第 2 步执行，插入撞上仍持有 `a=1` 的文档而被丢弃，最终只剩一条。

**验收标准**（issue 原文 1–4 条）：给出方案与端到端测试（红绿对照）、明确多/复合/稀疏唯一索引下的行为边界、量化并发度损失、不破坏现有三种 `shard_key` 的语义与默认行为。

## 根因分析

### 1. 为什么"按 unique index 字段分发"这个方向不可行

issue 里列的方向是"按唯一索引字段参与分发 hash"。静态哈希做不到，有两个独立的反例：

**反例一：update 的旧值不可得。** oplog 的 update 是 `{op:"u", o:{$v:2, diff:{u:{a:2}}}, o2:{_id:1}}`，`o2` 只有 `_id`，`o` 只有新值；change stream 的 `updateDescription.updatedFields` 同样只有新值。因此"把 `a:1→2` 路由回 `a=1` 的分区"没有输入。

**反例二：只按新值分发会破坏文档内顺序。** 同一文档的 insert 按唯一键路由、update 按 `_id` 路由，二者落在不同 worker。若 update 先执行，目标端该文档尚不存在，update 变成 no-op 丢失；随后 insert 成功，于是目标端同时存在两条相同唯一键的文档——比原缺陷更严重。

结论：**只要唯一键的值可以在文档间移动，任何静态分区都无法同时保证"同一唯一键全序"和"同一文档全序"**。可行的方案只有两种：运行期锁管理器（本文不涉及），或把该集合整体串行。

### 2. 现有 `auto` 的两个缺陷

`auto` 原本在启动时扫一次索引，**全局**决定用 `id` 还是 `collection`（`collector/coordinator/replication.go`）：

- **粒度太粗**：任意一个集合带唯一索引，就把所有集合都退化成按 namespace 哈希，没有唯一索引的集合白白丢掉 `_id` 级并发。
- **对 DDL 失明**：判定只做一次。启动后 `createIndexes` 建了唯一索引，`auto` 仍然按 `_id` 分发，静默产生第 1 节的丢失。

`executor/collision_matrix.go` 里的 `BarrierMatrix` 依赖 `PartialLog.UniqueIndexes`（`uk` 字段）切段，但全仓库没有任何地方填充该字段（`change_stream_event.go` 明确写着 "useless in change stream"），该路径恒等价于 no-op，不能复用。

## 方案

`incr_sync.shard_key = auto` 改为**按 namespace 判定 + DDL 感知**，新增 `oplog.UniqueIndexAwareHasher`：

| 集合类型 | 分发方式 | 保证 |
|---|---|---|
| 带 `_id` 之外的唯一索引 | 按 namespace 哈希（同 `collection`）：整表落在同一个 worker | 集合内全序，覆盖任意的索引形态 |
| 其余 | 按 `_id` 哈希（同 `id`）：保留文档级并发 | 与原 `id` 语义一致 |

- **启动期**：`common.MongoCommunityConn.ListUniqueIndexNamespaces` 逐库逐集合读取 `listIndexes`，返回"unique 且首列不是 `_id`"的 namespace。首列是 `_id` 的唯一索引不增加约束，不计入。分片源逐 shard 取并集，mongos 源（change stream）单独扫 mongos。**元数据读取失败时回退成 `collection`**（全部有序），不猜；扫描期间被 drop 的集合（`NamespaceNotFound`）跳过——它不会再有 oplog。
- **运行期**：DDL barrier 处把 namespace 切到有序回放，覆盖四种 oplog 形态：
  - `createIndexes` + `indexes` 数组（集合已有数据）；
  - `createIndexes` 单索引平铺（**集合为空**时 MongoDB 不写 `indexes` 数组，而是把 `unique/key/name` 平铺在 `o` 顶层；`executor/db_writer.go` 回放时也要把它包回数组）；
  - `commitIndexBuild`（非空集合的两阶段建索引；`startIndexBuild` 在 `AutologousFilter` 里被当作可忽略命令过滤掉了，实际由 `commitIndexBuild` 触发）；
  - `collMod` + `index.unique: true`（MongoDB 7.0+ 的「把已有索引转成唯一」，oplog 里只有索引名、没有 keyPattern，因此一律按"增加约束"处理）。
- **重命名**：`renameCollection` 的源 namespace 有序时，目标 namespace 一并切为有序（唯一索引随集合走）。
- **只增不减**：`dropIndex` / `drop` 不回切。索引删除后仍按有序回放只是变慢，而回切可能让在途 oplog 重新乱序。

### 前提条件（不满足时不会静默出错）

| 前提 | 不满足时的行为 |
|---|---|
| `incr_sync.executor = 1`（默认） | 一个 worker 的批次会按 `_id` 拆给多个 executor，整表有序被打破；启动时（已有唯一索引）和运行期 DDL 切换时都会输出 `Criticalf` |
| `filter.ddl_enable = true`（模板默认 false） | 运行期 DDL 不会下发到目标端，目标端不会真的建出唯一索引，因此数据仍然一致（目标端缺少该索引是既有行为）；启动期扫描不受影响。`applyOps` 内嵌的 DDL 不受该开关控制，见下一行 |
| 非 `applyOps` 内的 DDL | `applyOps` 内层 op 不走 barrier，无法中途切换；`auto` 模式下输出 `Criticalf` 提示该 namespace 可能已不一致 |

### 行为边界

| 场景 | 行为 |
|---|---|
| 单字段唯一索引 | 该集合整表有序回放，保证一致 |
| 复合唯一索引 | 同上（不区分索引列数，一律整表有序） |
| 同一集合多个唯一索引 | 同上（任一命中即整表有序） |
| 稀疏 / 部分（partial）唯一索引 | 同上。这类索引的"参与集合"是文档子集，整表有序必然覆盖 |
| 唯一索引首列是 `_id` | 不切换：`_id` 唯一性已由 `_id` 分发保证。例外：`collMod` 的 oplog 只有索引名、没有 keyPattern，无法识别，会保守地切换（只是多串行化一个集合） |
| 启动后才建唯一索引（集合已有数据） | `commitIndexBuild` 在 DDL barrier 处切换，保证一致 |
| 启动后在**空集合**上建唯一索引 | 平铺形态同样识别并切换，保证一致 |
| `collMod` 把已有索引转成唯一（7.0+） | DDL barrier 处切换，保证一致 |
| 唯一索引被删除 | 保持有序（只增不减），保证一致、牺牲并发 |
| `incr_sync.executor > 1` | **不保证**；启动时 `Criticalf` 告警 |
| `applyOps` 内嵌的唯一索引 DDL | **不保证**；`auto` 模式下 `Criticalf` 告警 |
| `shard_key = id` + 存在唯一索引 | **不保证**；启动时输出 `Criticalf` 列出全部受影响的 namespace |
| `shard_key = collection` | 不变 |

### 并发度评估

- 有唯一索引的集合：退化为单 worker（与改动前的 `collection` 模式相同）。这是唯一键跨文档约束的代价，无法用静态分区规避。
- 无唯一索引的集合：**保持 `_id` 级并发**。改动前只要库里有一个唯一索引，这些集合也会被一起退化成按 namespace 哈希，现在是本次改动的主要收益。
- 分片源：`incr_sync.worker` 被强制等于 shard 数，每个 syncer 只有 1 个 worker（`mod == 1`），本次改动对分片源无行为差异。
- `shard_key = id` 新增一次启动期元数据扫描（`listCollections` + `listIndexes`），用于输出告警。

## 验证

单元测试：

- `oplog/hasher_unique_test.go`：有序/并行 namespace 的路由、`mod == 1`、运行时切换、`createIndexes`（`indexes` 数组与空集合平铺两种形态）/ `startIndexBuild` / `commitIndexBuild` / `collMod index.unique` / `prepareUnique` / 复合索引 / 多索引用例 / 首列 `_id` / `renameCollection`。
- `collector/batcher_unique_ddl_test.go`：DDL barrier 触发切换、`filter.ddl_enable=false` 不切换、旧 hasher 类型下不 panic、`applyOps` 内层 DDL 不切换。
- `common/community_client_unique_index_test.go`：扫描结果只包含"unique 且首列非 `_id`"的集合；扫描期间被 drop 的集合（`NamespaceNotFound`）不报错。

端到端（`integration/unique_index_auto_test.go`，`go test -tags integration`）：

- `TestAutoUniqueIndexOrderedReplayAtStartup`：唯一索引在启动前存在（验证启动期扫描 + 有序回放本身；旧实现全局退化 `collection` 时同样通过，属回归保护而非红绿）。
- `TestAutoUniqueIndexCreatedAtRuntime`：启动时无唯一索引，运行期在**有数据**的集合上建索引（`commitIndexBuild` 形态），DDL 后同样的序列仍然一致。
- `TestAutoUniqueIndexOnEmptyCollection`：在**空集合**上建唯一索引（平铺形态），DDL 后仍然一致。
- `TestAutoUniqueIndexConvertedByCollMod`：`collMod` 把普通索引转成唯一（7.0+），DDL 后仍然一致。
- 红绿对照：把上面三个运行期用例放到 `b2eac1d`（漏掉平铺形态与 `collMod` 的版本）上跑，`TestAutoUniqueIndexOnEmptyCollection` 与 `TestAutoUniqueIndexConvertedByCollMod` 都在**第 0 轮**出现文档丢失（`document _id=100001 ... never reached the target`）。`MSHAKE_UNIQ_SHARD_KEY=id` 可把 `TestAutoUniqueIndexCreatedAtRuntime` 切到 `id` 模式做同样的对照。
- 三个**运行期**用例（`CreatedAtRuntime` / `OnEmptyCollection` / `ConvertedByCollMod`）的种子写入、建索引、建集合都发生在 `checkpoint.start_position` **固定之后**：读取端查询是 `ts > (startSec, 0)`，起点秒之前的写入不会被回放（起点秒内 `i > 0` 的写入仍会回放，所以关键是不要与起点跨秒竞争）。`OrderedReplayAtStartup` 的索引在启动前创建，不依赖回放。
