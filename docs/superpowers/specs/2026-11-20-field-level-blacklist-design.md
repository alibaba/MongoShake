# MongoShake 字段级过滤 —— 黑名单（排除大字段）补充设计（SDD）

- 日期：2026-11-20
- 状态：待需求方确认 → TDD 实现
- 关联设计：`docs/superpowers/specs/2026-09-22-field-level-sync-design.md`（字段白名单 v1，已随 PR #1007 合入 develop）
- 适用版本：MongoShake v2（`develop` 分支）

## 1. 背景与目标

白名单 v1 要求用户**枚举要同步的小字段**。有客户场景相反：宽集合里的大字段已知、要排除，但小字段太多无法穷举。客户原声：

> "这个参数是否可以加一个黑名单的……我把其它大字段拆出去了，但是原实例要做数据清理，我也是准备通过 mongoshake 同步数据操作方案，但是开发看起来提供不了完整小字段信息给我，所以我就想着能否改为黑名单，就是把已经知道大字段排除在外不同步，其它的都小字段全同步过去。"

目标：引入**字段黑名单**——排除列出的顶层字段，其余字段（含源端后续新增字段）全部同步。与白名单互补，覆盖"已知要排除什么、但无法枚举要保留什么"的场景。

## 2. 范围

### 2.1 纳入

- 顶层字段黑名单，全量 / 增量两套独立配置：`full_sync.field.blacklist` / `incr_sync.field.blacklist`。
- 增量仅 change stream（沿用白名单约束）。
- 索引过滤：key 含任一黑名单字段的索引整体跳过（白名单 all-or-nothing 的对偶）。
- 白名单 / 黑名单**按 ns 互斥**校验，full/incr 模式与字段集一致性校验。
- 与 `filter.namespace.white` 联动校验（沿用）。

### 2.2 不纳入（沿用白名单 v1 边界）

- 子字段 / 数组内字段挑选；字段名含 `.`；复合索引剔除分量改写；目标残留字段清理；运行期热变更；oplog 模式字段过滤；通配符 ns。

## 3. 语义（关键）

- **黑名单字段**：顶层字段名，禁止含 `.`；**禁止写 `_id`**（_id 永远保留，sanitize 拒绝黑名单含 _id）。
- **整字段排除**：排除一个顶层字段即排除其整棵子树（exclusion projection 天然如此）。
- **开放世界**：黑名单之外的一切字段都同步，包括源端运行期新增的字段。这是与白名单（封闭世界）的本质差异，也是客户诉求。
- **路径首段匹配（firstComponent）**：增量 update 的 `updatedFields`/`removedFields` 键可能是子路径。当且仅当 `firstComponent(key)` **∉** 黑名单时保留该变更条目（白名单的对偶）。排除顶层对象字段 `profile` 时，`profile.city` 更新也被排除。
- **update 丢弃**：若一条 update 的所有 `$set`/`$unset`/`$push` 条目首段都在黑名单内，重建后为空 → 丢弃该事件（目标 no-op），与白名单"空则丢弃"一致。
- **`$push`（数组截断）**：change stream 的 `truncatedArrays`（#986）被转成 `$push {$each:[],$slice:N}`。黑名单必须按 firstComponent 过滤 `$push`：非黑名单数组的纯截断事件要保留（否则目标数组残留多余元素），黑名单数组的截断要排除（否则 `$push` 会在目标凭空创建该字段）。白名单 `FilterModifiers` 同此处理（对偶）。"空"按重建后修饰符对象是否为空判定，而非仅看 `$set`/`$unset`。

> 语义对照（黑名单 `{big}`）：
> - 源 `$set:{small:1}` → 同步；源 `$set:{big:1}` → 忽略。
> - 源 `$set:{"big.sub":x}` → 忽略（firstComponent=big ∈ 黑名单）；源 `$set:{"small.sub":x}` → 同步。
> - insert/replace 全文档 → 投影掉 big，保留 _id 与其余字段。

## 4. 配置设计

### 4.1 新增配置项（`collector/configure/configure.go`）

```ini
full_sync.field.blacklist = db.coll:big1,big2;db2.coll2:blob
incr_sync.field.blacklist = db.coll:big1,big2;db2.coll2:blob
```

`[]string`，nimo 按 `;` 切分，规则格式与白名单完全一致（`db.coll:f1,f2`）。新增生成映射：

```go
FullSyncFieldBlacklist []string `config:"full_sync.field.blacklist"`
IncrSyncFieldBlacklist []string `config:"incr_sync.field.blacklist"`
FullSyncFieldBlacklistMap map[string]map[string]struct{}
IncrSyncFieldBlacklistMap map[string]map[string]struct{}
```

### 4.2 模式判定（per-ns）

每个 ns 至多一种模式：白名单 / 黑名单 / 无过滤。不同 ns 可分别采用不同模式（如 `db1.c1` 白名单、`db2.c2` 黑名单）。FieldFilter 与全量投影/索引过滤按 ns 查各自映射决定行为。

## 5. 校验（`cmd/collector/sanitize.go`）

复用白名单解析器（规则格式相同），新增/扩展：

1. **互斥（per-ns）**：同一 ns 不得同时出现在白名单与黑名单——含 `full_sync.field.whitelist` vs `full_sync.field.blacklist`、`incr_sync.field.whitelist` vs `incr_sync.field.blacklist`，以及跨阶段（full 白 + incr 黑、full 黑 + incr 白）的模式冲突。命中即报错，指明 ns 与冲突参数。
2. **_id 禁入黑名单**：黑名单字段含 `_id` → 报错。
3. **ns 覆盖**：黑名单 ns 必须被 `filter.namespace.white` 覆盖（沿用白名单逻辑）。
4. **full/incr 一致**：同一 ns 若 full 与 incr 都用黑名单，字段集必须相等（沿用 `fieldSetEqual`）。
5. **change_stream**：`incr_sync.field.blacklist` 非空要求 `incr_sync.mongo_fetch_method = change_stream`。
6.  dotted 字段、无点 ns、空字段等格式校验沿用白名单解析器。

## 6. 实现触点（4 处，与白名单对偶）

| 触点 | 文件 | 白名单 | 黑名单 |
|---|---|---|---|
| 全量数据投影 | `collector/docsyncer/doc_reader.go` | `BuildInclusionProjection` `{_id:1,f:1}` | `BuildExclusionProjection` `{big:0}`（_id 默认保留，不含 _id 项；排除投影不能与包含混用） |
| 全量索引过滤 | `collector/docsyncer/doc_syncer.go` | `IndexSpecCovered`（所有非 _id 键 ∈ 白名单） | `IndexSpecAllowed`（任一非 _id 键 ∈ 黑名单 → 跳过） |
| 增量文档/修饰符 | `collector/filter/field_filter.go` | `ProjectDocument` / `FilterModifiers`（保留 ∈ 白名单） | `ExcludeDocument` / `FilterModifiersExclude`（保留 ∉ 黑名单） |
| 增量 createIndexes | `collector/filter/field_filter.go` | `IndexSpecCovered` | `IndexSpecAllowed` |

FieldFilter 持有白名单与黑名单两个映射；`Filter`/`filterCommand` 先按 ns 判定模式再走对应分支。syncer 在任一映射非空时挂载 FieldFilter。

> 注意："把投影 1 变 0" 仅对全量数据投影成立；索引过滤与增量修饰符过滤需要真正的补集逻辑，不是简单取反投影。

## 7. 索引过滤（对偶）

- 白名单：索引保留 ⟺ 每个非 _id 键字段 ∈ 白名单。
- 黑名单：索引保留 ⟺ 没有非 _id 键字段 ∈ 黑名单（即任一黑名单字段出现在 key 中则整体跳过）。
- _id 索引始终跳过（沿用 `HaveIdIndexKey`）。
- 理由同白名单：同步"部分分量被排除"的复合索引会在目标造成假唯一/缺键隐患，故 all-or-nothing。

## 8. 验收（TDD）

- 单元（`collector/filter`）：`BuildExclusionProjection`、`ExcludeDocument`、`FilterModifiersExclude`、`IndexSpecAllowed` 的正/反例；FieldFilter 黑名单模式 insert/replace/update（含子路径、空则丢弃）/createIndexes。
- 单元（`cmd/collector`）：互斥（同 ns 白+黑、跨阶段模式冲突）、_id 禁入、ns 覆盖、full/incr 字段集相等、change_stream 约束。
- live-mongo（`collector/docsyncer`）：全量排除投影只去掉黑名单字段、保留其余；索引按黑名单跳过。
- integration（`integration/field_blacklist_test.go`，build tag `integration`）：端到端 insert 投影 / 黑名单 update no-op / 非黑名单 update 传播 / 子路径 / delete；断言用数值归一化（沿用 ffNum 教训，避免 int32-vs-int 恒 false）。
- 最终一致性口径：源端停止变化、队列处理完成后，目标文档 == 源文档按黑名单做 exclusion projection 的结果。

## 9. 兼容性与风险

- 纯新增配置，默认空 → 行为与现状完全一致，无回归面。
- 黑名单开放世界：源端新增字段会自动同步到目标，目标 schema 可能增长（客户预期行为，文档明示）。
- 白/黑互斥在 sanitize 启动期拦截，避免运行期歧义。