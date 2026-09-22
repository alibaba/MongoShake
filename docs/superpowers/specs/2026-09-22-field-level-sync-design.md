# MongoShake 集合字段级（顶层字段）过滤同步 —— 设计文档（SDD）

- 日期：2026-09-22
- 状态：已与需求方确认，待写实现计划（TDD）
- 关联设计输入：`~/Desktop/mongodb-partial-field-sync-design.md`（部分字段同步方案讨论）
- 适用版本：MongoShake v2（仓库 `develop` 分支）

## 1. 背景与目标

部分业务希望借助 MongoShake 做"集合字段级拆分"：源端一个宽集合，只把**选定的若干字段**同步到目标端。本特性在 MongoShake 中引入**字段白名单**，使全量与增量阶段都只同步白名单内的字段。

目标（v1）：

- 全量阶段：扫描源集合时只投影白名单字段写入目标。
- 增量阶段：change stream 事件只应用白名单字段的变更到目标。
- 索引：只同步"key 完全落在白名单内"的索引。
- 配置、校验、行为对用户清晰可控，错误配置在启动期（sanitize）即被拦截。

最终一致性验收口径（沿用输入设计文档）：源端停止变化、队列处理完成后，目标文档 == 源文档按白名单做 inclusion projection 的结果。

## 2. 范围

### 2.1 v1 纳入

- 顶层字段白名单（按字段名选择整字段）。
- 全量 / 增量两套**独立**配置参数。
- 增量**仅支持 change stream**（`incr_sync.mongo_fetch_method = change_stream`），不支持 oplog 模式。
- 索引过滤（all-or-nothing，见第 8 节）。
- 与 `filter.namespace.white` 的联动校验（见第 5.3 节）。

### 2.2 v1 不纳入（列为后续优化点，见第 10 节）

- 子字段挑选（如仅 `profile.city`）、数组内嵌文档字段（如 `items.sku`）。
- 字段名包含 `.`（字面点号 vs 嵌套路径歧义、`disambiguatedPaths`）。
- 复合索引"剔除非白名单分量后改写"。
- 目标端历史残留字段的主动清理。
- 白名单运行期热变更（需重新初始化）。
- oplog 模式下的字段过滤（含 applyOps 内层 op 投影）。
- 通配符 ns（`db.*`）。

## 3. 术语与字段语义（关键）

- **白名单字段**：顶层字段名，**禁止包含 `.`**。
- **整字段语义**：选中一个顶层字段，则同步它的**整个值**——即使该值是对象或数组，也整体保留（inclusion projection 天然返回整棵子树）。v1 不支持"只挑对象/数组里的子字段"。
- **路径首段匹配（firstComponent）**：对一个可能带点的路径 `k`，取第一个 `.` 之前的部分。
  - `firstComponent("a") = "a"`，`firstComponent("profile.city") = "profile"`。
  - 增量 update 的 `updatedFields`/`removedFields` 的键可能是子路径（如 `profile.city`）。**当且仅当 `firstComponent(key)` ∈ 白名单时保留该变更条目**。这样选中顶层对象字段 `profile` 时，其子路径更新 `profile.city` 也能正确同步，避免目标残留旧值。
- `_id` 始终保留，不需要也不能写进白名单。

> 语义对照：
> - 选中标量字段 `a`：源 `$set:{a:1}` → 同步；源 `$set:{b:1}` → 忽略。
> - 选中对象字段 `profile`：源 `$set:{profile:{...}}` 或 `$set:{"profile.city":x}` → 同步；源 `$set:{"other.x":1}` → 忽略。

## 4. 配置设计

### 4.1 新增配置项（`collector/configure/configure.go`）

两个独立参数（`[]string`，nimo 按分号 `;` 切分，与 `filter.namespace.white`、`incr_sync.executor.dup_key_skip_rules` 一致）：

```ini
# 全量阶段字段白名单；为空表示不过滤（同步全部字段）
full_sync.field.whitelist =
# 增量阶段字段白名单；为空表示不过滤
incr_sync.field.whitelist =
```

值语法（复用 `dup_key_skip_rules` 风格）：

```
db.collection:field1,field2;db2.collection2:fieldA
```

- 不同 ns 用 `;` 分隔；ns 与字段列表用 `:` 分隔；字段之间用 `,` 分隔。
- ns 必须是 `db.collection` 形态（含 `.`）。

对应结构体字段：

```go
FullSyncFieldWhitelist []string `config:"full_sync.field.whitelist"` // 全量段
IncrSyncFieldWhitelist []string `config:"incr_sync.field.whitelist"` // 增量段
```

### 4.2 生成态（解析后）字段

仿 `IncrSyncExecutorDupKeySkipRulesMap`，在 `Configuration` 的 generated 区新增：

```go
FullSyncFieldWhitelistMap map[string]map[string]struct{} // ns -> {field: {}}
IncrSyncFieldWhitelistMap map[string]map[string]struct{}
```

由 sanitize 解析填充；运行期 docsyncer / filter 直接读取，避免重复解析。

## 5. 校验规则（`cmd/collector/sanitize.go`，新增 `parseFieldWhitelists()`）

放在 `checkConflict()` 中、filter 相关校验之后（`filter.namespace.white/black` 已就绪）。任一不满足即返回 error，阻止启动。

### 5.1 格式与字段名

- 每条按 `:` 拆 ns 与字段串；字段串按 `,` 拆；逐项 `TrimSpace`，跳过空项。
- ns 不含 `.` → 报错：`field whitelist namespace should be db.collection; got [xxx]`。
- 字段名为空 → 报错。
- **字段名含 `.` → 报错**（v1 仅顶层）：`field whitelist does not support nested/dotted field [a.b] in v1`。
- 同一 ns 多次出现 → 合并字段集（去重）。

### 5.2 全量 ↔ 增量一致性（直接报错）

对同时出现在 `FullSyncFieldWhitelistMap` 与 `IncrSyncFieldWhitelistMap` 的 ns，两者字段集**必须完全相同**，否则报错：

```
field whitelist mismatch for ns[db.coll]: full_sync=[a b], incr_sync=[a c]
```

（允许某 ns 只在全量或只在增量出现；只要求"交集 ns 字段集相等"。）

### 5.3 与 `filter.namespace.white` 联动（超集校验）

当任一字段白名单非空时：

1. **必须**设置了 `filter.namespace.white`（`len(FilterNamespaceWhite)!=0`）。仅设 black 或未设 → 报错：
   `field whitelist requires filter.namespace.white to be set`。
   （white/black 互斥已由现有 `sanitize.go:520-522` 保证。）
2. **超集校验**：用 `filter.NewNamespaceFilter(FilterNamespaceWhite, nil)` 构造 ns 过滤器，对字段白名单里每个 ns（全量与增量都查）调用 `FilterNs(ns)`：
   - 返回 `false`（通过白名单）→ 覆盖，OK。
   - 返回 `true`（被过滤掉）→ 未覆盖，收集起来。
   存在未覆盖 ns → 报错并**明确列出**：
   `field whitelist namespace(s) [db.x, db.y] not covered by filter.namespace.white`。

   white 条目可为 db 级（`db1`）或集合级（`db1.coll1`），二者都能覆盖字段规则里的 `db1.coll1`（由现有 `convertToRule` 正则保证）。

### 5.4 增量强制 change stream（约束 #2）

`IncrSyncFieldWhitelist` 非空但 `IncrSyncMongoFetchMethod != change_stream` → 报错：

```
incr_sync.field.whitelist requires incr_sync.mongo_fetch_method = change_stream
```

### 5.5 解析产物

校验通过后写入 `conf.Options.FullSyncFieldWhitelistMap` / `IncrSyncFieldWhitelistMap`。

## 6. 全量阶段设计

### 6.1 数据投影（`collector/docsyncer/doc_reader.go`，`ensureNetwork()` ~line 410）

在构建 `findOptions`、调用 `Find` 之前插入：

```go
if fields := conf.Options.FullSyncFieldWhitelistMap[reader.ns.Str()]; len(fields) > 0 {
    findOptions.SetProjection(filter.BuildInclusionProjection(fields))
}
```

- `reader.ns` 是**源端** ns（`doc_syncer.go:443 collectionSync(id, ns源, toNS目标)`，reader 用源 ns）。
- `BuildInclusionProjection(fields)` 产出 `bson.D{{"_id",1},{f1,1},...}`，服务端 inclusion projection。
- 选中字段的整棵子树原样返回；下游 `splitSync → CollectionExecutor.Sync → DocExecutor.doSync` 全程不透明 `bson.Raw`，**无需改动写入侧**。
- split/hint 仍按 `_id`（默认）；range 查询是 filter，不依赖投影输出，故投影不影响并行分片扫描。

### 6.2 索引过滤（`collector/docsyncer/doc_syncer.go`，`StartIndexSync` 循环 ~line 270）

对每个 index spec（`bson.D`，含 `key`）：

```go
if fields := conf.Options.FullSyncFieldWhitelistMap[ns.Str()]; len(fields) > 0 {
    if !filter.IndexKeyCovered(indexKeyDoc, fields) {
        continue // 跳过未覆盖的索引
    }
}
```

- `_id` 索引由现有 `utils.HaveIdIndexKey(index)` 跳过逻辑处理（保持）。
- 覆盖判定见第 8 节。

## 7. 增量阶段设计（change stream）

### 7.1 注入点：新增 `OplogFilter`（`collector/filter/field_filter.go`）

在 `collector/syncer.go:152-164` 的过滤链构建处，当 `len(conf.Options.IncrSyncFieldWhitelistMap)!=0` 时 append `filter.NewFieldFilter(conf.Options.IncrSyncFieldWhitelistMap)`。

选择过滤链而非改 `ConvertEvent2Oplog`，理由：
- filter 包已 import conf/oplog，无环依赖，不改 `ConvertEvent2Oplog` 签名与其大量测试。
- 复用 `OpTypeFilter`/`NamespaceFilter` 已确立的"原地 mutate `log.Object` + 返回 true 丢弃"范式。
- sanitize 已强制 change_stream，故 flattened 形态假设成立（见下）。

### 7.2 `FieldFilter.Filter(log *oplog.PartialLog) bool`（true=丢弃）

```
fields, ok := whitelist[log.Namespace]   // 源 ns
if !ok or len(fields)==0: return false   // 该 ns 不过滤，原样保留

switch log.Operation:
  case "i":                 // insert: Object = fullDocument
      log.Object = ProjectDocument(log.Object, fields)   // 保留 _id + 键名精确∈白名单
      return false
  case "u":
      if log.ObjectHasPrefix("$"):        // 无后镜像 update: Object = {$set, $unset}
          newObj, empty := FilterModifiers(log.Object, fields)  // 按 firstComponent 过滤
          if empty: return true           // 全是非白名单字段变更 → 丢弃
          log.Object = newObj
          return false
      else:               // replace，或开了后镜像的 update: Object = fullDocument
          log.Object = ProjectDocument(log.Object, fields)
          return false
  case "c":                 // 命令：仅处理 createIndexes（见 7.3），其余原样
      return filterCreateIndexes(log, fields)
  default:                  // "d" delete(documentKey) / "n" noop / 其它：原样
      return false
```

要点：
- **insert/replace/后镜像 update** 走 `ProjectDocument`：保留 `_id` 与**键名精确等于**白名单字段的顶层键（fullDocument 的顶层键就是真实字段名，整值保留）。replace 投影后下游 `db_writer_bulk.go:421-437` 路由到 `ReplaceOne(upsert)`，目标文档=投影结果（假定目标集合由同步任务独占，见第 10 节）。
- **无后镜像 update**（direct tunnel 默认，见第 9 节）走 `FilterModifiers`：对 `$set`/`$unset` 的每个键按 `firstComponent ∈ 白名单` 取舍；重建 Object（空的 `$set`/`$unset` 元素省略）；若两者皆空 → 返回 true 丢弃。
- **delete** 原样（`documentKey` 只含 `_id`/分片键），目标按标识删除，正确。
- 不开 `fullDocument:updateLookup`（约束 #3），不碰 `sanitize.go:614-619` 的 direct-tunnel 强制关闭逻辑，无源端回查。

### 7.3 增量 createIndexes 过滤

change stream 经 `ConvertEvent2Oplog`（`oplog/change_stream_event.go:599-617`）只会产出 `{createIndexes: coll, indexes: [...]}` 数组形态（无 `commitIndexBuild`、无 legacy 单索引形态）。`filterCreateIndexes`：

- 取 `log.Object` 的 `indexes`（`bson.A`），逐个 index spec 用 `IndexKeyCovered(key, fields)` 过滤；
- 用 `oplog.SetFiled(log.Object, "indexes", remain)` 回写（复用 `NamespaceFilter` 改写 applyOps 的范式，`oplog_filter.go:378`）；
- `len(remain)==0` → 返回 true（丢弃整条 createIndexes）。
- 非 createIndexes 的 `op="c"`（drop/rename/DDL 等）原样不动。

> 注：DDL 是否真正下发还受 `filter.ddl_enable` 与 batcher 的 DDL 门控（`batcher.go:293,370-386`）影响；`FieldFilter` 在 `batcher.filter()` 链中先于 DDL 门控执行，因此无论 DDL 开关如何，索引覆盖过滤都生效且安全。

### 7.4 事务说明

change stream 下事务 DML 是**逐条事件**下发（各带自身 fullDocument/updateDescription），按普通 i/u/d 投影即可，无需处理 applyOps 内层。oplog 模式的 applyOps 内层投影不在 v1 范围（oplog 模式本就不支持字段过滤）。

## 8. 索引过滤规则（all-or-nothing）

`IndexKeyCovered(keyDoc bson.D, fields map[string]struct{}) bool`：

```
for each element e in keyDoc:
    if e.Key == "_id": continue
    if firstComponent(e.Key) not in fields: return false
return true
```

即：**一个索引只有当其 key 中所有非 `_id` 字段的首段都∈白名单时才同步，否则整个索引跳过**。复合索引"既有白名单字段又有非白名单字段" → **不保留**。

理由（关键：unique 复合索引的正确性地雷）：
- 复合索引 `{a:1,b:1}`，若 `b` 被过滤，目标端所有文档都缺 `b`，该分量恒为"缺失"。
- 若为 **unique**，`{a,b}` 唯一退化为"`a` + 恒定缺失"唯一 ⇒ 等价于对 `a` 唯一 ⇒ 源端 a 相同、b 不同的合法文档在目标端触发**假重复键冲突**，写入失败。属数据正确性问题。
- 保留它的唯一收益是服务前缀字段查询，收益边际且有条件；用户真需要可把另一字段加进白名单或显式建前缀索引。
- "改写复合索引剔除非白名单分量"会改变索引名/定义、对 unique 改变语义，v1 风险过大 → 后续。

边界：文本/地理等索引（key 为 `_fts`/`_ftsx`/`_2dsphere` 等特殊名）按 key 名覆盖判定，落在非白名单上的一律跳过（v1 已知边界）。

## 9. 关键约束依据（已核实的代码事实）

- `cmd/collector/sanitize.go:614-619`：`tunnel==direct` 时强制 `IncrSyncChangeStreamWatchFullDocument=false`。
- `cmd/collector/sanitize.go:570-573`：仅 `tunnel==direct` 支持全量。
- 故"全量+增量、direct tunnel"的字段拆分主场景下，后镜像本就不可用 → v1 走无后镜像差量过滤路线（约束 #3 的评估结论）。
- `common/change_stream.go:101`：change stream 以空 pipeline `Watch`；本特性不改 pipeline，投影在事件转换后的过滤链完成。
- `oplog.ParsedLog.ObjectHasPrefix("$")`（`oplog/lazy.go:217`）：可靠区分 modifier 形态与整文档形态（change-stream 转换出的 Object 为 `bson.D`，走 `FindFiledPrefix`）。

## 10. 已知限制与后续优化点

| 项 | v1 行为 | 后续 |
|---|---|---|
| 子字段挑选（`profile.city`） | 不支持（字段名含点直接报错） | 嵌套投影 + diff 应用 |
| 数组内嵌字段（`items.sku`） | 不支持 | 数组语义投影 |
| 字面点号字段名 | 不支持 | `disambiguatedPaths` 适配 |
| 复合索引部分覆盖 | 整条跳过 | 改写为白名单前缀（需处理 name/unique 语义） |
| 目标残留字段清理 | 不清理；假定目标集合由同步任务独占（配合 `full_sync.collection_exist_drop` 或全新目标） | 显式 `$unset` 已删字段 |
| 白名单热变更 | 需重启/重新初始化 | 重投影/补数据流程 |
| oplog 模式字段过滤 | 不支持（sanitize 拦截） | applyOps 内层投影 |
| 通配 ns | 不支持，仅精确 `db.coll` | `db.*` |

## 11. 数据流（简）

```
全量：源集合 --(Find + SetProjection{_id,白名单})--> bson.Raw --> 目标写入
      索引：fetchIndexes --> StartIndexSync(IndexKeyCovered 过滤) --> createIndexes

增量(change stream)：Watch(空pipeline) --> ConvertEvent2Oplog --> PartialLog
      --> batcher.filter 链[... , FieldFilter] --> worker --> executor --> 目标
            insert/replace/后镜像update : ProjectDocument(整文档)
            无后镜像 update            : FilterModifiers($set/$unset 按首段) ；空则丢弃
            delete                     : 原样(documentKey)
            createIndexes(op=c)        : IndexKeyCovered 过滤 indexes 数组；空则丢弃
```

## 12. 测试计划（TDD）

### 12.1 单元测试（无需 Mongo）

`cmd/collector/sanitize_test.go`（`parseFieldWhitelists`）：
- 合法解析：单 ns 多字段、多 ns、含空格 trim、字段去重。
- 非法：ns 不含点；字段名含点；字段为空。
- 一致性：同一 ns 全量/增量字段集不同 → 报错；相同 → 通过；只在一侧出现 → 通过。
- namespace 联动：字段白名单非空但未设 white → 报错；设了 black 未设 white → 报错；字段 ns 被 white 精确覆盖 → 通过；被 white db 级覆盖 → 通过；未被覆盖 → 报错且信息含具体 ns。
- change_stream 强制：增量白名单非空 + fetch_method=oplog → 报错；=change_stream → 通过。

`collector/filter/field_filter_test.go`（`FieldFilter`，构造 `PartialLog` fixture，仿 `oplog/change_stream_event_test.go`）：
- insert：fullDocument 投影后只留 `_id`+白名单字段；非白名单字段被剔除。
- replace（op=u，无 `$` 前缀，Object=整文档）：同 insert 投影。
- update 无后镜像（op=u，`$set/$unset`）：
  - `$set:{a:1,b:2}` 白名单{a} → 只留 `$set:{a:1}`。
  - `$set:{"profile.city":x}` 白名单{profile} → 保留（首段匹配）。
  - `$unset:["b"]` 白名单{a} → `$unset` 被清空。
  - 全是非白名单 → `Filter` 返回 true（丢弃）。
- delete（op=d，documentKey）：原样不动，返回 false。
- ns 不在白名单：原样不动。
- createIndexes（op=c）：indexes 数组按 `IndexKeyCovered` 过滤；全剔除 → 返回 true；非 createIndexes 的 op=c → 原样。
- helper：`ProjectDocument`、`FilterModifiers`、`IndexKeyCovered`、`BuildInclusionProjection`、`firstComponent` 各自的表驱动用例（含 `_id` 恒保留、unique 复合索引被跳过、文本索引被跳过）。

### 12.2 单元测试（需 live Mongo，沿用现有 `doc_syncer_test.go` 模式，可选）

- `StartIndexSync` 在白名单下只建覆盖索引（`TestStartIndexSync` 扩展）。

### 12.3 集成测试（`//go:build integration`，本地 docker 环境见 `AGENTS.md`）

新增 `integration/field_filter_test.go`：源副本集 `mshake-cs-src:27030` → 目标 `mshake-txn-tgt:27019`，配置 `incr_sync.mongo_fetch_method=change_stream` + `filter.namespace.white` + 全量/增量字段白名单。用例：
- insert 含额外字段 → 目标只含白名单顶层字段。
- update 白名单字段 → 目标更新；update 非白名单字段 → 目标不变（no-op）。
- update 选中对象字段的子路径（`profile.city`）→ 目标 `profile` 正确刷新。
- replace → 目标为投影后整文档。
- delete → 目标文档删除。
- createIndex（覆盖 / 不覆盖）→ 目标只出现覆盖索引。
- 复用 `MSHAKE_COLLECTOR_BIN` 做基线对照（基线无字段过滤，应失败/不一致）。

## 13. 改动文件清单

实现：
- `collector/configure/configure.go`：新增 2 个 `[]string` 配置 + 2 个生成态 map。
- `cmd/collector/sanitize.go`：新增 `parseFieldWhitelists()`，在 `checkConflict()` 调用。
- `collector/filter/field_filter.go`（新增）：`FieldFilter` + `ProjectDocument` + `FilterModifiers` + `IndexKeyCovered` + `BuildInclusionProjection` + `firstComponent`。
- `collector/docsyncer/doc_reader.go`：`ensureNetwork()` 加投影。
- `collector/docsyncer/doc_syncer.go`：`StartIndexSync` 加索引覆盖过滤。
- `collector/syncer.go`：过滤链 append `FieldFilter`。
- `conf/collector.conf`：新增两项配置的中英文说明。

测试：
- `cmd/collector/sanitize_test.go`、`collector/filter/field_filter_test.go`、`collector/docsyncer/doc_syncer_test.go`（索引）、`integration/field_filter_test.go`。

## 14. 验收标准

1. 配置非法（格式/含点字段/ns 未覆盖/一致性/非 change_stream）时，collector 启动期报错并给出明确信息。
2. 全量：目标集合文档仅含 `_id`+白名单顶层字段；索引仅含被覆盖者。
3. 增量：insert/replace/update/delete 行为符合第 7 节；非白名单字段更新为目标 no-op；选中对象字段的子路径更新正确刷新。
4. 源端停止变化、队列处理完成后，目标数据 == 源端按相同白名单 inclusion projection 的结果。
5. 全部新增单元测试通过；集成测试在本地 docker 环境通过；未配置字段白名单时，现有全量/增量行为零回归（既有测试全绿）。
