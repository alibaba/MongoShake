# 修复：OrphanFilter int64 精度丢失 + ComputeHash 类型覆盖不全

## 关联

- 接续 [`fix-orphan-document-mongos-warn.md`](./fix-orphan-document-mongos-warn.md) 中遗留的两个 latent bug
- 与 [Issue #978](https://github.com/alibaba/MongoShake/issues/978) 间接相关：用户报告的是 range 联合 key 场景，但同函数下 hashed 分片路径有这两处问题，本次一并清掉

## Bug 1：chunkLt / chunkGt 对 int64 chunk bound 丢精度

### 现象

`collector/filter/orphan_filter.go:229` `getBsonType()` 把 int64 一律 cast 成 float64，下游 `chunkLt / chunkGt / chunkEqual` 用 float64 比较。但 float64 只能精确表示 `[-2^53, 2^53]` 内的整数，hashed 分片的 chunk min/max 是**全范围 int64 hash 值**，超出 2^53 后，相邻 int64（甚至差几十）会折叠到同一个 float64：

```go
// 修复前可重现：
var v int64 = 1<<62 + 1
float64(v) == float64(v-1)     // true (false positive)
```

后果：hashed 分片下文档的 hashed key 与 chunk min/max 比较时，**严格小于/大于会被误判为相等**，造成边界判定错误，孤儿可能被放过，正常文档可能被当孤儿过滤掉。

### 修复

引入 `BsonTypeInt64`，让 int64 走 int64 精确比较；int64+float64 混合时再 promote 到 float64（混合 case 在 BSON shard key 实际配置中几乎没有）：

```go
case int64:
    return BsonTypeInt64, rx     // keep raw int64

func chunkLt(x, y interface{}) bool {
    xType, rx := getBsonType(x)
    yType, ry := getBsonType(y)
    if numericType(xType) && numericType(yType) {
        return numericCmp(xType, rx, yType, ry) < 0
    }
    ...
}

func numericCmp(...) int {
    if xType == BsonTypeInt64 && yType == BsonTypeInt64 {
        // exact int64 comparison
    }
    // else promote to float64 (loses precision; acceptable fallback)
}
```

### 测试

- 解除上次 commit 里 `TestOrphanFilter_HashedPrecisionBug` 的 `t.Skip`，改写成 `TestOrphanFilter_HashedTightChunk`（hashed key 在 ±1 chunk 内 / 不在 +100~+200 chunk 外）+ `TestOrphanFilter_HashedInt64Precision`（直接验证 `chunkLt/chunkGt/chunkEqual` 在 `1<<62 + 1` 附近的严格语义）

## Bug 2：ComputeHash 类型覆盖不全

### 现象

`ComputeHash` 之前仅支持 `string / int / int32 / int64 / float64 / primitive.ObjectID`。其它合法 hashed shard key 类型（`bool`、`Date`、`Timestamp`、`null`、`NumberDecimal`、`BinData`、`Symbol`）走 default 直接 `Panicf`。生产里 `bool` / `Date` / `Timestamp` 都是合法但偶尔有人用的 shard key。

### 修复

对照 MongoDB 内核 `hasher.cpp`（[已确认 4.0 与 8.0 算法完全一致](./fix-orphan-document-mongos-warn.md#latent-bug-2)）补 4 种最常用类型：

| 类型 | 内核 raw value 编码 | MongoShake 实现 |
|---|---|---|
| `bool` | 1 字节 (0x00/0x01) | ✅ 新增 |
| `primitive.DateTime` | int64 ms LE 8 字节 | ✅ 新增 |
| `primitive.Timestamp` | uint32(I) LE + uint32(T) LE = 8 字节 | ✅ 新增 |
| `nil` (BSON null) | 0 字节（只有 canonical type） | ✅ 新增 |
| `primitive.Decimal128` | 复杂：走 `safeNumberLongForHash` 归一化到 int64，但要正确处理 NaN/Inf/超范围 | ❌ 仍 panic，附带说明性 message |
| `primitive.Binary` | int32(len) + byte(subtype) + bytes | ❌ 仍 panic，hashed key 用 binary 极罕见 |
| `Symbol` | 同 String；已 deprecated | ❌ 仍 panic |

Decimal128 实现复杂且与内核 `safeNumberLongForHash` 的边界行为需要精确复刻才能在 NaN/Inf/超范围时不与 server 端 hash 出现微妙偏差，**收益远低于风险**，保留 panic 但 message 改成 `(Decimal128 / BinData / Symbol are intentionally not implemented; open an issue if you actually hit one)`，便于后续遇到时定位。

顺便把数字 case 拆开重写：之前 `int/int64/float64` 共用一段 type-assertion 链 + `uint64(float64)` 直接 cast。新写法每个类型一段，`int64(float64)` 保持与内核 `safeNumberLongForHash` 对 finite in-range 值的语义一致；NaN/Inf 与超 int64 范围的 float64 输入会与 server 不一致，但 float64 hashed shard key 极罕见，文档里记一笔不阻塞修复。

### 测试

- `TestOrphanFilter_HashedAllTypes` 从 3 类扩到 11 类：ObjectID/string/int64/int/int32/float64/bool×2/DateTime/Timestamp
- 新增 `TestComputeHash_NullKey` 单独验证 nil case（因为 `OrphanFilter.Filter` 上游 `oplog.GetKey` 把"null 值"和"字段缺失"等同，没法在 Filter 层测）
- `TestOrphanFilter_HashedUnsupportedTypePanics` 改用 `primitive.Decimal128{}` 验证 panic 仍触发

## 未在本次范围内的事

- **OrphanFilter.Filter 把 `nil` 与"字段缺失"等同**（`oplog.GetKey` 返回 nil → Filter 直接 Panicf）：是 Filter 自己的语义问题，与 ComputeHash 解耦。修起来要改 `oplog.GetKey` / `GetKeyWithIndex` 的返回签名，影响范围广，未做。
- **Decimal128 hashed shard key 支持**：见上文权衡，留作 follow-up。
- **mongos 模式 OrphanFilter 架构性不可用**：不是 latent bug，是已知架构限制。上一次 commit (`6ea9a2e`) 已经通过 WARN 暴露，无新动作。

## 改动文件清单

| 文件 | 类型 | 改动 |
|---|---|---|
| `collector/filter/orphan_filter.go` | 修改 | 引入 BsonTypeInt64/Bool/Date/Tstamp/Null 常量；getBsonType 保留 int64 原值；chunkLt/Gt/Equal 走 numericCmp 精确比较；ComputeHash 补 bool/Date/Timestamp/nil 四种类型 |
| `collector/filter/orphan_filter_test.go` | 修改 | 解除 PrecisionBug skip 改成 HashedInt64Precision / HashedTightChunk 真测；HashedAllTypes 扩到 11 case；新增 TestComputeHash_NullKey；unsupported-type 测试用 Decimal128 |
| `docs/fix-orphan-filter-latent-bugs.md` | 新增 | 本 spec |
