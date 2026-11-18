# 修复：ParsedLog.Hash 缺少 omitempty 导致 MongoDB 7.0+ applyOps 报错

## 需求描述

**现象：** 在 MongoDB 7.0+ 上使用 applyOps 回放 oplog 时，报错：

```
(IDLUnknownField) BSON field 'OplogEntryBase.h' is an unknown field.
```

**预期行为：** 当源端 oplog 不含 `h` 字段时，序列化后的 BSON 也应省略该字段，不传 `"h": null`。

## 根因分析

`oplog/oplog.go` 中 `ParsedLog.Hash` 字段声明为 `*int64`，bson tag 为 `bson:"h"`（无 omitempty）。

- 当 Hash 为 nil 时，Go BSON 序列化会输出 `"h": null`
- MongoDB 7.0 (SERVER-69062) 从 `OplogEntryBase` IDL 中完全移除了 `h` 字段
- applyOps 对输入做严格 IDL 校验，遇到未知字段直接报错
- MongoDB 9.0 (SERVER-128506) 将 `h` 重新定义为 `docHash`，语义与旧版完全不同

因此该字段必须在 BSON 中完全省略（absent），而不是序列化为 null。

## 修复方案

为 Hash 字段添加 `omitempty` tag：

```go
// Before
Hash *int64 `bson:"h" json:"h"`

// After
Hash *int64 `bson:"h,omitempty" json:"h,omitempty"`
```

nil 时字段不出现在 BSON/JSON 输出中，非 nil 时正常序列化。

## 影响范围

- **文件：** `oplog/oplog.go`
- **场景：** MongoDB 7.0+ 目标端的 applyOps 回放（time-series bucket 更新、system.views insert 等）
- **风险：** 低。omitempty 不影响非 nil Hash 的序列化行为
