# 字段级（顶层字段）过滤同步 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 MongoShake 增加集合"顶层字段白名单"过滤，全量用服务端 projection、增量（仅 change stream）用事件投影/modifier 过滤，并按白名单过滤索引。

**Architecture:** 新增两个独立配置 `full_sync.field.whitelist` / `incr_sync.field.whitelist`（`ns:f1,f2;...`），sanitize 解析+校验后写入 `conf.Options.*FieldWhitelistMap`。全量在 `DocumentReader.ensureNetwork()` 加 `SetProjection`、在 `StartIndexSync` 跳过未覆盖索引；增量在 batcher 过滤链挂一个 `FieldFilter`（复用 `OpTypeFilter` 的"原地 mutate + 可丢弃"范式）投影 insert/replace/update、过滤 createIndexes。

**Tech Stack:** Go；mongo-driver `bson`/`options`；testify；现有 `collector/filter`、`oplog`、`conf` 包；集成测试用本地 docker（见 `AGENTS.md`）。

**设计依据：** `docs/superpowers/specs/2026-09-22-field-level-sync-design.md`（SDD）。

## Global Constraints

- 字段白名单语义：**仅顶层字段名**；字段名**禁止含 `.`**；选中字段的整值同步（含对象/数组）；增量 update 按"路径首段 ∈ 白名单"保留（`firstComponent`）。
- 增量字段过滤**仅支持** `incr_sync.mongo_fetch_method = change_stream`；oplog 模式配置字段白名单 → sanitize 报错。
- 字段白名单非空时**必须**设置 `filter.namespace.white`，且每个字段 ns 必须被 white 覆盖（超集），否则报错。
- 同一 ns 同时出现在全量与增量白名单时，字段集**必须完全相同**，否则报错。
- `_id` 恒保留，不写进白名单。
- 索引：all-or-nothing —— key 中所有非 `_id` 字段首段都 ∈ 白名单才同步，否则整条跳过。
- 不开 `fullDocument:updateLookup`；不改 `common/change_stream.go` 的 watch pipeline；不改 `sanitize.go:614-619` 的 direct-tunnel 逻辑。
- 未配置任何字段白名单时，全量/增量/索引行为**零变化**（所有新增逻辑用 `len(fields)>0` 守卫）。
- 每个 commit 遵循仓库 AI 追踪约定（先写 `.git/.cr-ai-session` marker，commit message 末尾加 `Co-Authored-By` / `AI-Model` / `AI-Contributed/Feature` / `AI-Contributed/UT`，UT 行最后）。下面各步只展示 message 正文。
- 验证命令统一在仓库根 `/Users/zhongli/GolandProjects/MongoShake` 执行。

---

### Task 1: 配置字段 + sanitize 解析/校验

**Files:**
- Modify: `collector/configure/configure.go`（全量段、增量段、generated 段）
- Modify: `cmd/collector/sanitize.go`（新增 `parseFieldWhitelists` 等；在 `checkConflict` 调用；加 `sort` import）
- Test: `cmd/collector/sanitize_test.go`

**Interfaces:**
- Produces:
  - `conf.Options.FullSyncFieldWhitelist []string`、`conf.Options.IncrSyncFieldWhitelist []string`
  - `conf.Options.FullSyncFieldWhitelistMap map[string]map[string]struct{}`、`conf.Options.IncrSyncFieldWhitelistMap map[string]map[string]struct{}`（ns -> 字段集）
  - `parseFieldWhitelists() error`（package main）

- [ ] **Step 1: 写失败测试**（追加到 `cmd/collector/sanitize_test.go`）

```go
func TestParseFieldWhitelists(t *testing.T) {
	type in struct {
		full   []string
		incr   []string
		white  []string
		fetch  string
	}
	cases := []struct {
		name    string
		in      in
		wantErr string // "" 表示无错误
		wantFull map[string][]string
	}{
		{
			name:     "empty is no-op",
			in:       in{},
			wantErr:  "",
			wantFull: map[string][]string{},
		},
		{
			name:     "full only, covered by exact white",
			in:       in{full: []string{"db1.c1:a, b", "db1.c1:c"}, white: []string{"db1.c1"}},
			wantErr:  "",
			wantFull: map[string][]string{"db1.c1": {"a", "b", "c"}},
		},
		{
			name:    "covered by db-level white",
			in:      in{full: []string{"db1.c1:a"}, white: []string{"db1"}},
			wantErr: "",
			wantFull: map[string][]string{"db1.c1": {"a"}},
		},
		{
			name:    "missing namespace.white",
			in:      in{full: []string{"db1.c1:a"}},
			wantErr: "field whitelist requires filter.namespace.white to be set",
		},
		{
			name:    "ns not covered by white",
			in:      in{full: []string{"db1.c1:a"}, white: []string{"db2.c2"}},
			wantErr: "field whitelist namespace(s) [db1.c1] not covered by filter.namespace.white",
		},
		{
			name:    "dotted field rejected",
			in:      in{full: []string{"db1.c1:a.b"}, white: []string{"db1.c1"}},
			wantErr: "full_sync.field.whitelist does not support nested/dotted field [a.b] in v1",
		},
		{
			name:    "ns without dot rejected",
			in:      in{full: []string{"db1:a"}, white: []string{"db1"}},
			wantErr: "full_sync.field.whitelist namespace should be db.collection; got [db1:a]",
		},
		{
			name:    "incr without change_stream rejected",
			in:      in{incr: []string{"db1.c1:a"}, white: []string{"db1.c1"}, fetch: "oplog"},
			wantErr: "incr_sync.field.whitelist requires incr_sync.mongo_fetch_method = change_stream",
		},
		{
			name:    "full/incr mismatch rejected",
			in:      in{full: []string{"db1.c1:a,b"}, incr: []string{"db1.c1:a"}, white: []string{"db1.c1"}, fetch: "change_stream"},
			wantErr: "field whitelist mismatch for ns[db1.c1]: full_sync=[a b], incr_sync=[a]",
		},
		{
			name:     "full/incr match ok",
			in:       in{full: []string{"db1.c1:a,b"}, incr: []string{"db1.c1:b,a"}, white: []string{"db1.c1"}, fetch: "change_stream"},
			wantErr:  "",
			wantFull: map[string][]string{"db1.c1": {"a", "b"}},
		},
	}

	origin := conf.Options
	defer func() { conf.Options = origin }()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf.Options = conf.Configuration{
				FullSyncFieldWhitelist: c.in.full,
				IncrSyncFieldWhitelist: c.in.incr,
				FilterNamespaceWhite:   c.in.white,
			}
			if c.in.fetch != "" {
				conf.Options.IncrSyncMongoFetchMethod = c.in.fetch
			} else {
				conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
			}

			err := parseFieldWhitelists()
			if c.wantErr == "" {
				assert.NoError(t, err)
				for ns, fields := range c.wantFull {
					got := make([]string, 0, len(conf.Options.FullSyncFieldWhitelistMap[ns]))
					for f := range conf.Options.FullSyncFieldWhitelistMap[ns] {
						got = append(got, f)
					}
					sort.Strings(got)
					assert.Equal(t, fields, got)
				}
			} else {
				assert.EqualError(t, err, c.wantErr)
			}
		})
	}
}
```

> 注：测试文件已 import `sort`？若无则在该测试文件 import 块加入 `"sort"`（`assert`/`conf`/`utils` 已存在）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./cmd/collector -run TestParseFieldWhitelists -v`
Expected: 编译失败 `undefined: parseFieldWhitelists` 及 `conf.Options.FullSyncFieldWhitelist undefined`。

- [ ] **Step 3: 加配置字段**（`collector/configure/configure.go`）

全量段，在 `FullSyncExecutorImmutableShardKeyFallback`（约 line 77）之后加：

```go
	FullSyncFieldWhitelist                  []string `config:"full_sync.field.whitelist"` // add field-level sync
```

增量段，在 `IncrSyncBypassDocumentValidation`（约 line 101）之后加：

```go
	IncrSyncFieldWhitelist                  []string `config:"incr_sync.field.whitelist"` // add field-level sync
```

generated 段，在 `IncrSyncExecutorDupKeySkipRulesMap`（约 line 122）之后加：

```go
	FullSyncFieldWhitelistMap                map[string]map[string]struct{}
	IncrSyncFieldWhitelistMap                map[string]map[string]struct{}
```

- [ ] **Step 4: 加 sanitize 解析/校验**（`cmd/collector/sanitize.go`）

import 块加入 `"sort"`。在文件中新增：

```go
// parseFieldWhitelists parses full_sync.field.whitelist / incr_sync.field.whitelist
// (format: db.coll:f1,f2;db2.coll2:g1), validates them against filter.namespace.white
// and each other, then populates the generated maps on conf.Options.
func parseFieldWhitelists() error {
	fullMap, err := parseFieldWhitelistRules(conf.Options.FullSyncFieldWhitelist, "full_sync.field.whitelist")
	if err != nil {
		return err
	}
	incrMap, err := parseFieldWhitelistRules(conf.Options.IncrSyncFieldWhitelist, "incr_sync.field.whitelist")
	if err != nil {
		return err
	}

	if len(fullMap) == 0 && len(incrMap) == 0 {
		conf.Options.FullSyncFieldWhitelistMap = fullMap
		conf.Options.IncrSyncFieldWhitelistMap = incrMap
		return nil
	}

	if len(incrMap) != 0 &&
		conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodChangeStream {
		return fmt.Errorf("incr_sync.field.whitelist requires incr_sync.mongo_fetch_method = %s",
			utils.VarIncrSyncMongoFetchMethodChangeStream)
	}

	if len(conf.Options.FilterNamespaceWhite) == 0 {
		return fmt.Errorf("field whitelist requires filter.namespace.white to be set")
	}
	nsFilter := filter.NewNamespaceFilter(conf.Options.FilterNamespaceWhite, nil)
	notCovered := make([]string, 0)
	for ns := range fullMap {
		if nsFilter.FilterNs(ns) {
			notCovered = append(notCovered, ns)
		}
	}
	for ns := range incrMap {
		if nsFilter.FilterNs(ns) {
			notCovered = append(notCovered, ns)
		}
	}
	if len(notCovered) != 0 {
		sort.Strings(notCovered)
		return fmt.Errorf("field whitelist namespace(s) %v not covered by filter.namespace.white", notCovered)
	}

	for ns, fullFields := range fullMap {
		incrFields, ok := incrMap[ns]
		if !ok {
			continue
		}
		if !fieldSetEqual(fullFields, incrFields) {
			return fmt.Errorf("field whitelist mismatch for ns[%s]: full_sync=%v, incr_sync=%v",
				ns, sortedFieldSet(fullFields), sortedFieldSet(incrFields))
		}
	}

	conf.Options.FullSyncFieldWhitelistMap = fullMap
	conf.Options.IncrSyncFieldWhitelistMap = incrMap
	return nil
}

func parseFieldWhitelistRules(rules []string, confName string) (map[string]map[string]struct{}, error) {
	out := make(map[string]map[string]struct{})
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		parts := strings.SplitN(rule, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s should be db.collection:field1,field2; got [%s]", confName, rule)
		}
		ns := strings.TrimSpace(parts[0])
		if ns == "" || !strings.Contains(ns, ".") {
			return nil, fmt.Errorf("%s namespace should be db.collection; got [%s]", confName, rule)
		}
		if _, ok := out[ns]; !ok {
			out[ns] = make(map[string]struct{})
		}
		for _, field := range strings.Split(parts[1], ",") {
			field = strings.TrimSpace(field)
			if field == "" {
				return nil, fmt.Errorf("%s contains empty field for [%s]", confName, rule)
			}
			if strings.Contains(field, ".") {
				return nil, fmt.Errorf("%s does not support nested/dotted field [%s] in v1", confName, field)
			}
			out[ns][field] = struct{}{}
		}
	}
	return out, nil
}

func fieldSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func sortedFieldSet(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

在 `checkConflict()` 内、`filter.pass.special.db` 处理块（`filter.InitNs(...)` 那个 if，约 line 524-527）之后插入：

```go
	// field-level whitelist (parse + validate against filter.namespace.white)
	if err := parseFieldWhitelists(); err != nil {
		return err
	}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./cmd/collector -run TestParseFieldWhitelists -v`
Expected: PASS（全部子用例）。

Run: `go build ./... && go vet ./cmd/collector ./collector/configure`
Expected: 无输出（成功）。

- [ ] **Step 6: Commit**

```bash
git add collector/configure/configure.go cmd/collector/sanitize.go cmd/collector/sanitize_test.go
git commit -m "feat: add field whitelist config + sanitize validation"
```

---

### Task 2: filter 包的字段投影 helper（纯函数）

**Files:**
- Create: `collector/filter/field_filter.go`
- Test: `collector/filter/field_filter_test.go`

**Interfaces:**
- Produces（package filter，供 Task 3/4/5 使用）:
  - `func firstComponent(path string) string`
  - `func BuildInclusionProjection(fields map[string]struct{}) bson.D`
  - `func ProjectDocument(doc bson.D, fields map[string]struct{}) bson.D`
  - `func FilterModifiers(obj bson.D, fields map[string]struct{}) (bson.D, bool)`
  - `func IndexSpecCovered(indexSpec bson.D, fields map[string]struct{}) bool`

- [ ] **Step 1: 写失败测试**（创建 `collector/filter/field_filter_test.go`）

```go
package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func fieldSet(names ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}

func TestFirstComponent(t *testing.T) {
	assert.Equal(t, "a", firstComponent("a"))
	assert.Equal(t, "profile", firstComponent("profile.city"))
	assert.Equal(t, "items", firstComponent("items.0.sku"))
}

func TestBuildInclusionProjection(t *testing.T) {
	proj := BuildInclusionProjection(fieldSet("b", "a"))
	assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "b", Value: 1}}, proj)
}

func TestProjectDocument(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: 10},
		{Key: "secret", Value: "x"},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "hz"}, {Key: "phone", Value: "1"}}},
	}
	got := ProjectDocument(doc, fieldSet("a", "profile"))
	assert.Equal(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: 10},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "hz"}, {Key: "phone", Value: "1"}}},
	}, got)
}

func TestFilterModifiers(t *testing.T) {
	// keep $set entries whose first component is whitelisted; drop the rest
	obj := bson.D{
		{Key: "$set", Value: bson.M{"a": 1, "secret": 2, "profile.city": "hz"}},
		{Key: "$unset", Value: bson.M{"b": 1, "secret2": 1}},
	}
	got, empty := FilterModifiers(obj, fieldSet("a", "profile"))
	assert.False(t, empty)
	assert.Equal(t, bson.D{
		{Key: "$set", Value: bson.M{"a": 1, "profile.city": "hz"}},
	}, got)

	// nothing whitelisted -> empty, drop
	_, empty2 := FilterModifiers(obj, fieldSet("zzz"))
	assert.True(t, empty2)
}

func TestIndexSpecCovered(t *testing.T) {
	fields := fieldSet("a", "profile")
	assert.True(t, IndexSpecCovered(bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
		{Key: "name", Value: "a_1"},
	}, fields))
	assert.True(t, IndexSpecCovered(bson.D{
		{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
	}, fields))
	assert.True(t, IndexSpecCovered(bson.D{
		{Key: "key", Value: bson.D{{Key: "profile.city", Value: 1}}},
	}, fields))
	// compound with a non-whitelisted field -> not covered
	assert.False(t, IndexSpecCovered(bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: 1}, {Key: "secret", Value: 1}}},
	}, fields))
	// single non-whitelisted field -> not covered
	assert.False(t, IndexSpecCovered(bson.D{
		{Key: "key", Value: bson.D{{Key: "secret", Value: 1}}},
	}, fields))
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./collector/filter -run 'TestFirstComponent|TestBuildInclusionProjection|TestProjectDocument|TestFilterModifiers|TestIndexSpecCovered' -v`
Expected: 编译失败 `undefined: firstComponent` 等。

- [ ] **Step 3: 实现 helper**（创建 `collector/filter/field_filter.go`）

```go
package filter

import (
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/oplog"
)

// firstComponent returns the part of a field path before the first dot, so a
// whitelisted top-level field also matches its sub-path updates (e.g. selecting
// "profile" keeps "profile.city"). See SDD 2026-09-22 section 3.
func firstComponent(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// BuildInclusionProjection builds {_id:1, f1:1, ...} for server-side projection.
// Keys are sorted for deterministic output.
func BuildInclusionProjection(fields map[string]struct{}) bson.D {
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)

	proj := make(bson.D, 0, len(fields)+1)
	proj = append(proj, bson.E{Key: "_id", Value: 1})
	for _, f := range names {
		proj = append(proj, bson.E{Key: f, Value: 1})
	}
	return proj
}

// ProjectDocument keeps _id and top-level fields whose name is exactly whitelisted.
// Used for insert / replace / looked-up full documents.
func ProjectDocument(doc bson.D, fields map[string]struct{}) bson.D {
	out := make(bson.D, 0, len(fields)+1)
	for _, e := range doc {
		if e.Key == "_id" {
			out = append(out, e)
			continue
		}
		if _, ok := fields[e.Key]; ok {
			out = append(out, e)
		}
	}
	return out
}

// FilterModifiers keeps only $set/$unset entries whose first path component is
// whitelisted. Returns the rebuilt modifier object and whether nothing remains
// (caller should drop the event in that case).
func FilterModifiers(obj bson.D, fields map[string]struct{}) (bson.D, bool) {
	out := make(bson.D, 0, len(obj))
	empty := true
	for _, e := range obj {
		switch e.Key {
		case "$set", "$unset":
			kept := filterModifierValue(e.Value, fields)
			if len(kept) == 0 {
				continue
			}
			empty = false
			out = append(out, bson.E{Key: e.Key, Value: kept})
		default:
			out = append(out, e)
		}
	}
	return out, empty
}

func filterModifierValue(val interface{}, fields map[string]struct{}) bson.M {
	kept := bson.M{}
	switch v := val.(type) {
	case bson.M:
		for k, mv := range v {
			if _, ok := fields[firstComponent(k)]; ok {
				kept[k] = mv
			}
		}
	case bson.D:
		for _, e := range v {
			if _, ok := fields[firstComponent(e.Key)]; ok {
				kept[e.Key] = e.Value
			}
		}
	}
	return kept
}

// IndexSpecCovered reports whether every non-_id field in the index key is
// whitelisted (all-or-nothing). See SDD 2026-09-22 section 8.
func IndexSpecCovered(indexSpec bson.D, fields map[string]struct{}) bool {
	keyDoc, ok := oplog.GetKey(indexSpec, "key").(bson.D)
	if !ok {
		return false
	}
	for _, e := range keyDoc {
		if e.Key == "_id" {
			continue
		}
		if _, ok := fields[firstComponent(e.Key)]; !ok {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./collector/filter -run 'TestFirstComponent|TestBuildInclusionProjection|TestProjectDocument|TestFilterModifiers|TestIndexSpecCovered' -v`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add collector/filter/field_filter.go collector/filter/field_filter_test.go
git commit -m "feat: add field projection helpers (document/modifier/index)"
```

---

### Task 3: FieldFilter（增量过滤）+ 挂入 syncer 过滤链

**Files:**
- Modify: `collector/filter/field_filter.go`（追加 `FieldFilter`）
- Modify: `collector/syncer.go:152-164`（append FieldFilter）
- Test: `collector/filter/field_filter_test.go`（追加）

**Interfaces:**
- Consumes: Task 2 的 `ProjectDocument` / `FilterModifiers` / `IndexSpecCovered`；`conf.Options.IncrSyncFieldWhitelistMap`（Task 1）。
- Produces:
  - `type FieldFilter struct{...}`、`func NewFieldFilter(whitelist map[string]map[string]struct{}) *FieldFilter`
  - `func (f *FieldFilter) Filter(log *oplog.PartialLog) bool`（实现 `OplogFilter`）

- [ ] **Step 1: 写失败测试**（追加到 `collector/filter/field_filter_test.go`）

```go
func TestFieldFilter(t *testing.T) {
	wl := map[string]map[string]struct{}{
		"db1.c1": fieldSet("a", "profile"),
	}
	f := NewFieldFilter(wl)

	// ns not in whitelist -> untouched, keep
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.other", Operation: "i",
			Object: bson.D{{Key: "_id", Value: 1}, {Key: "secret", Value: "x"}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "secret", Value: "x"}}, log.Object)
	}

	// insert -> project full document
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "i",
			Object: bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 10}, {Key: "secret", Value: "x"}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 10}}, log.Object)
	}

	// replace (op=u, no $ prefix) -> project full document
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "u",
			Query:  bson.D{{Key: "_id", Value: 1}},
			Object: bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 11}, {Key: "secret", Value: "y"}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 11}}, log.Object)
	}

	// update modifier -> keep whitelisted (incl. sub-path), drop rest
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "u",
			Query: bson.D{{Key: "_id", Value: 1}},
			Object: bson.D{
				{Key: "$set", Value: bson.M{"a": 1, "secret": 2, "profile.city": "hz"}},
				{Key: "$unset", Value: bson.M{"secret2": 1}},
			},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "$set", Value: bson.M{"a": 1, "profile.city": "hz"}}}, log.Object)
	}

	// update touching only non-whitelisted fields -> dropped
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "u",
			Query:  bson.D{{Key: "_id", Value: 1}},
			Object: bson.D{{Key: "$set", Value: bson.M{"secret": 2}}},
		}}
		assert.True(t, f.Filter(log))
	}

	// delete -> untouched
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "d",
			Object: bson.D{{Key: "_id", Value: 1}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}}, log.Object)
	}
}

// createIndexes coverage is asserted separately below, because change stream
// emits createIndexes on ns "db.$cmd": FieldFilter rebuilds the collection ns
// (db + the createIndexes value) to look up the whitelist.
func TestFieldFilterCreateIndexes(t *testing.T) {
	wl := map[string]map[string]struct{}{"db1.c1": fieldSet("a")}
	f := NewFieldFilter(wl)

	// one covered + one not covered -> keep only covered
	log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.$cmd", Operation: "c",
		Object: bson.D{
			{Key: "createIndexes", Value: "c1"},
			{Key: "indexes", Value: bson.A{
				bson.D{{Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"}},
				bson.D{{Key: "key", Value: bson.D{{Key: "secret", Value: 1}}}, {Key: "name", Value: "secret_1"}},
			}},
		},
	}}
	assert.False(t, f.Filter(log))
	indexes := oplog.GetKey(log.Object, "indexes").(bson.A)
	assert.Len(t, indexes, 1)

	// only non-covered -> drop whole log
	log2 := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.$cmd", Operation: "c",
		Object: bson.D{
			{Key: "createIndexes", Value: "c1"},
			{Key: "indexes", Value: bson.A{
				bson.D{{Key: "key", Value: bson.D{{Key: "secret", Value: 1}}}, {Key: "name", Value: "secret_1"}},
			}},
		},
	}}
	assert.True(t, f.Filter(log2))
}
```

测试文件 import 块加入 `"github.com/alibaba/MongoShake/v2/oplog"`。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./collector/filter -run 'TestFieldFilter' -v`
Expected: 编译失败 `undefined: NewFieldFilter`。

- [ ] **Step 3: 实现 FieldFilter**（追加到 `collector/filter/field_filter.go`；并在 import 加入 `l`）

import 块补充（`field_filter.go` 只需要新增 `l`，**不要** import `conf`，否则 unused）：

```go
	l "github.com/alibaba/MongoShake/v2/pkg/log"
```

追加：

```go
// FieldFilter projects synced documents and indexes down to a per-namespace
// top-level field whitelist. Incremental (change stream) only. It mutates
// log.Object in place and returns true to drop the event when nothing
// whitelisted remains (mirrors OpTypeFilter's rewrite-and-maybe-drop pattern).
type FieldFilter struct {
	whitelist map[string]map[string]struct{}
}

func NewFieldFilter(whitelist map[string]map[string]struct{}) *FieldFilter {
	return &FieldFilter{whitelist: whitelist}
}

func (f *FieldFilter) Filter(log *oplog.PartialLog) bool {
	if log.Operation == "c" {
		return f.filterCommand(log)
	}

	fields, ok := f.whitelist[log.Namespace]
	if !ok || len(fields) == 0 {
		return false
	}

	switch log.Operation {
	case "i":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		log.Object = ProjectDocument(log.Object, fields)
		return false
	case "u":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		if log.ObjectHasPrefix("$") {
			newObj, empty := FilterModifiers(log.Object, fields)
			if empty {
				return true
			}
			log.Object = newObj
			return false
		}
		log.Object = ProjectDocument(log.Object, fields)
		return false
	default: // "d" delete, "n" noop, others: leave untouched
		return false
	}
}

// filterCommand only handles createIndexes; all other commands pass through.
// change stream emits createIndexes on ns "db.$cmd", so rebuild the collection
// namespace from the command value to look up the whitelist.
func (f *FieldFilter) filterCommand(log *oplog.PartialLog) bool {
	if err := log.MaterializeObject(); err != nil {
		l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
		return false
	}
	command, found := oplog.ExtraCommandName(log.Object)
	if !found || command != "createIndexes" {
		return false
	}
	coll, ok := oplog.GetKey(log.Object, "createIndexes").(string)
	if !ok {
		return false
	}
	db := strings.SplitN(log.Namespace, ".", 2)[0]
	fields, ok := f.whitelist[db+"."+coll]
	if !ok || len(fields) == 0 {
		return false
	}
	indexes, ok := oplog.GetKey(log.Object, "indexes").(bson.A)
	if !ok {
		return false
	}
	remain := make(bson.A, 0, len(indexes))
	for _, ele := range indexes {
		spec, ok := ele.(bson.D)
		if !ok {
			remain = append(remain, ele) // unknown shape, keep
			continue
		}
		if IndexSpecCovered(spec, fields) {
			remain = append(remain, ele)
		}
	}
	oplog.SetFiled(log.Object, "indexes", remain)
	return len(remain) == 0
}
```

> 注：`conf` import 仅在 Task 3 的 syncer 接线里需要；`field_filter.go` 本身用不到 `conf`，**不要**在该文件 import `conf`（避免 unused import）。上面 import 块只加 `l`（`pkg/log`），`conf` 加到 `syncer.go`（已存在）。

- [ ] **Step 4: 挂入过滤链**（`collector/syncer.go`，在 namespace filter append 之后、`NewBatcher` 之前，约 line 164-169 之间）

```go
	// field-level whitelist projection (incremental, change_stream only)
	if len(conf.Options.IncrSyncFieldWhitelistMap) != 0 {
		filterList = append(filterList, filter.NewFieldFilter(conf.Options.IncrSyncFieldWhitelistMap))
	}
```

- [ ] **Step 5: 跑测试确认通过 + 构建**

Run: `go test ./collector/filter -run 'TestFieldFilter' -v`
Expected: PASS（含 `TestFieldFilterCreateIndexes`）。

Run: `go build ./... && go vet ./collector/filter ./collector`
Expected: 成功。

- [ ] **Step 6: Commit**

```bash
git add collector/filter/field_filter.go collector/filter/field_filter_test.go collector/syncer.go
git commit -m "feat: incremental FieldFilter for change-stream field projection"
```

---

### Task 4: 全量数据投影（DocumentReader）

**Files:**
- Modify: `collector/docsyncer/doc_reader.go`（`ensureNetwork()`，`Find` 调用前 ~line 410；import 加 filter）
- Test: `collector/docsyncer/doc_reader_test.go`（新建，live Mongo）

**Interfaces:**
- Consumes: `filter.BuildInclusionProjection`（Task 2）、`conf.Options.FullSyncFieldWhitelistMap`（Task 1）。

- [ ] **Step 1: 写失败测试**（创建 `collector/docsyncer/doc_reader_test.go`）

```go
package docsyncer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestDocumentReaderFieldProjection(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no live mongo at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()

	const db, coll = "ff_full_test", "c1"
	ns := utils.NS{Database: db, Collection: coll}
	ctx := context.Background()
	c := conn.Client.Database(db).Collection(coll)
	_ = c.Drop(ctx)
	defer func() { _ = c.Drop(ctx) }()

	_, err = c.InsertOne(ctx, bson.M{"_id": 1, "a": 10, "secret": "x",
		"profile": bson.M{"city": "hz", "phone": "1"}})
	assert.NoError(t, err)

	origin := conf.Options
	defer func() { conf.Options = origin }()
	conf.Options.FullSyncFieldWhitelistMap = map[string]map[string]struct{}{
		"ff_full_test.c1": {"a": {}, "profile": {}},
	}
	conf.Options.FullSyncReaderFetchBatchSize = 16

	reader := NewDocumentReader(0, testMongoAddress, ns, "", nil, nil, "")
	defer reader.Close()

	doc, err := reader.NextDoc()
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	var got bson.M
	assert.NoError(t, bson.Unmarshal(doc, &got))
	assert.Equal(t, 1, got["_id"])
	assert.Equal(t, 10, got["a"])
	_, hasSecret := got["secret"]
	assert.False(t, hasSecret, "secret should be projected out")
	profile, ok := got["profile"].(bson.M)
	assert.True(t, ok)
	assert.Equal(t, "hz", profile["city"]) // whole sub-tree retained
}
```

> 该测试 import 需要 `"context"`；把它加进 import 块。

- [ ] **Step 2: 跑测试确认失败**

Run: `MONGOSHAKE_TEST_URL=mongodb://127.0.0.1:27030 go test ./collector/docsyncer -run TestDocumentReaderFieldProjection -v`
Expected: FAIL —— `secret` 仍存在（尚未加投影）。（无 live Mongo 时 SKIP；用 `AGENTS.md` 的 `mshake-cs-src:27030`。）

- [ ] **Step 3: 实现投影**（`collector/docsyncer/doc_reader.go`）

import 块加入：

```go
	"github.com/alibaba/MongoShake/v2/collector/filter"
```

在 `ensureNetwork()` 里、`findOptions.SetComment(...)` 之后、`Find(...)` 之前插入：

```go
	if fields := conf.Options.FullSyncFieldWhitelistMap[reader.ns.Str()]; len(fields) > 0 {
		findOptions.SetProjection(filter.BuildInclusionProjection(fields))
	}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `MONGOSHAKE_TEST_URL=mongodb://127.0.0.1:27030 go test ./collector/docsyncer -run TestDocumentReaderFieldProjection -v`
Expected: PASS。

Run: `go build ./...`
Expected: 成功。

- [ ] **Step 5: Commit**

```bash
git add collector/docsyncer/doc_reader.go collector/docsyncer/doc_reader_test.go
git commit -m "feat: full-sync server-side field projection in DocumentReader"
```

---

### Task 5: 全量索引过滤（StartIndexSync）

**Files:**
- Modify: `collector/docsyncer/doc_syncer.go`（`StartIndexSync` 索引循环 ~line 270-274）
- Test: `collector/docsyncer/doc_syncer_test.go`（追加，live Mongo）

**Interfaces:**
- Consumes: `filter.IndexSpecCovered`（Task 2）、`conf.Options.FullSyncFieldWhitelistMap`（Task 1）、`utils.HaveIdIndexKey`。

- [ ] **Step 1: 写失败测试**（追加到 `collector/docsyncer/doc_syncer_test.go`）

```go
func TestStartIndexSyncFieldWhitelist(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no live mongo at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()

	const db, coll = "ff_idx_test", "c1"
	ctx := context.Background()
	c := conn.Client.Database(db).Collection(coll)
	_ = c.Drop(ctx)
	defer func() { _ = c.Drop(ctx) }()

	origin := conf.Options
	defer func() { conf.Options = origin }()
	conf.Options.FullSyncFieldWhitelistMap = map[string]map[string]struct{}{
		"ff_idx_test.c1": {"a": {}},
	}

	indexMap := map[utils.NS][]bson.D{
		{Database: db, Collection: coll}: {
			{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "name", Value: "_id_"}},
			{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"}},
			{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "secret", Value: 1}}}, {Key: "name", Value: "secret_1"}},
			{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "a", Value: 1}, {Key: "secret", Value: 1}}}, {Key: "name", Value: "a_1_secret_1"}},
		},
	}

	assert.NoError(t, StartIndexSync(indexMap, testMongoAddress, nil, false))

	names := map[string]bool{}
	cur, err := c.Indexes().List(ctx)
	assert.NoError(t, err)
	var specs []bson.M
	assert.NoError(t, cur.All(ctx, &specs))
	for _, s := range specs {
		if n, ok := s["name"].(string); ok {
			names[n] = true
		}
	}
	assert.True(t, names["a_1"], "covered index should be created")
	assert.False(t, names["secret_1"], "non-covered single index should be skipped")
	assert.False(t, names["a_1_secret_1"], "compound with non-covered field should be skipped")
}
```

> import 块加入 `"context"`（若尚未存在）。

- [ ] **Step 2: 跑测试确认失败**

Run: `MONGOSHAKE_TEST_URL=mongodb://127.0.0.1:27030 go test ./collector/docsyncer -run TestStartIndexSyncFieldWhitelist -v`
Expected: FAIL —— `secret_1` / `a_1_secret_1` 仍被创建。

- [ ] **Step 3: 实现索引过滤**（`collector/docsyncer/doc_syncer.go`，`StartIndexSync` 内 `for _, index := range indexNs.indexList {` 循环体，紧跟现有 `_id` 跳过之后）

```go
					// ignore _id
					if utils.HaveIdIndexKey(index) {
						continue
					}

					// field whitelist: skip indexes referencing non-whitelisted fields
					if fields := conf.Options.FullSyncFieldWhitelistMap[ns.Str()]; len(fields) > 0 {
						if !filter.IndexSpecCovered(index, fields) {
							l.Logger.Infof("skip index for ns[%v] not covered by field whitelist: %v", ns, index)
							continue
						}
					}
```

> `ns` 即 `indexNs.ns`（源 ns）；`doc_syncer.go` 已 import `filter`/`conf`/`utils`/`l`。

- [ ] **Step 4: 跑测试确认通过**

Run: `MONGOSHAKE_TEST_URL=mongodb://127.0.0.1:27030 go test ./collector/docsyncer -run TestStartIndexSyncFieldWhitelist -v`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add collector/docsyncer/doc_syncer.go collector/docsyncer/doc_syncer_test.go
git commit -m "feat: filter full-sync indexes by field whitelist (all-or-nothing)"
```

---

### Task 6: 配置文档（conf/collector.conf）

**Files:**
- Modify: `conf/collector.conf`（全量段、增量段各加一项带注释）

- [ ] **Step 1: 加全量字段白名单文档**（在 `full_sync.executor.immutable_shard_key_fallback` 那一项之后，全量段末尾追加）

```ini
# top-level field whitelist for full sync. empty means sync all fields.
# format: db.collection:field1,field2;db2.collection2:fieldA
# - namespace must be db.collection and must be covered by filter.namespace.white
# - field names must NOT contain a dot (nested/array sub-field selection is not supported in v1)
# - the whole value of a selected top-level field is synced (object/array included)
# - if the same ns is also set in incr_sync.field.whitelist, the field sets must be identical
# 全量阶段顶层字段白名单，为空表示同步全部字段。被选中的顶层字段整值同步；
# 不支持子字段/数组内字段；字段名不能含点；ns 必须被 filter.namespace.white 覆盖。
full_sync.field.whitelist =
```

- [ ] **Step 2: 加增量字段白名单文档**（在 `incr_sync.executor.bypass_document_validation` 之后追加）

```ini
# top-level field whitelist for incremental sync. empty means sync all fields.
# requires incr_sync.mongo_fetch_method = change_stream (oplog mode is not supported).
# format and rules are the same as full_sync.field.whitelist.
# indexes are also filtered: an index is synced only if every non-_id key field is whitelisted.
# 增量阶段顶层字段白名单，仅支持 change_stream；格式与约束同 full_sync.field.whitelist。
# 索引同样过滤：仅当索引 key 中所有非 _id 字段都在白名单内才同步。
incr_sync.field.whitelist =
```

- [ ] **Step 3: 校验模板仍可解析**

Run: `go build ./...`
Expected: 成功（仅注释/空值，不影响解析）。

- [ ] **Step 4: Commit**

```bash
git add conf/collector.conf
git commit -m "docs: document field whitelist options in collector.conf"
```

---

### Task 7: 集成测试（端到端，本地 docker）

**Files:**
- Create: `integration/field_filter_test.go`（`//go:build integration`）

**Interfaces:**
- Consumes: 同 package 的 `csEnvOr` / `csDial` / `csBuildCollector` / `csStartCollector` / `csRepoRoot`（来自 `changestream_truncated_arrays_test.go`）。

- [ ] **Step 1: 写集成测试**（创建 `integration/field_filter_test.go`）

```go
//go:build integration

// End-to-end verification for field-level (top-level) whitelist sync:
//   - full sync projects documents to {_id + whitelisted top-level fields}
//   - full sync only creates indexes fully covered by the whitelist
//   - incremental (change stream) propagates whitelisted field updates,
//     ignores non-whitelisted updates, projects inserts/replaces, and deletes
//
// Run with:
//
//	go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m
//
// Environment (overridable): MSHAKE_CS_SRC_URL (replica set), MSHAKE_TGT_URL,
// MSHAKE_COLLECTOR_BIN. See AGENTS.md for the local docker topology.
package integration

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	ffDB     = "ff_it"
	ffColl   = "c1"
	ffCkptDB = "mongoshake"
	ffCkpt   = "ckpt_ff_it"
	ffTO     = 120 * time.Second
	ffPoll   = 500 * time.Millisecond
)

func ffWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(csRepoRoot(t), "conf", "collector.conf"))
	if err != nil {
		t.Fatalf("read conf template: %v", err)
	}
	confStr := string(tpl)
	set := func(key, value string) {
		re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*$`)
		if !re.MatchString(confStr) {
			confStr += "\n" + key + " = " + value + "\n"
			return
		}
		confStr = re.ReplaceAllString(confStr, key+" = "+value)
	}
	set("id", "ff_it")
	set("sync_mode", "all")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("tunnel", "direct")
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("filter.namespace.white", ffDB+"."+ffColl)
	set("full_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("incr_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("full_sync.collection_exist_drop", "true")
	set("full_sync.create_index", "foreground")
	set("checkpoint.storage.collection", ffCkpt)
	set("checkpoint.interval", "1000")
	set("full_sync.http_port", "19311")
	set("incr_sync.http_port", "19310")
	set("prom.http_port", "19312")
	set("system_profile_port", "19410")
	set("log.dir", logDir)
	set("log.file", "collector-cs986.log") // reuse csCollectorProc.dumpLog
	set("tunnel.kafka.producer.max_message_bytes", "18874368") // see issue #998

	path := filepath.Join(logDir, "collector-ff.conf")
	if err := os.WriteFile(path, []byte(confStr), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func ffWaitDoc(t *testing.T, tgt *mongo.Client, id interface{},
	pred func(bson.M) bool, timeout time.Duration) (bson.M, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bson.M
	for time.Now().Before(deadline) {
		var got bson.M
		err := tgt.Database(ffDB).Collection(ffColl).
			FindOne(context.Background(), bson.M{"_id": id}).Decode(&got)
		if err == nil {
			last = got
			if pred(got) {
				return got, true
			}
		}
		time.Sleep(ffPoll)
	}
	return last, false
}

func TestFieldFilterSync(t *testing.T) {
	srcURL := csEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := csEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := csDial(t, srcURL)
	tgt := csDial(t, tgtURL)
	ctx := context.Background()

	var hello bson.M
	if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("hello on source: %v", err)
	}
	if hello["setName"] == nil {
		t.Skipf("source %s is not a replica set; change streams unavailable", srcURL)
	}

	srcColl := src.Database(ffDB).Collection(ffColl)
	tgtColl := tgt.Database(ffDB).Collection(ffColl)
	_ = srcColl.Drop(ctx)
	_ = tgtColl.Drop(ctx)
	_ = src.Database(ffCkptDB).Collection(ffCkpt).Drop(ctx)

	// seed: one whitelisted scalar (a), one whitelisted object (profile), one secret
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10, "secret": "x",
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// indexes: covered (a_1), non-covered single (secret_1), non-covered compound (a_1_secret_1)
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}})
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "secret", Value: 1}}})
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "a", Value: 1}, {Key: "secret", Value: 1}}})

	bin := csBuildCollector(t)
	logDir := t.TempDir()
	confPath := ffWriteConf(t, srcURL, tgtURL, logDir)
	proc := csStartCollector(t, bin, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// 1. full sync projects the document
	got, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool {
		_, hasSecret := d["secret"]
		_, hasA := d["a"]
		_, hasProfile := d["profile"]
		return hasA && hasProfile && !hasSecret
	}, ffTO)
	if !ok {
		t.Fatalf("full sync did not project document within %v, got %v", ffTO, got)
	}
	if p, ok := got["profile"].(bson.M); !ok || p["city"] != "hz" || p["phone"] != "1" {
		t.Fatalf("whole profile sub-tree should be retained, got %v", got)
	}

	// 2. full sync index filtering
	deadline := time.Now().Add(ffTO)
	var names map[string]bool
	for time.Now().Before(deadline) {
		names = map[string]bool{}
		cur, err := tgtColl.Indexes().List(ctx)
		if err == nil {
			var specs []bson.M
			if cur.All(ctx, &specs) == nil {
				for _, s := range specs {
					if n, ok := s["name"].(string); ok {
						names[n] = true
					}
				}
			}
		}
		if names["a_1"] {
			break
		}
		time.Sleep(ffPoll)
	}
	if !names["a_1"] {
		t.Fatalf("covered index a_1 missing on target, got %v", names)
	}
	if names["secret_1"] || names["a_1_secret_1"] {
		t.Fatalf("non-covered indexes should be skipped, got %v", names)
	}

	// give the change stream a moment to be established after full sync
	time.Sleep(5 * time.Second)

	// 3. incremental: update whitelisted field -> propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"a": 99}}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool { return d["a"] == 99 }, ffTO); !ok {
		t.Fatalf("whitelisted field update did not propagate")
	}

	// 4. incremental: update non-whitelisted field -> target unchanged (still no secret)
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"secret": "y"}}); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	time.Sleep(5 * time.Second)
	var afterSecret bson.M
	if err := tgtColl.FindOne(ctx, bson.M{"_id": 1}).Decode(&afterSecret); err != nil {
		t.Fatalf("read target: %v", err)
	}
	if _, has := afterSecret["secret"]; has {
		t.Fatalf("non-whitelisted field must not appear on target, got %v", afterSecret)
	}

	// 5. incremental: update sub-path of whitelisted object -> propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$set": bson.M{"profile.city": "sh"}}); err != nil {
		t.Fatalf("update profile.city: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool {
		p, ok := d["profile"].(bson.M)
		return ok && p["city"] == "sh"
	}, ffTO); !ok {
		t.Fatalf("sub-path update of whitelisted object did not propagate")
	}

	// 6. incremental: insert new doc -> projected
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 2, "a": 5, "secret": "z"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 2, func(d bson.M) bool {
		_, hasSecret := d["secret"]
		return d["a"] == 5 && !hasSecret
	}, ffTO); !ok {
		t.Fatalf("inserted doc not projected on target")
	}

	// 7. incremental: delete -> removed on target
	if _, err := srcColl.DeleteOne(ctx, bson.M{"_id": 2}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	delDeadline := time.Now().Add(ffTO)
	for time.Now().Before(delDeadline) {
		err := tgtColl.FindOne(ctx, bson.M{"_id": 2}, options.FindOne()).Decode(&bson.M{})
		if err == mongo.ErrNoDocuments {
			break
		}
		time.Sleep(ffPoll)
	}
	var stillThere bson.M
	if err := tgtColl.FindOne(ctx, bson.M{"_id": 2}).Decode(&stillThere); err == nil {
		t.Fatalf("deleted doc still on target: %v", stillThere)
	}
}
```

- [ ] **Step 2: 起本地 docker 环境**（见 `AGENTS.md`）

Run: `docker start mshake-cs-src mshake-txn-tgt 2>/dev/null; docker ps --format '{{.Names}}' | grep mshake`
Expected: 至少看到 `mshake-cs-src`、`mshake-txn-tgt`（若不存在按 AGENTS.md 重建）。

- [ ] **Step 3: 跑集成测试**

Run: `go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m`
Expected: PASS。若环境缺失则 SKIP（`csDial` 的 `t.Skipf`）。

- [ ] **Step 4: 红绿对照（可选，证明测试有效）**

用 develop 基线二进制（无本特性）跑应失败/不一致：

Run: `MSHAKE_COLLECTOR_BIN=/path/to/baseline/collector go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m`
Expected: FAIL（基线不过滤字段，目标会含 `secret`）。

- [ ] **Step 5: Commit**

```bash
git add integration/field_filter_test.go
git commit -m "test: integration test for field-level whitelist sync (full + incr + index)"
```

---

## 验收对照（与 SDD 第 14 节）

- 非法配置启动期报错：Task 1（`TestParseFieldWhitelists` 覆盖格式/含点/未覆盖/一致性/change_stream）。
- 全量只同步白名单字段 + 索引：Task 4、Task 5、Task 7。
- 增量 insert/replace/update/delete + 非白名单 no-op + 子路径刷新：Task 3（单元）、Task 7（端到端）。
- 零回归：所有新增逻辑 `len(fields)>0` 守卫；运行 `go test ./...`（live-Mongo 用例外 SKIP）确认既有用例不回归。

## Self-Review 记录

- Spec 覆盖：第 4/5 节→Task 1；第 6.1→Task 4；第 6.2/8→Task 5；第 7.1-7.3→Task 3；第 5.3 联动→Task 1；文档→Task 6；测试→Task 1/2/3/4/5/7。无遗漏。
- 占位符：无 TBD/TODO；每个改代码步骤均给出完整代码。
- 类型一致：`map[string]map[string]struct{}`、`BuildInclusionProjection`/`ProjectDocument`/`FilterModifiers`/`IndexSpecCovered`/`firstComponent`/`NewFieldFilter`/`FieldFilter.Filter` 在各 Task 间签名一致；`ns.Str()`、`oplog.GetKey`、`oplog.SetFiled`、`oplog.ExtraCommandName`、`utils.HaveIdIndexKey`、`log.MaterializeObject`、`log.ObjectHasPrefix` 均按仓库现有签名使用。
