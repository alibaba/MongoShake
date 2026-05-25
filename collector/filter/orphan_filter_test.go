package filter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/sharding"
)

const testNs = "db.coll"

// keyAll covers the entire hash space; lets hashed-shard tests assert that
// ComputeHash is wired up without depending on the chunkLt/chunkGt int64
// precision issue (see TestOrphanFilter_HashedPrecisionBug below).
var keyAll = []*sharding.ChunkRange{{
	Mins: []interface{}{int64(math.MinInt64)},
	Maxs: []interface{}{int64(math.MaxInt64)},
}}

func mkRange(t *testing.T, sc *sharding.ShardCollection) sharding.DBChunkMap {
	t.Helper()
	return sharding.DBChunkMap{testNs: sc}
}

// chunkMap is nil: no chunk info loaded, do not filter.
func TestOrphanFilter_NilChunkMap(t *testing.T) {
	f := NewOrphanFilter("rs0", nil)
	assert.False(t, f.Filter(bson.D{{"x", 1}}, testNs))
}

// namespace not present in chunk map: collection isn't sharded, do not filter.
func TestOrphanFilter_NamespaceMissing(t *testing.T) {
	f := NewOrphanFilter("rs0", sharding.DBChunkMap{
		"other.coll": &sharding.ShardCollection{
			Keys: []string{"x"}, ShardType: sharding.RangedShard,
			Chunks: []*sharding.ChunkRange{{Mins: []interface{}{0}, Maxs: []interface{}{10}}},
		},
	})
	assert.False(t, f.Filter(bson.D{{"x", 1}}, testNs))
}

// range, single shard key: in/out/min/max boundary semantics.
func TestOrphanFilter_RangeSingleKey(t *testing.T) {
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"x"}, ShardType: sharding.RangedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{1}, Maxs: []interface{}{10}},
			{Mins: []interface{}{50}, Maxs: []interface{}{100}},
		},
	})
	f := NewOrphanFilter("rs0", cm)

	cases := []struct {
		name   string
		key    int
		orphan bool
	}{
		{"in first chunk", 5, false},
		{"in second chunk", 75, false},
		{"between chunks", 30, true},
		{"before all", 0, true},
		{"after all", 200, true},
		{"equal first min (inclusive)", 1, false},
		{"equal first max (exclusive)", 10, true},
		{"equal second min (inclusive)", 50, false},
		{"equal second max (exclusive)", 100, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.orphan, f.Filter(bson.D{{"x", c.key}}, testNs))
		})
	}
}

// range, compound shard key {a, b}: covers boundary cases that are most easy
// to get wrong in lexicographic comparison (this is exactly the layout from
// issue #978: "分片键为两个字段的联合分片").
func TestOrphanFilter_RangeCompoundKey(t *testing.T) {
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"a", "b"}, ShardType: sharding.RangedShard,
		Chunks: []*sharding.ChunkRange{
			// chunk: [{a:1, b:100}, {a:5, b:200})
			{Mins: []interface{}{1, 100}, Maxs: []interface{}{5, 200}},
		},
	})
	f := NewOrphanFilter("rs0", cm)

	cases := []struct {
		name   string
		a, b   int
		orphan bool
	}{
		{"a<min_a", 0, 150, true},
		{"a==min_a, b<min_b", 1, 50, true},
		{"a==min_a, b==min_b (lower bound inclusive)", 1, 100, false},
		{"a==min_a, b in range", 1, 150, false},
		{"a in range, b anywhere", 3, 1, false},
		{"a in range, b at upper sentinel", 3, 99999, false},
		{"a==max_a, b<max_b", 5, 199, false},
		{"a==max_a, b==max_b (upper bound exclusive)", 5, 200, true},
		{"a==max_a, b>max_b", 5, 201, true},
		{"a>max_a", 6, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.orphan, f.Filter(bson.D{{"a", c.a}, {"b", c.b}}, testNs))
		})
	}
}

// hashed shard: cover every key type supported by ComputeHash end-to-end
// (ComputeHash branch + Filter wiring). [MinInt64, MaxInt64] chunk so any
// hash lands inside — checks plumbing, not boundary math (the boundary
// math is exercised by TestOrphanFilter_HashedTightChunk below).
func TestOrphanFilter_HashedAllTypes(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
	}{
		{"ObjectID", primitive.NewObjectID()},
		{"string", "hello-orphan"},
		{"int64", int64(1234567890)},
		{"int", 42},
		{"int32", int32(42)},
		{"float64", 3.14},
		{"bool/true", true},
		{"bool/false", false},
		{"DateTime", primitive.DateTime(1715000000000)},
		{"Timestamp", primitive.Timestamp{T: 1715000000, I: 7}},
		// nil shard-key values are exercised separately in
		// TestComputeHash_NullKey because OrphanFilter.Filter itself
		// can't distinguish "missing field" from "field with value
		// null" (oplog.GetKey returns nil for both); pushing nil
		// through Filter would panic in a way unrelated to this bug.
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cm := mkRange(t, &sharding.ShardCollection{
				Keys: []string{"k"}, ShardType: sharding.HashedShard,
				Chunks: keyAll,
			})
			f := NewOrphanFilter("rs0", cm)
			assert.False(t, f.Filter(bson.D{{"k", c.val}}, testNs),
				"hashed key in [MinInt64, MaxInt64] chunk should never be orphan")
		})
	}
}

// hashed shard with a tight chunk around the computed hash: exercises the
// int64-precise comparison path in chunkLt/chunkGt that used to silently
// collapse via float64 (was TestOrphanFilter_HashedPrecisionBug, now
// fixed by introducing BsonTypeInt64).
func TestOrphanFilter_HashedTightChunk(t *testing.T) {
	oid := primitive.NewObjectID()
	hashed := ComputeHash(oid)

	in := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"_id"}, ShardType: sharding.HashedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{hashed - 1}, Maxs: []interface{}{hashed + 1}},
		},
	})
	assert.False(t, NewOrphanFilter("rs0", in).Filter(bson.D{{"_id", oid}}, testNs),
		"hashed key inside ±1 chunk should not be orphan")

	out := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"_id"}, ShardType: sharding.HashedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{hashed + 100}, Maxs: []interface{}{hashed + 200}},
		},
	})
	assert.True(t, NewOrphanFilter("rs0", out).Filter(bson.D{{"_id", oid}}, testNs),
		"hashed key below chunk min should be orphan")
}

// hashed shard, no chunks held by this replset: every doc is orphan
// (verifies the "iterate-all-then-fall-through" path).
func TestOrphanFilter_HashedNoChunks(t *testing.T) {
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"k"}, ShardType: sharding.HashedShard,
		Chunks: nil,
	})
	f := NewOrphanFilter("rs0", cm)
	assert.True(t, f.Filter(bson.D{{"k", primitive.NewObjectID()}}, testNs))
}

// Regression guard: hashed-shard chunk bounds at the extremes of int64
// must compare exactly, not via float64 (which would collapse pairs of
// adjacent int64s above 2^53 to the same float). Picks a hash value far
// outside float64's contiguous-integer range so the assertion fails if
// the precision fix regresses.
func TestOrphanFilter_HashedInt64Precision(t *testing.T) {
	// 2^62 + 1 and 2^62 - 1 are distinct int64 but collide in float64.
	const target = int64(1)<<62 + 1
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"k"}, ShardType: sharding.HashedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{target}, Maxs: []interface{}{target + 1}},
		},
	})
	// A key whose computed hash is target-1 should be orphan (below min);
	// we can't make ComputeHash produce target on demand, so instead test
	// the chunkLt/chunkGt primitives directly to anchor the contract.
	assert.True(t, chunkLt(target-1, target), "target-1 must be strictly < target")
	assert.True(t, chunkGt(target+1, target), "target+1 must be strictly > target")
	assert.False(t, chunkEqual(target-1, target), "target-1 must not equal target")
	assert.False(t, chunkEqual(target+1, target), "target+1 must not equal target")

	// And the Filter wiring with a tight chunk around int64-precise bounds
	// is exercised in TestOrphanFilter_HashedTightChunk above; keep this
	// reference here so the next reader knows where to look.
	_ = cm
}

// BSON null is hashed with no value bytes (only the canonical type tag).
// Tested at the ComputeHash level because OrphanFilter.Filter conflates
// "field missing" with "field is null" via oplog.GetKey.
func TestComputeHash_NullKey(t *testing.T) {
	// Should not panic, and should be deterministic / distinct from other
	// hashes (we just check it doesn't panic and returns a stable value).
	a := ComputeHash(nil)
	b := ComputeHash(nil)
	assert.Equal(t, a, b, "ComputeHash(nil) must be deterministic")
	assert.NotEqual(t, a, ComputeHash(int64(0)),
		"hash of null must differ from hash of int64(0)")
}

// ComputeHash supports the BSON value types most commonly used as
// hashed-shard keys. Decimal128 / BinData / Symbol are intentionally not
// supported (see comment in ComputeHash). This test pins the panic so any
// future broadening of type support has to update the assertion and
// double-check the byte layout against MongoDB hasher.cpp.
func TestOrphanFilter_HashedUnsupportedTypePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected ComputeHash to panic on unsupported type, got no panic")
		}
	}()
	ComputeHash(primitive.Decimal128{}) // not in the switch, must panic
}

// getBsonType must recognise primitive.MinKey / primitive.MaxKey as they come
// out of bson.Unmarshal when reading config.chunks boundaries from MongoDB.
// The int64 sentinel path (math.MinInt64/MaxInt64) is kept for backwards compat
// with test code that constructs chunk ranges by hand.
func TestGetBsonType_PrimitiveMinMaxKey(t *testing.T) {
	// Simulate what bson.Unmarshal produces for a chunk boundary containing MinKey/MaxKey.
	type chunkBound struct {
		X interface{} `bson:"x"`
	}
	// Encode MinKey, then decode — the round-tripped value is primitive.MinKey{}.
	minDoc, err := bson.Marshal(bson.D{{"x", primitive.MinKey{}}})
	assert.NoError(t, err)
	maxDoc, err := bson.Marshal(bson.D{{"x", primitive.MaxKey{}}})
	assert.NoError(t, err)

	var minResult, maxResult chunkBound
	assert.NoError(t, bson.Unmarshal(minDoc, &minResult))
	assert.NoError(t, bson.Unmarshal(maxDoc, &maxResult))

	// getBsonType must return BsonMinKey / BsonMaxKey
	typ, val := getBsonType(minResult.X)
	assert.Equal(t, BsonMinKey, typ)
	assert.Nil(t, val)

	typ, val = getBsonType(maxResult.X)
	assert.Equal(t, BsonMaxKey, typ)
	assert.Nil(t, val)

	// Also verify the int64 sentinel still works (backward compat)
	typ, _ = getBsonType(int64(math.MinInt64))
	assert.Equal(t, BsonMinKey, typ)
	typ, _ = getBsonType(int64(math.MaxInt64))
	assert.Equal(t, BsonMaxKey, typ)
}

// End-to-end test: range shard with real BSON MinKey/MaxKey boundaries (as
// decoded from config.chunks) must correctly classify docs as non-orphan.
func TestOrphanFilter_RangeWithBsonMinMaxKey(t *testing.T) {
	// First/last chunks in a real sharded collection use MinKey/MaxKey.
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"x"}, ShardType: sharding.RangedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{primitive.MinKey{}}, Maxs: []interface{}{50}},
			{Mins: []interface{}{50}, Maxs: []interface{}{primitive.MaxKey{}}},
		},
	})
	f := NewOrphanFilter("rs0", cm)

	// Everything should be non-orphan since chunks cover [MinKey, MaxKey)
	cases := []struct {
		name   string
		val    interface{}
		orphan bool
	}{
		{"below zero", -100, false},
		{"zero", 0, false},
		{"at split", 50, false},
		{"above split", 99, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.orphan, f.Filter(bson.D{{"x", c.val}}, testNs))
		})
	}
}
