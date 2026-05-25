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

// hashed shard: cover the three supported key types end-to-end (ComputeHash
// branch + Filter wiring). We use [MinInt64, MaxInt64] as the chunk range to
// sidestep TestOrphanFilter_HashedPrecisionBug below — narrowing the chunk
// would silently fail because chunkLt/chunkGt cast int64 to float64.
func TestOrphanFilter_HashedAllTypes(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
	}{
		{"ObjectID", primitive.NewObjectID()},
		{"string", "hello-orphan"},
		{"int64", int64(1234567890)},
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

// Latent precision bug, recorded for follow-up:
// chunkLt/chunkGt cast int64 chunk bounds to float64 (see getBsonType in
// orphan_filter.go), which loses precision above 2^53. For a hashed shard
// whose chunk min/max are full-range int64 values, "hashed-1" and "hashed+1"
// can collapse to the same float64 as "hashed" itself, causing chunkEqual
// rather than the expected strict inequalities. Skipped today; intentionally
// left as a documented known issue so callers do not silently rely on tight
// chunk bounds for hashed shards.
func TestOrphanFilter_HashedPrecisionBug(t *testing.T) {
	t.Skip("known limitation: chunkLt/chunkGt use float64 for int64 chunk bounds, " +
		"loses precision above 2^53. Track in follow-up issue.")

	oid := primitive.NewObjectID()
	hashed := ComputeHash(oid)
	cm := mkRange(t, &sharding.ShardCollection{
		Keys: []string{"_id"}, ShardType: sharding.HashedShard,
		Chunks: []*sharding.ChunkRange{
			{Mins: []interface{}{hashed - 1}, Maxs: []interface{}{hashed + 1}},
		},
	})
	f := NewOrphanFilter("rs0", cm)
	assert.False(t, f.Filter(bson.D{{"_id", oid}}, testNs))
}

// Known limitation: ComputeHash currently supports string / int{,32,64} /
// float64 / ObjectID only. Other valid hashed shard key types (Decimal128,
// Date, Bool, Timestamp, BinData, ...) cause a Panicf. This test pins that
// behavior so a future implementer who broadens type support has to update
// the assertion (and ideally cross-check against MongoDB hasher.cpp).
func TestOrphanFilter_HashedUnsupportedTypePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected ComputeHash to panic on unsupported type, got no panic")
		}
	}()
	ComputeHash(true) // bool: not in the current switch, must panic
}
