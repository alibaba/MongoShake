package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// uuidOf builds a deterministic 16-byte subtype-4 Binary, the shape
// config.chunks / config.collections use for "uuid".
func uuidOf(fill byte) primitive.Binary {
	data := make([]byte, 16)
	for i := range data {
		data[i] = fill
	}
	return primitive.Binary{Subtype: 0x04, Data: data}
}

func rawOf(t *testing.T, d bson.D) *bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	assert.NoError(t, err)
	r := bson.Raw(b)
	return &r
}

func mustIndex(t *testing.T, docs ...CollectionDoc) *collectionIndex {
	t.Helper()
	idx, err := buildCollectionIndex(docs)
	assert.NoError(t, err)
	assert.NotNil(t, idx)
	return idx
}

func testShardMap() map[string]string {
	return map[string]string{"shard1": "rs1", "shard2": "rs2"}
}

// ---------------------------------------------------------------------------
// BSON struct tags. This is where #1017 actually lived: ChunkDoc only carried
// an "ns" tag, so a 5.0+ chunk decoded to an empty namespace.
// ---------------------------------------------------------------------------

// TestChunkDocDecodesBothShapes pins the dual shape of config.chunks across
// the 5.0 boundary. Verified against real servers: 4.4.30 writes ns and no
// uuid; 5.0.33 / 6.0.28 / 7.0.37 / 8.0.4 / 8.2.12 write uuid and no ns.
func TestChunkDocDecodesBothShapes(t *testing.T) {
	t.Run("5.0+ shape carries uuid and no ns", func(t *testing.T) {
		u := uuidOf(0x11)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: primitive.ObjectID{}},
			{Key: "uuid", Value: u},
			{Key: "min", Value: bson.D{{Key: "k", Value: primitive.MinKey{}}}},
			{Key: "max", Value: bson.D{{Key: "k", Value: primitive.MaxKey{}}}},
			{Key: "shard", Value: "shard1"},
			{Key: "lastmod", Value: primitive.Timestamp{T: 1, I: 0}},
			{Key: "history", Value: []bson.M{}},
		})
		assert.NoError(t, err)

		var got ChunkDoc
		assert.NoError(t, bson.Unmarshal(raw, &got))
		assert.Equal(t, "", got.Ns, "5.0+ chunks carry no ns")
		assert.Equal(t, u.Data, got.UUID.Data, "uuid must survive decoding on 5.0+")
		assert.Equal(t, byte(0x04), got.UUID.Subtype)
		assert.Equal(t, "shard1", got.Shard)
		assert.NotNil(t, got.Min)
		assert.NotNil(t, got.Max)
	})

	t.Run("pre-5.0 shape carries ns and no uuid", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: primitive.ObjectID{}},
			{Key: "ns", Value: "mst.probe"},
			{Key: "min", Value: bson.D{{Key: "k", Value: primitive.MinKey{}}}},
			{Key: "max", Value: bson.D{{Key: "k", Value: primitive.MaxKey{}}}},
			{Key: "shard", Value: "shard1"},
			{Key: "lastmodEpoch", Value: primitive.ObjectID{}},
		})
		assert.NoError(t, err)

		var got ChunkDoc
		assert.NoError(t, bson.Unmarshal(raw, &got))
		assert.Equal(t, "mst.probe", got.Ns)
		assert.Empty(t, got.UUID.Data, "pre-5.0 chunks carry no uuid")
		assert.Equal(t, "shard1", got.Shard)
	})
}

// TestCollectionDocDecodes pins the config.collections tags. _id is the
// namespace string on every version probed (4.4 through 8.2), which is what
// makes the uuid -> ns reverse lookup possible without a version probe.
func TestCollectionDocDecodes(t *testing.T) {
	u := uuidOf(0x22)
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "mst.probe"},
		{Key: "lastmodEpoch", Value: primitive.ObjectID{}},
		{Key: "lastmod", Value: primitive.Timestamp{T: 1, I: 0}},
		{Key: "uuid", Value: u},
		{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "hashed"}}},
		{Key: "unique", Value: false},
		{Key: "noBalance", Value: false},
	})
	assert.NoError(t, err)

	var got CollectionDoc
	assert.NoError(t, bson.Unmarshal(raw, &got))
	assert.Equal(t, "mst.probe", got.Ns)
	assert.Equal(t, u.Data, got.UUID.Data)
	assert.Equal(t, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "hashed"}}, got.Key)

	// key order must survive: parseShardKey and the chunk min/max validation
	// both depend on positional correspondence with the shard key.
	assert.Equal(t, "a", got.Key[0].Key)
	assert.Equal(t, "b", got.Key[1].Key)
}

// ---------------------------------------------------------------------------
// buildCollectionIndex / resolve
// ---------------------------------------------------------------------------

func TestBuildCollectionIndex_ResolvesByNsAndByUUID(t *testing.T) {
	u1, u2 := uuidOf(0x01), uuidOf(0x02)
	idx := mustIndex(t,
		CollectionDoc{Ns: "db.ranged", UUID: u1, Key: bson.D{{Key: "a", Value: int32(1)}}},
		CollectionDoc{Ns: "db.hashed", UUID: u2, Key: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "hashed"}}},
	)

	t.Run("by ns (pre-5.0 chunk)", func(t *testing.T) {
		m, err := idx.resolve("db.ranged", primitive.Binary{})
		assert.NoError(t, err)
		assert.Equal(t, "db.ranged", m.ns)
		assert.Equal(t, []string{"a"}, m.keys)
		assert.Equal(t, []string{RangedShard}, m.shardTypes)
	})

	t.Run("by uuid (5.0+ chunk)", func(t *testing.T) {
		m, err := idx.resolve("", u2)
		assert.NoError(t, err)
		assert.Equal(t, "db.hashed", m.ns)
		assert.Equal(t, []string{"a", "b"}, m.keys)
		assert.Equal(t, []string{RangedShard, HashedShard}, m.shardTypes)
	})

	t.Run("ns wins when both present", func(t *testing.T) {
		m, err := idx.resolve("db.ranged", u2)
		assert.NoError(t, err)
		assert.Equal(t, "db.ranged", m.ns, "ns is authoritative when a chunk carries it")
	})

	t.Run("ns lookup beats uuid fallback", func(t *testing.T) {
		// a chunk whose ns is unknown but whose uuid is known still resolves
		m, err := idx.resolve("db.nope", u1)
		assert.NoError(t, err)
		assert.Equal(t, "db.ranged", m.ns)
	})
}

func TestResolve_Failures(t *testing.T) {
	idx := mustIndex(t,
		CollectionDoc{Ns: "db.ranged", UUID: uuidOf(0x01), Key: bson.D{{Key: "a", Value: int32(1)}}},
	)

	t.Run("unknown uuid", func(t *testing.T) {
		_, err := idx.resolve("", uuidOf(0x99))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "uuid")
	})

	t.Run("chunk carries neither ns nor uuid", func(t *testing.T) {
		_, err := idx.resolve("", primitive.Binary{})
		assert.Error(t, err)
	})

	t.Run("unknown ns and no uuid", func(t *testing.T) {
		_, err := idx.resolve("db.ghost", primitive.Binary{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "db.ghost")
	})
}

func TestBuildCollectionIndex_Errors(t *testing.T) {
	t.Run("unsupported shard key value type names the namespace", func(t *testing.T) {
		_, err := buildCollectionIndex([]CollectionDoc{
			{Ns: "db.bad", UUID: uuidOf(0x03), Key: bson.D{{Key: "a", Value: true}}},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "db.bad")
	})

	t.Run("empty input yields an empty usable index", func(t *testing.T) {
		idx := mustIndex(t)
		_, err := idx.resolve("db.any", primitive.Binary{})
		assert.Error(t, err)
	})
}

// TestBuildCollectionIndex_SkipsUnusableEntries pins that an entry which
// cannot describe a shard key is dropped from the index instead of failing the
// whole sync. Failing here would be stricter than the previous behaviour (which
// only errored once a chunk actually referenced the namespace), and keeping it
// would leave the chunk min/max validation indexing an empty key list.
func TestBuildCollectionIndex_SkipsUnusableEntries(t *testing.T) {
	noKey := uuidOf(0x06)
	good := uuidOf(0x07)

	idx, err := buildCollectionIndex([]CollectionDoc{
		{Ns: "db.nokey", UUID: noKey},
		{Ns: "db.good", UUID: good, Key: bson.D{{Key: "a", Value: int32(1)}}},
	})
	assert.NoError(t, err)

	t.Run("collection without a shard key is not resolvable", func(t *testing.T) {
		_, err := idx.resolve("db.nokey", noKey)
		assert.Error(t, err)
	})

	t.Run("healthy entry in the same batch still resolves", func(t *testing.T) {
		meta, err := idx.resolve("", good)
		assert.NoError(t, err)
		assert.Equal(t, "db.good", meta.ns)
	})
}

// TestBuildCollectionIndex_KeepsDroppedEntry records a deliberate choice: a
// config.collections entry carrying dropped:true is still indexed as long as it
// has a shard key. Filtering it out would be stricter than the previous
// behaviour, and on a cluster whose only chunk belongs to a collection that was
// just dropped it would trip the "nothing resolved" guard in buildChunkMap and
// abort a sync that used to work. A stale namespace in the chunk map is inert —
// full sync enumerates live collections, so nothing reads it.
func TestBuildCollectionIndex_KeepsDroppedEntry(t *testing.T) {
	u := uuidOf(0x0f)
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "db.gone"},
		{Key: "uuid", Value: u},
		{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}}},
		{Key: "dropped", Value: true},
	})
	assert.NoError(t, err)

	var doc CollectionDoc
	assert.NoError(t, bson.Unmarshal(raw, &doc))

	idx, err := buildCollectionIndex([]CollectionDoc{doc})
	assert.NoError(t, err)

	meta, err := idx.resolve("", u)
	assert.NoError(t, err, "a dropped entry with a usable shard key must stay resolvable")
	assert.Equal(t, "db.gone", meta.ns)
}

// TestBuildChunkMap_SkippedCollectionDoesNotPanic closes the loop on the skip
// above: a chunk belonging to a skipped collection must be dropped with a
// warning, not blow up the min/max validation against an empty key list.
func TestBuildChunkMap_SkippedCollectionDoesNotPanic(t *testing.T) {
	good, skipped := uuidOf(0x08), uuidOf(0x09)
	idx, err := buildCollectionIndex([]CollectionDoc{
		{Ns: "db.good", UUID: good, Key: bson.D{{Key: "k", Value: int32(1)}}},
		{Ns: "db.nokey", UUID: skipped},
	})
	assert.NoError(t, err)

	minRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}})
	maxRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}})
	got, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
		{UUID: good, Shard: "shard1", Min: minRaw, Max: maxRaw},
		{UUID: skipped, Shard: "shard2", Min: minRaw, Max: maxRaw},
	})
	assert.NoError(t, err)
	assert.Contains(t, got["rs1"], "db.good")
	assert.NotContains(t, got["rs1"], "db.nokey")
	assert.Len(t, got["rs1"]["db.good"].Chunks, 1)
}

// ---------------------------------------------------------------------------
// buildChunkMap
// ---------------------------------------------------------------------------

// TestBuildChunkMap_UUIDOnlyChunks is the #1017 regression. On MongoDB 5.0+
// every config.chunks document carries uuid and no ns, so the old code
// resolved every chunk to the empty namespace and GetColShardType("") hit
// FindOne(config.collections, {_id:""}) -> ErrNoDocuments, aborting full sync
// with "fetch chunk map failed[mongo: no documents in result]".
func TestBuildChunkMap_UUIDOnlyChunks(t *testing.T) {
	u := uuidOf(0x0a)
	idx := mustIndex(t, CollectionDoc{Ns: "db.ranged", UUID: u, Key: bson.D{{Key: "k", Value: int32(1)}}})

	chunks := []ChunkDoc{
		{
			UUID:  u,
			Shard: "shard1",
			Min:   rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}}),
			Max:   rawOf(t, bson.D{{Key: "k", Value: int32(50)}}),
		},
		{
			UUID:  u,
			Shard: "shard2",
			Min:   rawOf(t, bson.D{{Key: "k", Value: int32(50)}}),
			Max:   rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}}),
		},
	}

	got, err := buildChunkMap(testShardMap(), idx, chunks)
	assert.NoError(t, err)

	// OrphanFilter needs the namespace present on every replset, including
	// shards that hold none of its chunks, so it can tell "sharded collection"
	// apart from "not sharded".
	assert.Contains(t, got["rs1"], "db.ranged")
	assert.Contains(t, got["rs2"], "db.ranged")
	assert.Len(t, got["rs1"]["db.ranged"].Chunks, 1)
	assert.Len(t, got["rs2"]["db.ranged"].Chunks, 1)
	assert.Equal(t, []string{"k"}, got["rs1"]["db.ranged"].Keys)
	assert.Equal(t, []string{RangedShard}, got["rs1"]["db.ranged"].ShardTypes)

	// boundary values must round-trip, not just the count
	assert.Equal(t, primitive.MinKey{}, got["rs1"]["db.ranged"].Chunks[0].Mins[0])
	assert.Equal(t, int32(50), got["rs1"]["db.ranged"].Chunks[0].Maxs[0])
	assert.Equal(t, int32(50), got["rs2"]["db.ranged"].Chunks[0].Mins[0])
	assert.Equal(t, primitive.MaxKey{}, got["rs2"]["db.ranged"].Chunks[0].Maxs[0])
}

// TestBuildChunkMap_NsAndUUIDShapesAgree is the strongest guard against the
// fix silently changing behaviour on old servers: the same logical chunk set
// expressed in pre-5.0 shape and in 5.0+ shape must produce an identical map.
func TestBuildChunkMap_NsAndUUIDShapesAgree(t *testing.T) {
	u := uuidOf(0x0b)
	idx := mustIndex(t, CollectionDoc{Ns: "db.ranged", UUID: u, Key: bson.D{{Key: "k", Value: int32(1)}}})

	minRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}})
	midRaw := rawOf(t, bson.D{{Key: "k", Value: int32(50)}})
	maxRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}})

	oldShape := []ChunkDoc{
		{Ns: "db.ranged", Shard: "shard1", Min: minRaw, Max: midRaw},
		{Ns: "db.ranged", Shard: "shard2", Min: midRaw, Max: maxRaw},
	}
	newShape := []ChunkDoc{
		{UUID: u, Shard: "shard1", Min: minRaw, Max: midRaw},
		{UUID: u, Shard: "shard2", Min: midRaw, Max: maxRaw},
	}

	gotOld, err := buildChunkMap(testShardMap(), idx, oldShape)
	assert.NoError(t, err)
	gotNew, err := buildChunkMap(testShardMap(), idx, newShape)
	assert.NoError(t, err)

	assert.Equal(t, gotOld, gotNew, "pre-5.0 and 5.0+ chunk shapes must yield the same chunk map")
	assert.Len(t, gotOld, 2)
}

// TestBuildChunkMap_CompoundHashedViaUUID makes sure per-column shard types
// survive the uuid path, not just the ns path. A collection treated as
// uniformly ranged would skip ComputeHash and mis-classify every doc.
func TestBuildChunkMap_CompoundHashedViaUUID(t *testing.T) {
	u := uuidOf(0x0c)
	idx := mustIndex(t, CollectionDoc{
		Ns:   "db.mixed",
		UUID: u,
		Key:  bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "hashed"}},
	})

	chunks := []ChunkDoc{{
		UUID:  u,
		Shard: "shard1",
		Min:   rawOf(t, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int64(-9223372036854775808)}}),
		Max:   rawOf(t, bson.D{{Key: "a", Value: int32(9)}, {Key: "b", Value: int64(0)}}),
	}}

	got, err := buildChunkMap(testShardMap(), idx, chunks)
	assert.NoError(t, err)
	sc := got["rs1"]["db.mixed"]
	assert.NotNil(t, sc)
	assert.Equal(t, []string{"a", "b"}, sc.Keys)
	assert.Equal(t, []string{RangedShard, HashedShard}, sc.ShardTypes)
	assert.Len(t, sc.Chunks, 1)
	assert.Len(t, sc.Chunks[0].Mins, 2)
}

func TestBuildChunkMap_Guards(t *testing.T) {
	u := uuidOf(0x0d)
	idx := mustIndex(t, CollectionDoc{Ns: "db.ranged", UUID: u, Key: bson.D{{Key: "k", Value: int32(1)}}})
	minRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}})
	maxRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}})

	t.Run("no chunks at all is fine", func(t *testing.T) {
		// A cluster with no sharded collections has an empty config.chunks.
		// That must not be reported as a failure.
		got, err := buildChunkMap(testShardMap(), idx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, got)
		assert.Len(t, got, 2, "one entry per replset from config.shards")
	})

	t.Run("chunk on an unknown shard errors instead of panicking", func(t *testing.T) {
		// shardMap["ghost"] is missing, so the old indexing would have hit a
		// nil *ShardCollection and dereferenced it.
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "ghost", Min: minRaw, Max: maxRaw},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ghost")
	})

	t.Run("min/max width mismatch errors instead of panicking", func(t *testing.T) {
		// collection is sharded on {k} but the chunk carries {k, extra}
		wide := rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}, {Key: "extra", Value: int32(1)}})
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "shard1", Min: wide, Max: maxRaw},
		})
		assert.Error(t, err)
	})

	t.Run("min/max key names must match the shard key", func(t *testing.T) {
		wrong := rawOf(t, bson.D{{Key: "other", Value: primitive.MinKey{}}})
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "shard1", Min: wrong, Max: maxRaw},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "keys")
	})

	t.Run("nil min or max errors instead of dereferencing nil", func(t *testing.T) {
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "shard1", Min: nil, Max: maxRaw},
		})
		assert.Error(t, err)
	})

	t.Run("corrupt min or max BSON errors", func(t *testing.T) {
		bad := bson.Raw([]byte{0x01, 0x02, 0x03})
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "shard1", Min: &bad, Max: maxRaw},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "illegal min/max")
	})

	t.Run("max key names must match the shard key", func(t *testing.T) {
		// min is fine, only max disagrees — exercises the second validation loop
		wrongMax := rawOf(t, bson.D{{Key: "other", Value: primitive.MaxKey{}}})
		_, err := buildChunkMap(testShardMap(), idx, []ChunkDoc{
			{UUID: u, Shard: "shard1", Min: minRaw, Max: wrongMax},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max keys")
	})
}

// TestBuildChunkMap_NoChunkResolved pins the loud-failure guard. If chunks
// exist but none can be resolved against config.collections, the result would
// be an empty chunk map and OrphanFilter would silently stop filtering — the
// exact failure class #978 was about. That has to be an error, not a warning.
func TestBuildChunkMap_NoChunkResolved(t *testing.T) {
	emptyIdx := mustIndex(t)
	chunks := []ChunkDoc{{
		UUID:  uuidOf(0xff),
		Shard: "shard1",
		Min:   rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}}),
		Max:   rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}}),
	}}

	_, err := buildChunkMap(testShardMap(), emptyIdx, chunks)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.collections")
}

// TestBuildChunkMap_SingleUnresolvableChunkIsSkipped keeps the guard from
// being too eager: one chunk that cannot be resolved (e.g. its collection was
// dropped between the two reads) must not abort the whole sync as long as
// other chunks did resolve.
func TestBuildChunkMap_SingleUnresolvableChunkIsSkipped(t *testing.T) {
	u := uuidOf(0x0e)
	idx := mustIndex(t, CollectionDoc{Ns: "db.ranged", UUID: u, Key: bson.D{{Key: "k", Value: int32(1)}}})
	minRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MinKey{}}})
	maxRaw := rawOf(t, bson.D{{Key: "k", Value: primitive.MaxKey{}}})

	chunks := []ChunkDoc{
		{UUID: u, Shard: "shard1", Min: minRaw, Max: maxRaw},
		{UUID: uuidOf(0xee), Shard: "shard2", Min: minRaw, Max: maxRaw}, // unknown uuid
	}

	got, err := buildChunkMap(testShardMap(), idx, chunks)
	assert.NoError(t, err)
	assert.Len(t, got["rs1"]["db.ranged"].Chunks, 1)
	assert.Empty(t, got["rs2"]["db.ranged"].Chunks, "rs2 holds no chunk of db.ranged")
}
