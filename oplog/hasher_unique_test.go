package oplog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func uniqueTestLog(op, ns string, object bson.D) *PartialLog {
	return &PartialLog{ParsedLog: ParsedLog{Operation: op, Namespace: ns, Object: object}}
}

func uniqueTestInsert(ns string, id interface{}, a interface{}) *PartialLog {
	return uniqueTestLog("i", ns, bson.D{
		{Key: "_id", Value: id},
		{Key: "a", Value: a},
	})
}

func createIndexesOplog(db, coll string, indexes bson.A) *PartialLog {
	return uniqueTestLog("c", db+".$cmd", bson.D{
		{Key: "createIndexes", Value: coll},
		{Key: "indexes", Value: indexes},
	})
}

func indexSpec(key bson.D, name string, unique bool) bson.D {
	spec := bson.D{{Key: "key", Value: key}, {Key: "name", Value: name}}
	if unique {
		spec = append(spec, bson.E{Key: "unique", Value: true})
	}
	return spec
}

// flatCreateIndexesOplog renders the shape MongoDB writes for an index built on
// an empty collection: the single index specification is inlined in o instead
// of being wrapped in an "indexes" array.
func flatCreateIndexesOplog(db, coll string, spec bson.D) *PartialLog {
	object := bson.D{{Key: "createIndexes", Value: coll}, {Key: "v", Value: 2}}
	return uniqueTestLog("c", db+".$cmd", append(object, spec...))
}

func collModIndexOplog(db, coll string, index bson.D) *PartialLog {
	return uniqueTestLog("c", db+".$cmd", bson.D{
		{Key: "collMod", Value: coll},
		{Key: "index", Value: index},
	})
}

// TestUniqueIndexAwareHasherRouting pins the two routing rules: a namespace
// that carries a non-_id unique index replays on a single worker (like
// TableHasher), everything else hashes by _id (like PrimaryKeyHasher).
func TestUniqueIndexAwareHasherRouting(t *testing.T) {
	const mod = 8
	hasher := NewUniqueIndexAwareHasher([]string{"db.ordered"})
	table := &TableHasher{}
	pk := &PrimaryKeyHasher{}

	orderedLogs := []*PartialLog{
		uniqueTestInsert("db.ordered", 1, 1),
		uniqueTestInsert("db.ordered", 2, 2),
		uniqueTestInsert("db.ordered", "str-id", 3),
	}
	for _, log := range orderedLogs {
		assert.Equal(t, table.DistributeOplogByMod(log, mod), hasher.DistributeOplogByMod(log, mod),
			"ordered namespace must follow the namespace hasher: %v", log.Object)
	}
	assert.Equal(t, hasher.DistributeOplogByMod(orderedLogs[0], mod),
		hasher.DistributeOplogByMod(orderedLogs[1], mod),
		"documents of an ordered namespace must never be spread over workers")

	for _, id := range []interface{}{1, 2, 3, 4, 5, 6, 7, 8} {
		log := uniqueTestInsert("db.parallel", id, id)
		assert.Equal(t, pk.DistributeOplogByMod(log, mod), hasher.DistributeOplogByMod(log, mod),
			"a namespace without a non-_id unique index keeps hashing by _id")
	}

	// a single worker is always worker 0, whatever the namespace
	assert.Equal(t, uint32(0), hasher.DistributeOplogByMod(orderedLogs[0], 1))
	assert.Equal(t, uint32(0), hasher.DistributeOplogByMod(uniqueTestInsert("db.parallel", 1, 1), 1))
}

func TestUniqueIndexAwareHasherMarkOrdered(t *testing.T) {
	hasher := NewUniqueIndexAwareHasher(nil)
	require.False(t, hasher.IsOrdered("db.c"))

	assert.True(t, hasher.MarkOrdered("db.c"), "first switch must report a change")
	assert.False(t, hasher.MarkOrdered("db.c"), "repeated switch must be a no-op")
	assert.True(t, hasher.IsOrdered("db.c"))
	assert.Equal(t, []string{"db.c"}, hasher.OrderedNamespaces())
}

// TestMarkOrderedByDDL covers the DDL shapes that can introduce a constraint
// the _id based dispatch cannot honour.
func TestMarkOrderedByDDL(t *testing.T) {
	t.Run("unique secondary index", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}}, "a_1", true),
		}))
		assert.Equal(t, []string{"db.c"}, switched)
		assert.True(t, hasher.IsOrdered("db.c"))
	})

	t.Run("compound unique index", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 1}}, "a_1_b_1", true),
		}))
		assert.Equal(t, []string{"db.c"}, switched)
	})

	t.Run("one of several indexes is unique", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "plain", Value: 1}}, "plain_1", false),
			indexSpec(bson.D{{Key: "uniq", Value: 1}}, "uniq_1", true),
		}))
		assert.Equal(t, []string{"db.c"}, switched)
	})

	t.Run("two phase index build", func(t *testing.T) {
		// a replica set records an index build as startIndexBuild plus
		// commitIndexBuild when the collection already holds documents; the
		// specifications ride on both entries
		for _, command := range []string{"startIndexBuild", "commitIndexBuild"} {
			hasher := NewUniqueIndexAwareHasher(nil)
			ddl := uniqueTestLog("c", "db.$cmd", bson.D{
				{Key: command, Value: "c"},
				{Key: "indexBuildUUID", Value: primitive.Binary{Subtype: 4, Data: []byte{1, 2, 3, 4}}},
				{Key: "indexes", Value: bson.A{
					bson.D{
						{Key: "v", Value: 2},
						{Key: "unique", Value: true},
						{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
						{Key: "name", Value: "a_1"},
					},
				}},
			})
			assert.Equal(t, []string{"db.c"}, hasher.MarkOrderedByDDL(ddl), command)
		}
	})

	t.Run("index built on an empty collection", func(t *testing.T) {
		// an empty collection inlines the single index specification in o, with
		// no "indexes" array at all
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(flatCreateIndexesOplog("db", "c", bson.D{
			{Key: "unique", Value: true},
			{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
			{Key: "name", Value: "a_1"},
		}))
		assert.Equal(t, []string{"db.c"}, switched)
	})

	t.Run("flat non unique index", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(flatCreateIndexesOplog("db", "c", bson.D{
			{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
			{Key: "name", Value: "a_1"},
		}))
		assert.Empty(t, switched)
	})

	t.Run("flat unique index led by _id", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(flatCreateIndexesOplog("db", "c", bson.D{
			{Key: "unique", Value: true},
			{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "name", Value: "_id_unique"},
		}))
		assert.Empty(t, switched)
	})

	t.Run("collMod converts an index to unique", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(collModIndexOplog("db", "c", bson.D{
			{Key: "name", Value: "a_1"},
			{Key: "unique", Value: true},
		}))
		assert.Equal(t, []string{"db.c"}, switched)
	})

	t.Run("collMod prepareUnique is not unique yet", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(collModIndexOplog("db", "c", bson.D{
			{Key: "name", Value: "a_1"},
			{Key: "prepareUnique", Value: true},
		}))
		assert.Empty(t, switched, "prepareUnique only stages the conversion, the index is not enforced yet")
	})

	t.Run("collMod without an index change", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(uniqueTestLog("c", "db.$cmd", bson.D{
			{Key: "collMod", Value: "c"},
			{Key: "validator", Value: bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: 0}}}}},
		}))
		assert.Empty(t, switched)
	})

	t.Run("collMod with unique false", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(collModIndexOplog("db", "c", bson.D{
			{Key: "name", Value: "a_1"},
			{Key: "unique", Value: false},
		}))
		assert.Empty(t, switched)
	})

	t.Run("non unique index", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}}, "a_1", false),
		}))
		assert.Empty(t, switched)
		assert.False(t, hasher.IsOrdered("db.c"))
	})

	t.Run("unique index led by _id adds no constraint", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}}, "_id_1_a_1", true),
		}))
		assert.Empty(t, switched)
		assert.False(t, hasher.IsOrdered("db.c"))
	})

	t.Run("unique index with _id not leading does switch", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}, {Key: "_id", Value: 1}}, "a_1__id_1", true),
		}))
		assert.Equal(t, []string{"db.c"}, switched)
	})

	t.Run("idempotent", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher([]string{"db.c"})
		switched := hasher.MarkOrderedByDDL(createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}}, "a_1", true),
		}))
		assert.Empty(t, switched, "a namespace that already replays in order is not reported again")
	})

	t.Run("create command does not switch", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(uniqueTestLog("c", "db.$cmd", bson.D{
			{Key: "create", Value: "c"},
			{Key: "idIndex", Value: bson.D{
				{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "name", Value: "_id_"},
			}},
		}))
		assert.Empty(t, switched)
	})

	t.Run("drop keeps the namespace ordered", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher([]string{"db.c"})
		switched := hasher.MarkOrderedByDDL(uniqueTestLog("c", "db.$cmd", bson.D{{Key: "drop", Value: "c"}}))
		assert.Empty(t, switched)
		assert.True(t, hasher.IsOrdered("db.c"),
			"the constraint is only recomputed at startup, so a dropped namespace stays ordered for the run")
	})

	t.Run("rename carries the constraint to the target", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher([]string{"db.from"})
		switched := hasher.MarkOrderedByDDL(uniqueTestLog("c", "admin.$cmd", bson.D{
			{Key: "renameCollection", Value: "db.from"},
			{Key: "to", Value: "db.to"},
		}))
		assert.Equal(t, []string{"db.to"}, switched)
		assert.True(t, hasher.IsOrdered("db.to"))
	})

	t.Run("rename of a parallel namespace does not switch", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		switched := hasher.MarkOrderedByDDL(uniqueTestLog("c", "admin.$cmd", bson.D{
			{Key: "renameCollection", Value: "db.from"},
			{Key: "to", Value: "db.to"},
		}))
		assert.Empty(t, switched)
	})

	t.Run("unique index in a multi index command on a raw object", func(t *testing.T) {
		hasher := NewUniqueIndexAwareHasher(nil)
		spec := createIndexesOplog("db", "c", bson.A{
			indexSpec(bson.D{{Key: "a", Value: 1}}, "a_1", true),
		})
		raw, err := bson.Marshal(spec.Object)
		require.NoError(t, err)
		spec.Object = nil
		spec.objectRaw = raw
		assert.Equal(t, []string{"db.c"}, hasher.MarkOrderedByDDL(spec),
			"lazy oplog parsing leaves o as raw BSON")
	})
}
