package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/oplog"
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

// change stream emits createIndexes on ns "db.$cmd": FieldFilter rebuilds the
// collection ns (db + the createIndexes value) to look up the whitelist.
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
