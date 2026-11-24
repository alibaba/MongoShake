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
	f := NewFieldFilter(wl, nil)

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
	f := NewFieldFilter(wl, nil)

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

func TestBuildExclusionProjection(t *testing.T) {
	proj := BuildExclusionProjection(fieldSet("big", "blob"))
	// exclusion projection: no _id entry (kept by default), sorted, value 0
	assert.Equal(t, bson.D{{Key: "big", Value: 0}, {Key: "blob", Value: 0}}, proj)
}

func TestExcludeDocument(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: 10},
		{Key: "big", Value: "huge"},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "hz"}}},
	}
	got := ExcludeDocument(doc, fieldSet("big"))
	assert.Equal(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: 10},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "hz"}}},
	}, got)
}

func TestFilterModifiersExclude(t *testing.T) {
	obj := bson.D{
		{Key: "$set", Value: bson.M{"a": 1, "big": 2, "profile.city": "hz"}},
		{Key: "$unset", Value: bson.M{"b": 1, "big2": 1}},
	}
	got, empty := FilterModifiersExclude(obj, fieldSet("big", "big2"))
	assert.False(t, empty)
	assert.Equal(t, bson.D{
		{Key: "$set", Value: bson.M{"a": 1, "profile.city": "hz"}},
		{Key: "$unset", Value: bson.M{"b": 1}},
	}, got)

	// everything blacklisted -> empty, drop
	_, empty2 := FilterModifiersExclude(obj, fieldSet("a", "big", "profile", "b", "big2"))
	assert.True(t, empty2)
}

func TestFilterModifiersExcludePush(t *testing.T) {
	pushVal := bson.M{"$each": bson.A{}, "$slice": 2}

	// pure non-blacklisted array truncation -> kept, not dropped
	obj := bson.D{{Key: "$push", Value: bson.M{"items": pushVal}}}
	got, empty := FilterModifiersExclude(obj, fieldSet("big"))
	assert.False(t, empty, "non-blacklisted $push must not be dropped")
	assert.Equal(t, obj, got)

	// blacklisted array truncation -> filtered out, empty
	obj2 := bson.D{{Key: "$push", Value: bson.M{"big": pushVal}}}
	_, empty2 := FilterModifiersExclude(obj2, fieldSet("big"))
	assert.True(t, empty2)

	// mixed: $set small + $push blacklisted -> keep $set, drop $push (no leak)
	obj3 := bson.D{
		{Key: "$set", Value: bson.M{"a": 1}},
		{Key: "$push", Value: bson.M{"big": pushVal}},
	}
	got3, empty3 := FilterModifiersExclude(obj3, fieldSet("big"))
	assert.False(t, empty3)
	assert.Equal(t, bson.D{{Key: "$set", Value: bson.M{"a": 1}}}, got3)
}

func TestFilterModifiersPush(t *testing.T) {
	pushVal := bson.M{"$each": bson.A{}, "$slice": 2}

	// whitelist dual: pure whitelisted array truncation -> kept
	obj := bson.D{{Key: "$push", Value: bson.M{"items": pushVal}}}
	got, empty := FilterModifiers(obj, fieldSet("items"))
	assert.False(t, empty)
	assert.Equal(t, obj, got)

	// non-whitelisted array truncation -> dropped
	obj2 := bson.D{{Key: "$push", Value: bson.M{"secret": pushVal}}}
	_, empty2 := FilterModifiers(obj2, fieldSet("items"))
	assert.True(t, empty2)
}

func TestIndexSpecAllowed(t *testing.T) {
	bl := fieldSet("big", "blob")
	// no blacklisted key field -> allowed
	assert.True(t, IndexSpecAllowed(bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
	}, bl))
	// _id only -> allowed
	assert.True(t, IndexSpecAllowed(bson.D{
		{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
	}, bl))
	// compound with a blacklisted field -> not allowed
	assert.False(t, IndexSpecAllowed(bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: 1}, {Key: "big", Value: 1}}},
	}, bl))
	// sub-path of blacklisted object field -> not allowed
	assert.False(t, IndexSpecAllowed(bson.D{
		{Key: "key", Value: bson.D{{Key: "big.sub", Value: 1}}},
	}, bl))
}

func TestFieldFilterBlacklist(t *testing.T) {
	bl := map[string]map[string]struct{}{"db1.c1": fieldSet("big", "blob")}
	f := NewFieldFilter(nil, bl)

	// ns not in blacklist -> untouched
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.other", Operation: "i",
			Object: bson.D{{Key: "_id", Value: 1}, {Key: "big", Value: "x"}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "big", Value: "x"}}, log.Object)
	}

	// insert -> exclude blacklisted, keep rest
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "i",
			Object: bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 10}, {Key: "big", Value: "huge"}},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 10}}, log.Object)
	}

	// update modifier -> keep non-blacklisted (incl sub-path), drop blacklisted
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "u",
			Query: bson.D{{Key: "_id", Value: 1}},
			Object: bson.D{
				{Key: "$set", Value: bson.M{"a": 1, "big": 2, "profile.city": "hz"}},
			},
		}}
		assert.False(t, f.Filter(log))
		assert.Equal(t, bson.D{{Key: "$set", Value: bson.M{"a": 1, "profile.city": "hz"}}}, log.Object)
	}

	// update touching only blacklisted fields (incl sub-path) -> dropped
	{
		log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Namespace: "db1.c1", Operation: "u",
			Query:  bson.D{{Key: "_id", Value: 1}},
			Object: bson.D{{Key: "$set", Value: bson.M{"big": 2, "blob.sub": 3}}},
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

func TestFieldFilterBlacklistCreateIndexes(t *testing.T) {
	bl := map[string]map[string]struct{}{"db1.c1": fieldSet("big")}
	f := NewFieldFilter(nil, bl)

	// one allowed + one with blacklisted key -> keep only allowed
	log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.$cmd", Operation: "c",
		Object: bson.D{
			{Key: "createIndexes", Value: "c1"},
			{Key: "indexes", Value: bson.A{
				bson.D{{Key: "key", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "name", Value: "a_1"}},
				bson.D{{Key: "key", Value: bson.D{{Key: "big", Value: 1}}}, {Key: "name", Value: "big_1"}},
			}},
		},
	}}
	assert.False(t, f.Filter(log))
	assert.Len(t, oplog.GetKey(log.Object, "indexes").(bson.A), 1)

	// only blacklisted-key indexes -> drop whole log
	log2 := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.$cmd", Operation: "c",
		Object: bson.D{
			{Key: "createIndexes", Value: "c1"},
			{Key: "indexes", Value: bson.A{
				bson.D{{Key: "key", Value: bson.D{{Key: "big", Value: 1}}}, {Key: "name", Value: "big_1"}},
			}},
		},
	}}
	assert.True(t, f.Filter(log2))
}

func TestFieldFilterMixedModes(t *testing.T) {
	wl := map[string]map[string]struct{}{"db1.wl": fieldSet("a")}
	bl := map[string]map[string]struct{}{"db1.bl": fieldSet("big")}
	f := NewFieldFilter(wl, bl)

	// whitelist ns: keep only whitelisted
	wlLog := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.wl", Operation: "i",
		Object: bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "b", Value: 2}},
	}}
	assert.False(t, f.Filter(wlLog))
	assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}}, wlLog.Object)

	// blacklist ns: keep all but blacklisted
	blLog := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Namespace: "db1.bl", Operation: "i",
		Object: bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "big", Value: 2}},
	}}
	assert.False(t, f.Filter(blLog))
	assert.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}}, blLog.Object)
}
