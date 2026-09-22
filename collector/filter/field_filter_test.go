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
