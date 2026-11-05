package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

const uniqueIndexTestDb = "unique_index_scan_test"

func uniqueScanTestCollection(t *testing.T, conn *MongoCommunityConn) *mongo.Collection {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, conn.Client.Database(uniqueIndexTestDb).Drop(ctx))
	t.Cleanup(func() { _ = conn.Client.Database(uniqueIndexTestDb).Drop(context.Background()) })
	return conn.Client.Database(uniqueIndexTestDb).Collection("c")
}

func containsNamespace(nsList []string, ns string) bool {
	for _, item := range nsList {
		if item == ns {
			return true
		}
	}
	return false
}

// TestListUniqueIndexNamespaces pins which collections force ordered replay:
// a unique index counts only when its leading key field is not _id.
func TestListUniqueIndexNamespaces(t *testing.T) {
	_ = InitialLogger("", "", "debug", true, 1)

	conn, err := NewMongoCommunityConn(unit_test_common.TestUrl, VarMongoConnectModePrimary, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	coll := uniqueScanTestCollection(t, conn)
	ctx := context.Background()

	_, err = coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "plain", Value: 1}}},
		{Keys: bson.D{{Key: "uniq", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	require.NoError(t, err)

	// an index whose leading key field is _id adds no constraint beyond the
	// primary key, so a collection carrying only that one must not be reported
	idLed := conn.Client.Database(uniqueIndexTestDb).Collection("id_led_only")
	_, err = idLed.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "_id", Value: 1}, {Key: "tail", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("id_led_unique")})
	require.NoError(t, err)
	_, err = idLed.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}})
	require.NoError(t, err)

	nsList, err := conn.ListUniqueIndexNamespaces(bson.M{"name": bson.M{"$in": bson.A{"c", "id_led_only"}}})
	require.NoError(t, err)

	ns := uniqueIndexTestDb + ".c"
	assert.True(t, containsNamespace(nsList, ns), "a unique index besides _id must be reported")
	assert.False(t, containsNamespace(nsList, uniqueIndexTestDb+".id_led_only"),
		"a unique index led by _id must not be reported")
	assert.Equal(t, []string{ns}, filterNamespaces(nsList, uniqueIndexTestDb),
		"one entry per namespace, no matter how many unique indexes it carries")
}

func filterNamespaces(nsList []string, database string) []string {
	matches := make([]string, 0, len(nsList))
	for _, ns := range nsList {
		if len(ns) > len(database) && ns[:len(database)+1] == database+"." {
			matches = append(matches, ns)
		}
	}
	return matches
}

// TestHasUniqueIndexBesidesIDIgnoresDroppedCollection: the scan races with
// application drops, and NamespaceNotFound must not degrade the whole run.
func TestHasUniqueIndexBesidesIDIgnoresDroppedCollection(t *testing.T) {
	_ = InitialLogger("", "", "debug", true, 1)

	conn, err := NewMongoCommunityConn(unit_test_common.TestUrl, VarMongoConnectModePrimary, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	coll := uniqueScanTestCollection(t, conn)
	_, err = coll.InsertOne(context.Background(), bson.D{{Key: "a", Value: 1}})
	require.NoError(t, err)

	unique, err := conn.hasUniqueIndexBesidesID(uniqueIndexTestDb, "c")
	require.NoError(t, err)
	assert.False(t, unique)

	require.NoError(t, coll.Drop(context.Background()))

	unique, err = conn.hasUniqueIndexBesidesID(uniqueIndexTestDb, "c")
	assert.NoError(t, err, "a collection dropped between listCollections and listIndexes is not an error")
	assert.False(t, unique)

	// an unfiltered listCollections hands views to the index scan, and
	// listIndexes answers 166 CommandNotSupportedOnView
	require.NoError(t, conn.Client.Database(uniqueIndexTestDb).RunCommand(context.Background(), bson.D{
		{Key: "create", Value: "v"},
		{Key: "viewOn", Value: "c"},
		{Key: "pipeline", Value: bson.A{}},
	}).Err())
	unique, err = conn.hasUniqueIndexBesidesID(uniqueIndexTestDb, "v")
	assert.NoError(t, err, "a view cannot carry a unique index and must not abort the scan")
	assert.False(t, unique)
}
