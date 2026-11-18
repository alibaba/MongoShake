package docsyncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// TestDestListCollectionFilter covers #1041. StartDropDestCollection lists the
// target's collections to decide whether to warn about an existing namespace.
// It used to reuse the source-side filter, which carries a "type" key that Amazon
// DocumentDB rejects outright ("Field 'type' is currently not supported"), so full
// sync died before moving a single document whenever
// full_sync.collection_exist_drop = false.
func TestDestListCollectionFilter(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)

	origin := conf.Options
	defer func() { conf.Options = origin }()

	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err, "connect test mongodb")
	require.NotNil(t, conn)
	defer conn.Close()

	// The MongoDB target keeps the historical filter, so the two branches are
	// genuinely different rather than both returning an empty document.
	conf.Options.SpecialTargetDBFlag = ""
	mongoFilter := destListCollectionFilter(conn)
	require.Contains(t, mongoFilter, "type", "MongoDB target must keep the type filter")
	assert.Equal(t, bson.M{"$in": bson.A{"collection", "timeseries"}}, mongoFilter["type"])

	conf.Options.SpecialTargetDBFlag = utils.VarSpecialTargetDBFlagDocumentDB
	docdbFilter := destListCollectionFilter(conn)
	assert.NotContains(t, docdbFilter, "type", "DocumentDB rejects the type key in the filter")
	assert.Empty(t, docdbFilter)

	// Both filters must be accepted by a real server, and the unfiltered one must
	// still surface the collection the warning is about.
	const db = "docdb_list_filter_test"
	require.NoError(t, conn.Client.Database(db).Drop(context.Background()))
	_, err = conn.Client.Database(db).Collection("c").InsertOne(context.Background(), bson.M{"a": 1})
	require.NoError(t, err)
	defer conn.Client.Database(db).Drop(context.Background())

	names, err := conn.Client.Database(db).ListCollectionNames(context.Background(), docdbFilter)
	require.NoError(t, err)
	assert.Contains(t, names, "c")
}
