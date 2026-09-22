package docsyncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestDocumentReaderFieldProjection(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModeStandalone, true,
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
	conf.Options.MongoConnectMode = utils.VarMongoConnectModeStandalone
	conf.Options.FullSyncReaderFetchBatchSize = 16
	conf.Options.FullSyncFieldWhitelistMap = map[string]map[string]struct{}{
		"ff_full_test.c1": {"a": {}, "profile": {}},
	}

	reader := NewDocumentReader(0, testMongoAddress, ns, "", nil, nil, "")
	defer reader.Close()

	doc, err := reader.NextDoc()
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	var got bson.M
	assert.NoError(t, bson.Unmarshal(doc, &got))
	assert.Equal(t, int32(1), got["_id"])
	assert.Equal(t, int32(10), got["a"])
	_, hasSecret := got["secret"]
	assert.False(t, hasSecret, "secret should be projected out")
	profile, ok := got["profile"].(bson.M)
	assert.True(t, ok)
	assert.Equal(t, "hz", profile["city"]) // whole sub-tree retained
	assert.Equal(t, "1", profile["phone"])
}
