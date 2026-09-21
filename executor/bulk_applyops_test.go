package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

// This regression uses eager records and can be backported without lazy parsing.
func TestBulkUpdateAllApplyOps(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("all records already replayed", func(mt *mtest.T) {
		writer := &BulkWriter{conn: &utils.MongoCommunityConn{Client: mt.Client}}
		records := make([]*OplogRecord, 2)
		for i := range records {
			records[i] = &OplogRecord{original: &PartialLogWithCallback{partialLog: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
				Operation: "u", Namespace: "target.system.buckets.coll",
				Object: bson.D{{"$v", int32(2)}, {"diff", bson.D{{"sdata", bson.D{{"b", primitive.Binary{Subtype: 7, Data: []byte{1, 2}}}}}}}},
				Query:  bson.D{{"_id", int32(i)}},
			}}}}
			mt.AddMockResponses(mtest.CreateSuccessResponse())
		}
		require.NoError(mt, writer.doUpdate("target", "system.buckets.coll", bson.E{}, records, false))
		events := mt.GetAllStartedEvents()
		require.Len(mt, events, 2)
		for _, event := range events {
			require.Equal(mt, "applyOps", event.CommandName)
		}
	})
}
