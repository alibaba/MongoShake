package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestBulkUpdateOverlapWindow(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, tc := range []struct {
		name      string
		finish    uint32
		applyOps  bool
		concern   bool
		wantRetry bool
	}{
		{name: "failed update at full sync end", finish: 2, wantRetry: true},
		{name: "failed update after full sync end", finish: 1},
		{name: "no full sync window"},
		{name: "write concern error", finish: 3, concern: true},
		{name: "model index after applyOps", finish: 2, applyOps: true, wantRetry: true},
	} {
		mt.Run(tc.name, func(mt *mtest.T) {
			collection := "coll"
			if tc.applyOps {
				collection = "system.buckets.coll"
			}
			record := func(id int32, increment uint32, update bson.D) *OplogRecord {
				return &OplogRecord{original: &PartialLogWithCallback{partialLog: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Operation: "u",
						Namespace: "target." + collection,
						Timestamp: primitive.Timestamp{T: 100, I: increment},
						Object:    update,
						Query:     bson.D{{Key: "_id", Value: id}},
					},
				}}}
			}
			set := bson.D{{Key: "$set", Value: bson.D{{Key: "status.4.state", Value: 1}}}}
			records := []*OplogRecord{record(1, 1, set), record(2, 2, set), record(3, 3, set)}
			if tc.applyOps {
				binary := primitive.Binary{Subtype: 7, Data: []byte{1, 2}}
				column := bson.D{{Key: "b", Value: binary}}
				diff := bson.D{{Key: "sdata", Value: column}}
				columnUpdate := bson.D{{Key: "$v", Value: int32(2)}, {Key: "diff", Value: diff}}
				records = append([]*OplogRecord{record(0, 1, columnUpdate)}, records...)
				mt.AddMockResponses(mtest.CreateSuccessResponse())
			}

			response := mtest.CreateWriteErrorsResponse(mtest.WriteError{
				Index: 1, Code: 28, Message: "Cannot create field 'state' in element [4: null]",
			})
			if tc.concern {
				response = append(response, bson.E{Key: "writeConcernError", Value: bson.D{
					{Key: "code", Value: 64}, {Key: "errmsg", Value: "durability not confirmed"},
				}})
			}
			mt.AddMockResponses(response)
			if tc.wantRetry {
				mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}))
			}

			writer := &BulkWriter{conn: &utils.MongoCommunityConn{Client: mt.Client}}
			if tc.finish != 0 {
				writer.fullFinishTs = utils.TimeStampToInt64(primitive.Timestamp{T: 100, I: tc.finish})
			}
			err := writer.doUpdate("target", collection, bson.E{}, records, true)
			events := mt.GetAllStartedEvents()
			if tc.applyOps {
				require.NotEmpty(mt, events)
				require.Equal(mt, "applyOps", events[0].CommandName)
				events = events[1:]
			}
			if !tc.wantRetry {
				var bulkErr mongo.BulkWriteException
				require.ErrorAs(mt, err, &bulkErr)
				require.True(mt, bulkErr.HasErrorCode(28))
				if tc.concern {
					require.NotNil(mt, bulkErr.WriteConcernError)
				}
				require.Len(mt, events, 1)
				return
			}
			require.NoError(mt, err)
			require.Len(mt, events, 2)
			require.Equal(mt, "update", events[1].CommandName)
			updates, err := events[1].Command.Lookup("updates").Array().Values()
			require.NoError(mt, err)
			require.Len(mt, updates, 1)
			require.Equal(mt, int32(3), updates[0].Document().Lookup("q", "_id").Int32())
			require.True(mt, updates[0].Document().Lookup("upsert").Boolean())
		})
	}
}
