package executor

import (
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// Exercise the immutable-shard-key fallback added by develop after the original
// lazy-parser branch, including its bulk-error and command-error branches.
func TestLazyImmutableShardKeyFallback(t *testing.T) {
	original := conf.Options
	defer func() { conf.Options = original }()
	conf.Options.IncrSyncExecutorImmutableShardKeyFallback = true
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, kind := range []string{"bulk", "single"} {
		for _, upsert := range []bool{false, true} {
			for _, commandError := range []bool{false, true} {
				mt.Run(kind, func(mt *mtest.T) {
					object := bson.D{{"_id", int32(1)}, {"shardKey", "new"}, {"nested", bson.D{{"value", "payload"}}}}
					record, raw := lazyRecord(mt, "i", object)
					if commandError {
						mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 66, Message: "immutable field altered"}))
					} else {
						mt.AddMockResponses(mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 66, Message: "immutable field altered"}))
					}
					mt.AddMockResponses(
						mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}),
						mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}),
					)
					conn := &utils.MongoCommunityConn{Client: mt.Client}
					var writer BasicWriter = &BulkWriter{conn: conn}
					if kind == "single" {
						writer = &SingleWriter{conn: conn}
					}
					require.NoError(mt, writer.doUpdateOnInsert("target", "coll", bson.E{}, []*OplogRecord{record}, upsert))
					events := mt.GetAllStartedEvents()
					require.Len(mt, events, 3, "fallback must delete and reinsert the raw record")
					require.Equal(mt, "update", events[0].CommandName)
					require.Equal(mt, "delete", events[1].CommandName)
					filter := events[1].Command.Lookup("deletes").Array().Index(0).Value().Document().Lookup("q").Document()
					require.Equal(mt, int32(1), filter.Lookup("_id").Int32())
					require.Equal(mt, "insert", events[2].CommandName)
					sent := events[2].Command.Lookup("documents").Array().Index(0).Value().Document()
					require.Equal(mt, []byte(bson.Raw(raw).Lookup("o").Document()), []byte(sent))
					require.Nil(mt, record.original.partialLog.Object)
				})
			}
		}
	}
}
