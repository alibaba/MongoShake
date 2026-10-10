package docsyncer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

const (
	cursorResumeLiveDatabase   = "cursor_resume_live"
	cursorResumeLiveCollection = "documents"
)

func TestCursorResumeAfterKillCursors(t *testing.T) {
	oldConnectMode := conf.Options.MongoConnectMode
	oldFetchBatchSize := conf.Options.FullSyncReaderFetchBatchSize
	conf.Options.MongoConnectMode = utils.VarMongoConnectModePrimary
	conf.Options.FullSyncReaderFetchBatchSize = 10
	t.Cleanup(func() {
		conf.Options.MongoConnectMode = oldConnectMode
		conf.Options.FullSyncReaderFetchBatchSize = oldFetchBatchSize
	})

	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl,
		utils.VarMongoConnectModePrimary, false, utils.ReadWriteConcernDefault,
		utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("skip cursor-resume live test: MongoDB is unavailable at %s: %v", unit_test_common.TestUrl, err)
	}
	t.Cleanup(conn.Close)

	database := conn.Client.Database(cursorResumeLiveDatabase)
	collection := database.Collection(cursorResumeLiveCollection)
	require.NoError(t, database.Drop(nil))
	t.Cleanup(func() {
		_ = database.Drop(nil)
	})

	documents := make([]interface{}, 0, 1000)
	for id := int64(0); id < 1000; id++ {
		documents = append(documents, bson.D{
			{Key: "_id", Value: id},
			{Key: "sk", Value: fmt.Sprintf("k%06d", id)},
		})
	}
	_, err = collection.InsertMany(nil, documents)
	require.NoError(t, err)

	tests := []struct {
		name          string
		key           string
		start         interface{}
		end           interface{}
		killAfterRead []int
		firstID       int64
		expectedCount int
	}{
		{
			name:          "single reader",
			killAfterRead: []int{100},
			firstID:       0,
			expectedCount: 1000,
		},
		{
			name:          "shard key piece",
			key:           "sk",
			start:         "k000000",
			end:           "k000999",
			killAfterRead: []int{100},
			firstID:       1,
			expectedCount: 999,
		},
		{
			name:          "multiple cursor kills",
			killAfterRead: []int{100, 200, 300, 400, 500},
			firstID:       0,
			expectedCount: 1000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := NewDocumentReader(0, unit_test_common.TestUrl,
				utils.NS{Database: cursorResumeLiveDatabase, Collection: cursorResumeLiveCollection},
				test.key, test.start, test.end, "")
			ids := readDocumentsAfterCursorKills(t, reader, database, cursorResumeLiveCollection, test.killAfterRead)

			require.Len(t, ids, test.expectedCount)
			for index, id := range ids {
				require.Equal(t, test.firstID+int64(index), id)
			}
			require.Zero(t, reader.rebuild)
		})
	}
}

func readDocumentsAfterCursorKills(t *testing.T, reader *DocumentReader, database *mongo.Database,
	collection string, killAfterRead []int) []int64 {
	t.Helper()
	defer reader.Close()

	ids := make([]int64, 0)
	nextKill := 0
	for {
		doc, err := reader.NextDoc()
		require.NoError(t, err)
		if doc == nil {
			return ids
		}

		id, ok := doc.Lookup("_id").Int64OK()
		require.True(t, ok, "test documents must have int64 _id values")
		ids = append(ids, id)

		if nextKill >= len(killAfterRead) || len(ids) != killAfterRead[nextKill] {
			continue
		}
		require.NotNil(t, reader.docCursor)
		cursorID := reader.docCursor.ID()
		err = database.RunCommand(nil, bson.D{
			{Key: "killCursors", Value: collection},
			{Key: "cursors", Value: bson.A{cursorID}},
		}).Err()
		require.NoError(t, err)
		reader.releaseCursor()
		nextKill++
	}
}
