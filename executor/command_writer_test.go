package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	unit_test_common "github.com/alibaba/MongoShake/v2/unit_test_common"
)

func cwConn(t *testing.T) *utils.MongoCommunityConn {
	t.Helper()
	if unit_test_common.TestUrl == "" {
		t.Skip("MONGOSHAKE_TEST_URL not set")
	}
	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func cwColl(t *testing.T, conn *utils.MongoCommunityConn, name string) string {
	t.Helper()
	db := "cmdwriter_it"
	require.NoError(t, conn.Client.Database(db).Collection(name).Drop(context.Background()))
	t.Cleanup(func() { _ = conn.Client.Database(db).Collection(name).Drop(context.Background()) })
	return db
}

func count(t *testing.T, conn *utils.MongoCommunityConn, db, coll string, q bson.M) int64 {
	t.Helper()
	n, err := conn.Client.Database(db).Collection(coll).CountDocuments(context.Background(), q)
	require.NoError(t, err)
	return n
}

func TestCommandWriterInsertAndIdDup(t *testing.T) {
	conn := cwConn(t)
	db := cwColl(t, conn, "ins")
	cw := &CommandWriter{conn: conn}
	// real MongoDB rejects an insert.g field (gid is internal to cascade), so
	// drive the commands with empty metadata.
	meta := bson.E{}

	recs := []*OplogRecord{mockOplogRecord(1, 10, 0), mockOplogRecord(2, 20, 0)}
	require.NoError(t, cw.doInsert(db, "ins", meta, recs, false))
	assert.Equal(t, int64(2), count(t, conn, db, "ins", bson.M{}))

	// re-insert same _id with the skip strategy: ordered bulk fails, then
	// retryInsertIndividually treats the _id dup as already applied
	saved := conf.Options.IncrSyncExecutorDupKeyStrategy
	conf.Options.IncrSyncExecutorDupKeyStrategy = utils.VarIncrSyncExecutorDupKeyStrategySkip
	t.Cleanup(func() { conf.Options.IncrSyncExecutorDupKeyStrategy = saved })
	require.NoError(t, cw.doInsert(db, "ins", meta, recs, false))
	assert.Equal(t, int64(2), count(t, conn, db, "ins", bson.M{}))
}

func TestCommandWriterUpdateOnInsert(t *testing.T) {
	conn := cwConn(t)
	db := cwColl(t, conn, "uoi")
	cw := &CommandWriter{conn: conn}
	meta := bson.E{}

	// upsert=true creates the missing doc
	require.NoError(t, cw.doUpdateOnInsert(db, "uoi", meta, []*OplogRecord{mockOplogRecord(7, 70, 0)}, true))
	assert.Equal(t, int64(1), count(t, conn, db, "uoi", bson.M{"_id": 7}))

	// upsert=false on a missing _id must not create it
	require.NoError(t, cw.doUpdateOnInsert(db, "uoi", meta, []*OplogRecord{mockOplogRecord(8, 80, 0)}, false))
	assert.Equal(t, int64(0), count(t, conn, db, "uoi", bson.M{"_id": 8}))
}

func TestCommandWriterUpdateAndDelete(t *testing.T) {
	conn := cwConn(t)
	db := cwColl(t, conn, "upd")
	cw := &CommandWriter{conn: conn}
	meta := bson.E{}

	require.NoError(t, cw.doInsert(db, "upd", meta, []*OplogRecord{mockOplogRecord(3, 30, 0)}, false))

	upd := &OplogRecord{original: &PartialLogWithCallback{partialLog: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation: "u",
			Object:    bson.D{primitive.E{Key: "$set", Value: bson.D{primitive.E{Key: "x", Value: 99}}}},
			Query:     bson.D{primitive.E{Key: "_id", Value: 3}},
		},
	}}}
	require.NoError(t, cw.doUpdate(db, "upd", meta, []*OplogRecord{upd}, false))
	var doc bson.M
	require.NoError(t, conn.Client.Database(db).Collection("upd").FindOne(context.Background(), bson.M{"_id": 3}).Decode(&doc))
	assert.Equal(t, int32(99), doc["x"])

	require.NoError(t, cw.doDelete(db, "upd", meta, []*OplogRecord{mockDeleteOplogRecord(3)}))
	assert.Equal(t, int64(0), count(t, conn, db, "upd", bson.M{"_id": 3}))
}

func TestCommandWriterDoCommandDDL(t *testing.T) {
	conn := cwConn(t)
	db := cwColl(t, conn, "ddl")
	cw := &CommandWriter{conn: conn}

	saved := conf.Options.FilterDDLEnable
	conf.Options.FilterDDLEnable = true
	t.Cleanup(func() { conf.Options.FilterDDLEnable = saved })

	// seed a doc so createIndexes has a collection to act on
	require.NoError(t, cw.doInsert(db, "ddl", bson.E{}, []*OplogRecord{mockOplogRecord(1, 1, 0)}, false))

	ddl := &OplogRecord{original: &PartialLogWithCallback{partialLog: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation: "c",
			Object: bson.D{
				primitive.E{Key: "createIndexes", Value: "ddl"},
				primitive.E{Key: "indexes", Value: bson.A{bson.D{
					primitive.E{Key: "key", Value: bson.D{primitive.E{Key: "x", Value: 1}}},
					primitive.E{Key: "name", Value: "x_1"},
				}}},
			},
		},
	}}}
	require.NoError(t, cw.doCommand(db, bson.E{}, []*OplogRecord{ddl}))

	cur, err := conn.Client.Database(db).Collection("ddl").Indexes().List(context.Background())
	require.NoError(t, err)
	var names []string
	for cur.Next(context.Background()) {
		var idx bson.M
		require.NoError(t, cur.Decode(&idx))
		names = append(names, idx["name"].(string))
	}
	assert.Contains(t, names, "x_1")
}
