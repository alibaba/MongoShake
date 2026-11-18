package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// TestDDLReplayResolvesTargetByNamespace covers #1039.
//
// The DDL applyOps replay used to forward the source collection's uuid. applyOps
// resolves a command op by that uuid, so on a target whose collection was
// created independently - full sync creates ordinary collections implicitly,
// hence a target-generated uuid - the command is accepted and then skipped:
// drop / collMod / dropIndexes report success while changing nothing.
//
// The source uuid below matches no target collection on purpose. Replaying the
// same oplog entries without it must take effect, which is what each subtest
// asserts.
func TestDDLReplayResolvesTargetByNamespace(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)

	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.Equal(t, nil, err, "connect test mongodb")
	require.NotNil(t, conn)

	const (
		db   = "ddl_uuid_replay_test"
		coll = "c"
	)
	ctx := context.Background()

	// a v4 uuid that no target collection has: stands for the source collection
	sourceUUID := &primitive.Binary{Subtype: 4, Data: []byte{
		0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x43, 0x33,
		0x84, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
	}}

	makeLog := func(ts uint32, obj bson.D) *oplog.PartialLog {
		return &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Timestamp: primitive.Timestamp{T: ts, I: 1},
			Operation: "c",
			Namespace: db + ".$cmd",
			Object:    obj,
			UI:        sourceUUID,
		}}
	}

	setup := func(t *testing.T) {
		require.NoError(t, conn.Client.Database(db).Drop(ctx))
		_, err := conn.Client.Database(db).Collection(coll).InsertOne(ctx, bson.M{"a": 1})
		require.NoError(t, err)
		_, err = conn.Client.Database(db).Collection(coll).Indexes().CreateOne(ctx,
			mongo.IndexModel{Keys: bson.D{{Key: "x", Value: 1}}})
		require.NoError(t, err)
	}

	indexNames := func(t *testing.T) []string {
		cur, err := conn.Client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)
		var indexes []bson.M
		require.NoError(t, cur.All(ctx, &indexes))
		names := make([]string, 0, len(indexes))
		for _, idx := range indexes {
			name, _ := idx["name"].(string)
			names = append(names, name)
		}
		return names
	}

	collectionExists := func(t *testing.T, name string) bool {
		names, err := conn.Client.Database(db).ListCollectionNames(ctx, bson.M{"name": name})
		require.NoError(t, err)
		return len(names) > 0
	}

	collectionUUID := func(t *testing.T, name string) primitive.Binary {
		var res struct {
			Cursor struct {
				FirstBatch []struct {
					Info struct {
						UUID primitive.Binary `bson:"uuid"`
					} `bson:"info"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		require.NoError(t, conn.Client.Database(db).RunCommand(ctx, bson.D{
			{Key: "listCollections", Value: 1},
			{Key: "filter", Value: bson.M{"name": name}},
		}).Decode(&res))
		require.Len(t, res.Cursor.FirstBatch, 1)
		return res.Cursor.FirstBatch[0].Info.UUID
	}

	t.Run("dropIndexes", func(t *testing.T) {
		setup(t)
		log := makeLog(1, bson.D{
			{Key: "dropIndexes", Value: coll},
			{Key: "index", Value: "x_1"},
		})
		require.NoError(t, RunCommand(db, "dropIndexes", log, conn.Client))
		assert.NotContains(t, indexNames(t), "x_1", "dropped index must be gone on the target")
		assert.True(t, collectionExists(t, coll), "dropIndexes must not drop the collection")
	})

	t.Run("collMod", func(t *testing.T) {
		setup(t)
		log := makeLog(2, bson.D{
			{Key: "collMod", Value: coll},
			{Key: "validationLevel", Value: "moderate"},
		})
		require.NoError(t, RunCommand(db, "collMod", log, conn.Client))

		var res struct {
			Cursor struct {
				FirstBatch []struct {
					Options bson.M `bson:"options"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		require.NoError(t, conn.Client.Database(db).RunCommand(ctx, bson.D{
			{Key: "listCollections", Value: 1},
			{Key: "filter", Value: bson.M{"name": coll}},
		}).Decode(&res))
		require.Len(t, res.Cursor.FirstBatch, 1)
		assert.Equal(t, "moderate", res.Cursor.FirstBatch[0].Options["validationLevel"],
			"collMod must reach the target collection")
	})

	t.Run("drop", func(t *testing.T) {
		setup(t)
		log := makeLog(3, bson.D{{Key: "drop", Value: coll}})
		require.NoError(t, RunCommand(db, "drop", log, conn.Client))
		assert.False(t, collectionExists(t, coll), "dropped collection must be gone on the target")
	})

	t.Run("create", func(t *testing.T) {
		require.NoError(t, conn.Client.Database(db).Drop(ctx))
		log := makeLog(4, bson.D{
			{Key: "create", Value: "created_by_ddl"},
			{Key: "idIndex", Value: bson.D{
				{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "name", Value: "_id_"},
			}},
		})
		require.NoError(t, RunCommand(db, "create", log, conn.Client))
		assert.True(t, collectionExists(t, "created_by_ddl"), "create must reach the target")
	})

	// A create that carries a uuid naming nothing on the target used to rename
	// the existing collection to tmp<rand>.create and build an empty one in its
	// place. Replaying it without the uuid is a no-op, which is what a create
	// over an existing namespace has to be.
	t.Run("create over an existing collection", func(t *testing.T) {
		setup(t)
		before := collectionUUID(t, coll)
		require.NotEmpty(t, before)
		log := makeLog(5, bson.D{
			{Key: "create", Value: coll},
			{Key: "idIndex", Value: bson.D{
				{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "name", Value: "_id_"},
			}},
		})
		require.NoError(t, RunCommand(db, "create", log, conn.Client))

		assert.Equal(t, before, collectionUUID(t, coll), "existing collection must not be replaced")
		count, err := conn.Client.Database(db).Collection(coll).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "existing documents must stay in place")
		names, err := conn.Client.Database(db).ListCollectionNames(ctx, bson.M{})
		require.NoError(t, err)
		for _, name := range names {
			assert.False(t, strings.HasPrefix(name, "tmp") && strings.HasSuffix(name, ".create"),
				"create must not leave a renamed collection behind: %s", name)
		}
	})

	// Views and time-series collections create their system.views container
	// through the same create oplog. Forwarding a foreign uuid here renames the
	// target's system.views - dropping every view it held - and then rebuilds it
	// empty.
	t.Run("create system.views over an existing one", func(t *testing.T) {
		require.NoError(t, conn.Client.Database(db).Drop(ctx))
		_, err := conn.Client.Database(db).Collection(coll).InsertOne(ctx, bson.M{"a": 1})
		require.NoError(t, err)
		require.NoError(t, conn.Client.Database(db).RunCommand(ctx, bson.D{
			{Key: "create", Value: "v1"},
			{Key: "viewOn", Value: coll},
			{Key: "pipeline", Value: bson.A{}},
		}).Err())

		log := makeLog(6, bson.D{
			{Key: "create", Value: "system.views"},
			{Key: "idIndex", Value: bson.D{
				{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "name", Value: "_id_"},
			}},
		})
		require.NoError(t, RunCommand(db, "create", log, conn.Client))

		var view struct {
			ID string `bson:"_id"`
		}
		require.NoError(t, conn.Client.Database(db).Collection("system.views").
			FindOne(ctx, bson.M{"_id": db + ".v1"}).Decode(&view),
			"existing view must survive a replayed create system.views")
		assert.Equal(t, db+".v1", view.ID)

		require.NoError(t, conn.Client.Database(db).Drop(ctx))
	})
}
