package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// TestNativeDDLCommand covers the unwrapping used for DocumentDB targets (#1041).
// applyOps carries the oplog envelope, a direct command must not: "ui" resolves the
// command by a source uuid the target does not own, and "$db" is rejected outright.
func TestNativeDDLCommand(t *testing.T) {
	log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Operation: "c",
		Namespace: "db.$cmd",
		Object: bson.D{
			{Key: "create", Value: "c"},
			{Key: "idIndex", Value: bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
				{Key: "name", Value: "_id_"}}},
			{Key: "ui", Value: &primitive.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}},
			{Key: "$db", Value: "db"},
		},
	}}

	cmd := nativeDDLCommand(log)
	assert.Equal(t, bson.D{
		{Key: "create", Value: "c"},
		{Key: "idIndex", Value: bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "name", Value: "_id_"}}},
	}, cmd)
	assert.Equal(t, "create", cmd[0].Key, "the command name must stay first")
}

// TestDropIndexesByName guards the idempotency suppression. DocumentDB answers
// IndexNotFound (27) not only for an absent index but also for a mismatched key
// pattern or a name list it applied only part of - verified on documentdb-local:
// dropIndexes ["a_1","ghost_1"] returns 27 and a_1 survives. Swallowing those
// would leave indexes behind while logging success, so only the single-name form
// may be treated as already applied.
func TestDropIndexesByName(t *testing.T) {
	assert.True(t, dropIndexesByName(bson.D{{Key: "dropIndexes", Value: "c"}, {Key: "index", Value: "a_1"}}))
	assert.False(t, dropIndexesByName(bson.D{{Key: "dropIndexes", Value: "c"},
		{Key: "index", Value: bson.A{"a_1", "ghost_1"}}}), "a name list can be partially applied")
	assert.False(t, dropIndexesByName(bson.D{{Key: "dropIndexes", Value: "c"},
		{Key: "index", Value: bson.D{{Key: "a", Value: 1}}}}), "a key pattern may not match")
	assert.False(t, dropIndexesByName(bson.D{{Key: "dropIndexes", Value: "c"}}), "no index selector")
}

// TestNativeDDLCommandLegacyDropIndexNames covers the oplog spellings that are not
// real commands. applyOps understood deleteIndex / deleteIndexes / dropIndex, but
// DocumentDB rejects them with CommandNotFound (115), so they must be normalized.
func TestNativeDDLCommandLegacyDropIndexNames(t *testing.T) {
	for _, legacy := range []string{"deleteIndex", "deleteIndexes", "dropIndex"} {
		t.Run(legacy, func(t *testing.T) {
			assert.Equal(t, "dropIndexes", nativeDDLName(legacy))
			cmd := nativeDDLCommand(&oplog.PartialLog{ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Object:    bson.D{{Key: legacy, Value: "c"}, {Key: "index", Value: "x_1"}},
			}})
			assert.Equal(t, bson.D{{Key: "dropIndexes", Value: "c"}, {Key: "index", Value: "x_1"}}, cmd)
		})
	}
	for _, kept := range []string{"create", "collMod", "drop", "dropIndexes"} {
		assert.Equal(t, kept, nativeDDLName(kept), "real command names must pass through")
	}
}

// TestDocumentDBDDLReplayTakesEffect replays DDL through the native-command path
// against a real server. DocumentDB has no applyOps, so the translated command has
// to be equivalent, not merely accepted: each subtest asserts the effect on the
// target. Running it on MongoDB keeps the translation itself under test even where
// DocumentDB is unavailable.
func TestDocumentDBDDLReplayTakesEffect(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)

	origin := conf.Options
	defer func() { conf.Options = origin }()
	conf.Options.SpecialTargetDBFlag = utils.VarSpecialTargetDBFlagDocumentDB

	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err, "connect test mongodb")
	require.NotNil(t, conn)
	defer conn.Close()

	const db = "docdb_ddl_replay_test"
	ctx := context.Background()
	require.NoError(t, conn.Client.Database(db).Drop(ctx))
	defer conn.Client.Database(db).Drop(ctx)

	makeLog := func(obj bson.D) *oplog.PartialLog {
		return &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Timestamp: primitive.Timestamp{T: 1, I: 1},
			Operation: "c",
			Namespace: db + ".$cmd",
			Object:    obj,
			UI:        &primitive.Binary{Subtype: 4, Data: []byte("0123456789abcdef")},
		}}
	}
	collections := func(t *testing.T) []string {
		names, err := conn.Client.Database(db).ListCollectionNames(ctx, bson.M{})
		require.NoError(t, err)
		return names
	}
	indexNames := func(t *testing.T, coll string) []string {
		cur, err := conn.Client.Database(db).Collection(coll).Indexes().List(ctx)
		require.NoError(t, err)
		var indexes []bson.M
		require.NoError(t, cur.All(ctx, &indexes))
		names := make([]string, 0, len(indexes))
		for _, idx := range indexes {
			names = append(names, idx["name"].(string))
		}
		return names
	}

	t.Run("create", func(t *testing.T) {
		// the exact shape a 4.4 source emits, plus the envelope keys that must go
		require.NoError(t, RunCommand(db, "create", makeLog(bson.D{
			{Key: "create", Value: "c"},
			{Key: "idIndex", Value: bson.D{{Key: "v", Value: 2},
				{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "name", Value: "_id_"}}},
			{Key: "ui", Value: &primitive.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}},
		}), conn.Client))
		assert.Contains(t, collections(t), "c")
	})

	t.Run("createIndexes then dropIndexes", func(t *testing.T) {
		require.NoError(t, RunCommand(db, "createIndexes", makeLog(bson.D{
			{Key: "createIndexes", Value: "c"},
			{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "x", Value: 1}}},
				{Key: "name", Value: "x_1"}}}},
		}), conn.Client))
		require.Contains(t, indexNames(t, "c"), "x_1")

		require.NoError(t, RunCommand(db, "dropIndexes", makeLog(bson.D{
			{Key: "dropIndexes", Value: "c"},
			{Key: "index", Value: "x_1"},
		}), conn.Client))
		assert.NotContains(t, indexNames(t, "c"), "x_1")

		// Replaying the same drop must stay a no-op. applyOps tolerated an already
		// dropped index; the native command answers IndexNotFound (27), which
		// IgnoreError does not swallow for op "c" - left unhandled it retries
		// forever and freezes the checkpoint, reproducing #1041 after a restart.
		require.NoError(t, RunCommand(db, "dropIndexes", makeLog(bson.D{
			{Key: "dropIndexes", Value: "c"},
			{Key: "index", Value: "x_1"},
		}), conn.Client), "replayed dropIndexes must be idempotent")

		// the legacy oplog spelling must reach the same outcome. dropIndex
		// (singular) is rejected natively by MongoDB too, so this stays sensitive
		// to the normalization even when the target is not DocumentDB.
		require.NoError(t, RunCommand(db, "dropIndex", makeLog(bson.D{
			{Key: "dropIndex", Value: "c"},
			{Key: "index", Value: "x_1"},
		}), conn.Client), "legacy dropIndex must be normalized and idempotent")
	})

	t.Run("name list is not swallowed", func(t *testing.T) {
		_, err := conn.Client.Database(db).Collection("c").Indexes().CreateOne(ctx,
			mongo.IndexModel{Keys: bson.D{{Key: "z", Value: 1}},
				Options: options.Index().SetName("z_1")})
		require.NoError(t, err)

		// A name list is applied only partially: the server reports IndexNotFound
		// for the missing entry while z_1 survives (verified on 7.0 and on
		// DocumentDB). Swallowing this would leave the index behind and report
		// success, so the error must reach the caller.
		err = RunCommand(db, "dropIndexes", makeLog(bson.D{
			{Key: "dropIndexes", Value: "c"},
			{Key: "index", Value: bson.A{"z_1", "ghost_1"}},
		}), conn.Client)
		require.Error(t, err, "a partially applied name list must not be treated as applied")
		assert.Contains(t, indexNames(t, "c"), "z_1", "the surviving index proves the drop did not complete")
	})

	t.Run("collMod", func(t *testing.T) {
		_, err := conn.Client.Database(db).Collection("c").Indexes().CreateOne(ctx,
			mongo.IndexModel{Keys: bson.D{{Key: "y", Value: 1}},
				Options: options.Index().SetName("y_1")})
		require.NoError(t, err)
		require.NoError(t, RunCommand(db, "collMod", makeLog(bson.D{
			{Key: "collMod", Value: "c"},
			{Key: "index", Value: bson.D{{Key: "name", Value: "y_1"}, {Key: "hidden", Value: true}}},
		}), conn.Client))

		cur, err := conn.Client.Database(db).Collection("c").Indexes().List(ctx)
		require.NoError(t, err)
		var indexes []bson.M
		require.NoError(t, cur.All(ctx, &indexes))
		var found bool
		for _, idx := range indexes {
			if idx["name"] == "y_1" {
				found = true
				assert.Equal(t, true, idx["hidden"], "collMod must reach the target index")
			}
		}
		require.True(t, found, "y_1 must exist, or the hidden assertion verifies nothing")
	})

	t.Run("drop", func(t *testing.T) {
		require.NoError(t, RunCommand(db, "drop", makeLog(bson.D{
			{Key: "drop", Value: "c"},
		}), conn.Client))
		assert.NotContains(t, collections(t), "c")
	})
}
