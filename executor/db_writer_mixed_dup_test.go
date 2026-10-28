package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

const mixedDupCollection = "mixed_dup_insert"

func mixedDupLevelRecord(id, level interface{}) *OplogRecord {
	return &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Operation: "i",
					Namespace: testDb + "." + mixedDupCollection,
					Object: bson.D{
						{Key: "_id", Value: id},
						{Key: "level", Value: level},
					},
				},
			},
		},
	}
}

// A bulk insert may fail with duplicate-key errors mixed with other write
// errors. mongo.IsDuplicateKeyError only reports that SOME write error is a
// duplicate, so the duplicate-key strategy must not acknowledge the batch:
// the rejected documents would be dropped while the checkpoint moves past
// them (#1030).
func TestBulkWriterMixedDupAndNonDupInsert(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	validator := bson.D{{Key: "level", Value: bson.D{{Key: "$gte", Value: 0}}}}
	setup := func(t *testing.T) {
		t.Helper()
		require.NoError(t, conn.Client.Database(testDb).Collection(mixedDupCollection).Drop(ctx))
		require.NoError(t, conn.Client.Database(testDb).CreateCollection(ctx, mixedDupCollection,
			options.CreateCollection().SetValidator(validator)))
		_, err := conn.Client.Database(testDb).Collection(mixedDupCollection).InsertOne(ctx, bson.D{
			{Key: "_id", Value: 1}, {Key: "level", Value: 1}})
		require.NoError(t, err)
	}

	defer func() {
		conf.Options.IncrSyncExecutorDupKeyStrategy = ""
		conf.Options.IncrSyncExecutorUpsert = false
		conf.Options.IncrSyncExecutorInsertOnDupUpdate = false
		conf.Options.IncrSyncConflictWriteTo = NoDumpConflict
	}()
	conf.Options.IncrSyncConflictWriteTo = NoDumpConflict

	cases := []struct {
		name       string
		strategy   string
		insertOnUp bool
		upsert     bool
		oplogs     []*OplogRecord
		wantErr    bool
	}{
		{
			name:     "mixed batch, ignore strategy",
			strategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore,
			oplogs:   []*OplogRecord{mixedDupLevelRecord(1, 1), mixedDupLevelRecord(2, -1)},
			wantErr:  true,
		},
		{
			name:     "mixed batch, default strategy",
			strategy: "",
			oplogs:   []*OplogRecord{mixedDupLevelRecord(1, 1), mixedDupLevelRecord(2, -1)},
			wantErr:  true,
		},
		{
			name:       "mixed batch, insert_on_dup_update without upsert",
			insertOnUp: true,
			oplogs:     []*OplogRecord{mixedDupLevelRecord(1, 1), mixedDupLevelRecord(2, -1)},
			wantErr:    true,
		},
		{
			name:       "mixed batch, insert_on_dup_update with upsert",
			insertOnUp: true,
			upsert:     true,
			oplogs:     []*OplogRecord{mixedDupLevelRecord(1, 1), mixedDupLevelRecord(2, -1)},
			wantErr:    true,
		},
		{
			name:     "mixed batch, skip strategy",
			strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip,
			oplogs:   []*OplogRecord{mixedDupLevelRecord(1, 1), mixedDupLevelRecord(2, -1)},
			wantErr:  true,
		},
		{
			name:     "pure duplicate batch, ignore strategy",
			strategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore,
			oplogs:   []*OplogRecord{mixedDupLevelRecord(1, 1)},
			wantErr:  false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setup(t)

			conf.Options.IncrSyncExecutorDupKeyStrategy = c.strategy
			conf.Options.IncrSyncExecutorInsertOnDupUpdate = c.insertOnUp
			conf.Options.IncrSyncExecutorUpsert = c.upsert

			writer := NewDbWriter(conn, bson.E{}, true, -1)
			insertErr := writer.doInsert(testDb, mixedDupCollection, bson.E{}, c.oplogs, c.insertOnUp)

			count, err := conn.Client.Database(testDb).Collection(mixedDupCollection).
				CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			// the negative level document is rejected by the target validator in
			// every case; a correct writer must never claim otherwise
			require.EqualValues(t, 1, count, "target must keep only the pre-existing doc")

			if c.wantErr {
				require.Error(t, insertErr,
					"mixed duplicate/non-duplicate batch must not be reported as applied")
			} else {
				require.NoError(t, insertErr)
			}
		})
	}
}
