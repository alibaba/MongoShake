package executor

import (
	"context"
	"errors"
	"math"
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
)

// #1047: inside the full/incr overlap window (oplog ts <= fullFinishTs) an
// insert hitting an _id duplicate is expected — the full scan already carried
// the document (at a version >= the insert oplog). Acknowledge it without the
// whole-batch $set conversion or the per-document single-writer fallback.
//
// Writes are measured through ns-filtered oplog counts on the test replica set
// so concurrent traffic on other namespaces cannot pollute the deltas.

const overlapColl = "overlap_window_1047"

// window boundary far from wall clock: only the packed comparison matters
var overlapWindowTs = primitive.Timestamp{T: 2000000000, I: 0}

func overlapRecord(id interface{}, version int, ts primitive.Timestamp) *OplogRecord {
	return &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Operation: "i",
					Namespace: testDb + "." + overlapColl,
					Timestamp: ts,
					Object: bson.D{
						{Key: "_id", Value: id},
						{Key: "v", Value: version},
					},
				},
			},
		},
	}
}

func overlapInWindowTs() primitive.Timestamp {
	return primitive.Timestamp{T: overlapWindowTs.T - 1, I: 1}
}

func overlapOutWindowTs() primitive.Timestamp {
	return primitive.Timestamp{T: overlapWindowTs.T + 1, I: 1}
}

func overlapSetup(t *testing.T, conn *utils.MongoCommunityConn, seedVersion int) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))
	_, err := conn.Client.Database(testDb).Collection(overlapColl).InsertOne(ctx,
		bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: seedVersion}})
	require.NoError(t, err)
}

func overlapNewestTs(t *testing.T, conn *utils.MongoCommunityConn) primitive.Timestamp {
	t.Helper()
	var doc struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	err := conn.Client.Database("local").Collection("oplog.rs").FindOne(context.Background(),
		bson.M{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&doc)
	require.NoError(t, err)
	return doc.Ts
}

func overlapNsOpCount(t *testing.T, conn *utils.MongoCommunityConn, op string, since primitive.Timestamp) int64 {
	t.Helper()
	n, err := conn.Client.Database("local").Collection("oplog.rs").CountDocuments(context.Background(),
		bson.M{"ns": testDb + "." + overlapColl, "op": op, "ts": bson.M{"$gt": since}})
	require.NoError(t, err)
	return n
}

func overlapDocVersion(t *testing.T, conn *utils.MongoCommunityConn, id interface{}) int {
	t.Helper()
	var doc struct {
		V int `bson:"v"`
	}
	err := conn.Client.Database(testDb).Collection(overlapColl).FindOne(context.Background(),
		bson.M{"_id": id}).Decode(&doc)
	require.NoError(t, err)
	return doc.V
}

func withOverlapOptions(t *testing.T, dupUpdate bool, strategy string) {
	t.Helper()
	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.IncrSyncExecutorInsertOnDupUpdate = dupUpdate
	conf.Options.IncrSyncExecutorDupKeyStrategy = strategy
	conf.Options.IncrSyncConflictWriteTo = NoDumpConflict
	// the shortcut requires DDL replay (F4 guard); tests exercising it must
	// hold that premise, the DDL-filtered case has its own tests below
	conf.Options.FilterDDLEnable = true
}

// In-window _id dups must be acknowledged with ZERO follow-up writes, even
// with insert_on_dup_update=true (which previously converted the whole batch).
// The scanned newer version must not be regressed to the oplog's older one.
func TestBulkWriterOverlapWindowIgnoresIDDup(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{
		overlapRecord(1, 1, overlapInWindowTs()),
		overlapRecord(1, 1, overlapInWindowTs()),
	}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Equal(t, int64(0), overlapNsOpCount(t, conn, "u", since),
		"in-window _id dups must not trigger any update")
	assert.Equal(t, 9, overlapDocVersion(t, conn, 1),
		"the scanned newer version must not be regressed")
}

// Sentinel guard: fullFinishTs=0 (legacy checkpoint / parallel before backfill)
// disables the window, so the configured conversion still applies.
func TestBulkWriterOverlapWindowDisabledWhenZero(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: 0}
	records := []*OplogRecord{overlapRecord(1, 1, overlapInWindowTs())}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0),
		"without window info the conversion must still run")
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1))
}

// MaxInt64 means "no finite overlap bound" (serverless / mongos-only). The
// insert shortcut must stay OFF there: its convergence premise (empty or
// dedicated target) is not guaranteed for a plain mongos-only config, so
// inserts keep the pre-#1047 conversion path (#1050 review F2). IgnoreError's
// always-in-window u/ui semantics are unaffected by this gate.
func TestBulkWriterOverlapWindowMaxInt64DisablesShortcut(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: math.MaxInt64}
	records := []*OplogRecord{overlapRecord(1, 1, overlapOutWindowTs())}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0),
		"with no finite bound the conversion must still run")
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1),
		"conversion semantics apply, not the window shortcut")
}

// Out of window the existing conversion semantics stay untouched.
func TestBulkWriterOutOfWindowStillConverts(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{overlapRecord(1, 1, overlapOutWindowTs())}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0))
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1),
		"out-of-window dup keeps the convert-to-update semantics")
}

// The conversion must only rewrite the FAILED subset: documents that the
// unordered insert already applied must not be $set a second time. A no-op
// $set leaves no oplog entry, so updates are counted through the profiler,
// which records every update attempt including no-ops.
func TestBulkWriterDupUpdateConvertsOnlyFailedSubset(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()
	// registered AFTER conn.Close so LIFO runs the reset while the client is
	// still open; t.Cleanup would run after the defer and hit a closed client,
	// leaving profiling enabled on the shared database (#1050 review M4)
	defer func() {
		require.NoError(t, conn.Client.Database(testDb).RunCommand(context.Background(),
			bson.D{{Key: "profile", Value: 0}}).Err())
	}()

	overlapSetup(t, conn, 9)
	ctx := context.Background()
	// fresh profiler: drop any earlier entries so the count needs no time
	// filter (profile timestamps are millisecond-truncated and race with
	// time.Now() at sub-millisecond boundaries)
	require.NoError(t, conn.Client.Database(testDb).RunCommand(ctx,
		bson.D{{Key: "profile", Value: 0}}).Err())
	_ = conn.Client.Database(testDb).Collection("system.profile").Drop(ctx)
	require.NoError(t, conn.Client.Database(testDb).RunCommand(ctx,
		bson.D{{Key: "profile", Value: 2}}).Err())
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{
		overlapRecord(1, 1, overlapOutWindowTs()), // dup against seed v9
		overlapRecord(2, 2, overlapOutWindowTs()), // new
		overlapRecord(3, 3, overlapOutWindowTs()), // new
	}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	updates, err := conn.Client.Database(testDb).Collection("system.profile").CountDocuments(ctx,
		bson.M{"ns": testDb + "." + overlapColl, "op": "update"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), updates,
		"only the failed document may be converted, not the whole batch")
	assert.Equal(t, int64(2), overlapNsOpCount(t, conn, "i", since))
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1))
	assert.Equal(t, 2, overlapDocVersion(t, conn, 2))
	assert.Equal(t, 3, overlapDocVersion(t, conn, 3))
}

// A secondary unique-index duplicate is a real conflict even inside the
// window: it must stay on the configured strategy path, not be swallowed by
// the window shortcut. With strategy=error the batch must keep failing; a
// window branch that ignores all-dup batches regardless of index would
// wrongly acknowledge it.
func TestBulkWriterWindowSecondaryUniqueDupStaysOnStrategy(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, false, utils.VarIncrSyncExecutorDupKeyStrategyError)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))
	_, err = conn.Client.Database(testDb).Collection(overlapColl).InsertOne(ctx,
		bson.D{{Key: "_id", Value: 1}, {Key: "u", Value: "shared"}, {Key: "v", Value: 9}})
	require.NoError(t, err)
	_, err = conn.Client.Database(testDb).Collection(overlapColl).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.D{{Key: "u", Value: 1}}, Options: options.Index().SetUnique(true)})
	require.NoError(t, err)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	record := &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Operation: "i",
					Namespace: testDb + "." + overlapColl,
					Timestamp: overlapInWindowTs(),
					Object: bson.D{
						{Key: "_id", Value: 2},
						{Key: "u", Value: "shared"},
						{Key: "v", Value: 1},
					},
				},
			},
		},
	}
	err = bw.doInsert(testDb, overlapColl, bson.E{}, []*OplogRecord{record}, false)
	require.Error(t, err, "secondary-index dup must not be acknowledged by the window shortcut")

	var doc bson.M
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).FindOne(ctx,
		bson.M{"_id": 1}).Decode(&doc))
	assert.EqualValues(t, 9, doc["v"], "the conflicting document must remain untouched")
}

// Review B1 regression anchor: with filter.op_types=d the delete between
// insert(v1) and insert(v2) of the same _id is dropped, so both inserts land
// in one batch. The window shortcut must stay OFF — skipping the duplicate
// v2 would strand the target at v1 while the source holds v2. The conversion
// path (ordered $set of the failed subset) must run instead.
func TestBulkWriterWindowDisabledWhenDeletesFiltered(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)
	conf.Options.FilterOpTypes = []string{"d"}

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{
		overlapRecord(1, 1, overlapInWindowTs()),
		overlapRecord(1, 2, overlapInWindowTs()),
	}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Equal(t, 2, overlapDocVersion(t, conn, 1),
		"with deletes filtered the target must converge to the newest insert")
	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0),
		"the conversion must run when the window shortcut is unsafe")
}

func TestSingleWriterWindowDisabledWhenDeletesFiltered(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)
	conf.Options.FilterOpTypes = []string{"d"}

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))

	sw := &SingleWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{
		overlapRecord(1, 1, overlapInWindowTs()),
		overlapRecord(1, 2, overlapInWindowTs()),
	}
	require.NoError(t, sw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Equal(t, 2, overlapDocVersion(t, conn, 1))
}

// SingleWriter gets the same window shortcut, per document.
func TestSingleWriterOverlapWindowIgnoresIDDup(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	sw := &SingleWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{overlapRecord(1, 1, overlapInWindowTs())}
	require.NoError(t, sw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Equal(t, int64(0), overlapNsOpCount(t, conn, "u", since))
	assert.Equal(t, 9, overlapDocVersion(t, conn, 1))
}

// Port of the BulkWriter secondary-index guard test to SingleWriter: deleting
// the _id_ check from the single-writer window shortcut must turn this red
// (#1050 review M2 — the mutation previously left the whole package green).
func TestSingleWriterWindowSecondaryUniqueDupStaysOnStrategy(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, false, utils.VarIncrSyncExecutorDupKeyStrategyError)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))
	_, err = conn.Client.Database(testDb).Collection(overlapColl).InsertOne(ctx,
		bson.D{{Key: "_id", Value: 1}, {Key: "u", Value: "shared"}, {Key: "v", Value: 9}})
	require.NoError(t, err)
	_, err = conn.Client.Database(testDb).Collection(overlapColl).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.D{{Key: "u", Value: 1}}, Options: options.Index().SetUnique(true)})
	require.NoError(t, err)

	sw := &SingleWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	record := &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Operation: "i",
					Namespace: testDb + "." + overlapColl,
					Timestamp: overlapInWindowTs(),
					Object: bson.D{
						{Key: "_id", Value: 2},
						{Key: "u", Value: "shared"},
						{Key: "v", Value: 1},
					},
				},
			},
		},
	}
	err = sw.doInsert(testDb, overlapColl, bson.E{}, []*OplogRecord{record}, false)
	require.Error(t, err, "secondary-index dup must not be acknowledged by the window shortcut")

	var doc bson.M
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).FindOne(ctx,
		bson.M{"_id": 1}).Decode(&doc))
	assert.EqualValues(t, 9, doc["v"], "the conflicting document must remain untouched")
}

// With conflict_write_to=db the window shortcut must dump ONLY the actual
// duplicates, not the whole batch: the unordered bulk already inserted the
// rest, and dumping them pollutes the conflict collection and amplifies
// writes (#1050 review B3, regression against the skip path on develop).
func TestBulkWriterWindowConflictDumpOnlyFailedDocs(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, false, utils.VarIncrSyncExecutorDupKeyStrategySkip)
	conf.Options.IncrSyncConflictWriteTo = DumpConflictToDB

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	overlapSetup(t, conn, 9)
	conflictColl := conn.Client.Database(utils.APPConflictDatabase).Collection(overlapColl)
	require.NoError(t, conflictColl.Drop(ctx))
	// leave no residue on the shared conflict database (#1050 review R6)
	defer func() { _ = conflictColl.Drop(context.Background()) }()

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	// the dup sits at a NON-zero index so dumping the wrong documents with
	// the right count cannot pass (#1050 review R5)
	records := []*OplogRecord{
		overlapRecord(2, 2, overlapInWindowTs()),
		overlapRecord(3, 3, overlapInWindowTs()),
		overlapRecord(1, 1, overlapInWindowTs()), // the only dup, index 2
	}
	for id := 4; id <= 9; id++ {
		records = append(records, overlapRecord(id, id, overlapInWindowTs()))
	}
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, false))

	dumped, err := conflictColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), dumped, "only the failed document may enter the conflict collection")
	var conflictDoc bson.M
	require.NoError(t, conflictColl.FindOne(ctx, bson.M{}).Decode(&conflictDoc))
	assert.EqualValues(t, 1, conflictDoc["_id"], "the dumped document must be the actual duplicate")
	n, err := conn.Client.Database(testDb).Collection(overlapColl).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(9), n, "the 8 non-conflicting documents must be inserted")
}

// DocumentDB reports E11000 as "Index '<name>'" instead of "index: <name>";
// the parser must accept both, otherwise the window shortcut (and skip /
// delete_and_retry) never fire on DocumentDB targets (#1050 review M3).
func TestParseDupKeyIndexNameDocumentDBFormat(t *testing.T) {
	mongoFormat := errors.New("E11000 duplicate key error collection: db.coll index: _id_ dup key: { _id: 1 }")
	docdbFormat := errors.New("Duplicate key violation on the requested collection: Index '_id_'")
	docdbSecondary := errors.New("Duplicate key violation on the requested collection: Index 'u_1'")

	assert.Equal(t, "_id_", parseDupKeyIndexName(mongoFormat))
	assert.Equal(t, "_id_", parseDupKeyIndexName(docdbFormat))
	assert.Equal(t, "u_1", parseDupKeyIndexName(docdbSecondary))
	assert.Equal(t, "", parseDupKeyIndexName(nil))
	assert.Equal(t, "", parseDupKeyIndexName(errors.New("some other error")))
}

// failedInsertSubset must sort by model index (mongos can report write errors
// out of order) and fall back to the whole batch on out-of-range indices
// (#1050 review N4 — neither branch had a fixture).
func TestFailedInsertSubsetOrderingAndFallback(t *testing.T) {
	models := []*OplogRecord{
		overlapRecord(10, 1, overlapInWindowTs()),
		overlapRecord(11, 1, overlapInWindowTs()),
		overlapRecord(12, 1, overlapInWindowTs()),
		overlapRecord(13, 1, overlapInWindowTs()),
	}
	outOfOrder := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 3, Code: 11000}},
		{WriteError: mongo.WriteError{Index: 1, Code: 11000}},
	}}
	subset := failedInsertSubset(models, outOfOrder)
	require.Len(t, subset, 2)
	assert.Equal(t, models[1], subset[0], "subset must follow model order, not error order")
	assert.Equal(t, models[3], subset[1])

	outOfRange := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 7, Code: 11000}},
	}}
	assert.Equal(t, models, failedInsertSubset(models, outOfRange),
		"out-of-range index falls back to the whole batch")

	empty := mongo.BulkWriteException{}
	assert.Equal(t, models, failedInsertSubset(models, empty),
		"no write errors falls back to the whole batch")
}

// F4 symmetry anchor: filter.ddl_enable=false (factory default) drops
// drop/create inside the window, so the target can keep an OLD incarnation
// and the new incarnation's insert dups on _id. The shortcut must stay OFF
// and the conversion must run, exactly like the delete-filter guard.
func TestBulkWriterWindowDisabledWhenDDLFiltered(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)
	conf.Options.FilterDDLEnable = false

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9) // old incarnation carried by the scan
	since := overlapNewestTs(t, conn)

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{overlapRecord(1, 1, overlapInWindowTs())} // new incarnation
	require.NoError(t, bw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0),
		"with DDL filtered the conversion must run, not the shortcut")
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1),
		"the new incarnation's content must converge")
}

func TestSingleWriterWindowDisabledWhenDDLFiltered(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, true, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)
	conf.Options.FilterDDLEnable = false

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	overlapSetup(t, conn, 9)
	since := overlapNewestTs(t, conn)

	sw := &SingleWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	records := []*OplogRecord{overlapRecord(1, 1, overlapInWindowTs())}
	require.NoError(t, sw.doInsert(testDb, overlapColl, bson.E{}, records, true))

	assert.Greater(t, overlapNsOpCount(t, conn, "u", since), int64(0))
	assert.Equal(t, 1, overlapDocVersion(t, conn, 1))
}

// F6: the plumbing activates IgnoreError's u/ui window tolerance on the
// parallel/restart paths (previously dead code there because fullFinishTs=0).
// Pin the semantics: an in-window PathNotViable update is tolerated (the scan
// can only have carried a newer shape, making the old update redundant);
// out of window the same error must surface.
func TestBulkWriterWindowUpdateTolerance(t *testing.T) {
	_ = utils.InitialLogger("", "", "debug", true, 1)
	withOverlapOptions(t, false, utils.VarIncrSyncExecutorDupKeyStrategyIgnore)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, conn.Client.Database(testDb).Collection(overlapColl).Drop(ctx))
	_, err = conn.Client.Database(testDb).Collection(overlapColl).InsertOne(ctx,
		bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 5}})
	require.NoError(t, err)

	// $set on a.b against a scalar a is necessarily PathNotViable (code 28)
	updateRecord := func(ts primitive.Timestamp) *OplogRecord {
		return &OplogRecord{
			original: &PartialLogWithCallback{
				partialLog: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Operation: "u",
						Namespace: testDb + "." + overlapColl,
						Timestamp: ts,
						Object:    bson.D{{Key: "$set", Value: bson.D{{Key: "a.b", Value: 1}}}},
						Query:     bson.D{{Key: "_id", Value: 1}},
					},
				},
			},
		}
	}

	bw := &BulkWriter{conn: conn, fullFinishTs: utils.TimeStampToInt64(overlapWindowTs)}
	require.NoError(t, bw.doUpdate(testDb, overlapColl, bson.E{},
		[]*OplogRecord{updateRecord(overlapInWindowTs())}, false),
		"in-window PathNotViable must be tolerated")

	err = bw.doUpdate(testDb, overlapColl, bson.E{},
		[]*OplogRecord{updateRecord(overlapOutWindowTs())}, false)
	require.Error(t, err, "out-of-window PathNotViable must surface")
}
