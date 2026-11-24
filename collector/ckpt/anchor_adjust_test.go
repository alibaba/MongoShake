package ckpt

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

func newAnchorTestStore(t *testing.T) (*MongoCheckpoint, *utils.MongoCommunityConn, string) {
	t.Helper()
	url := unit_test_common.TestUrl
	conn, err := utils.NewMongoCommunityConn(url, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	db := fmt.Sprintf("ut_anchor_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = conn.Client.Database(db).Drop(context.Background())
		conn.Close()
	})
	mc := &MongoCheckpoint{URL: url, DB: db, Table: "checkpoint"}
	mc.Name = "rs0"
	return mc, conn, db
}

func (mc *MongoCheckpoint) coll() *mongo.Collection {
	return mc.client.Client.Database(mc.DB).Collection(mc.Table)
}

func seedAnchor(t *testing.T, conn *utils.MongoCommunityConn, db string, doc bson.M) {
	t.Helper()
	_, err := conn.Client.Database(db).Collection("checkpoint").InsertOne(context.Background(), doc)
	require.NoError(t, err)
}

// T9: CAS must match every physical encoding decodeCheckpointTimestamp accepts.
func TestAdjustFromMatchesLegacyAndCurrentEncodings(t *testing.T) {
	packed := int64(1700000000) << 32
	cases := []struct {
		name string
		ckpt interface{}
	}{
		{"int64_current", packed},
		{"bson_timestamp_legacy", primitive.Timestamp{T: uint32(packed >> 32), I: uint32(packed)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mc, conn, db := newAnchorTestStore(t)
			seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": c.ckpt, "version": 2})
			require.True(t, mc.ensureNetwork())
			mirror, adjusted, err := mc.AdjustFrom(packed, bson.M{"ckpt": packed + 100, "version": 2})
			require.NoError(t, err)
			require.True(t, adjusted, "CAS must match the %s encoding", c.name)
			require.Equal(t, packed+100, mirror.Timestamp)
		})
	}
	// int32 form (small logical value)
	mc, conn, db := newAnchorTestStore(t)
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": int32(5), "version": 2})
	require.True(t, mc.ensureNetwork())
	mirror, adjusted, err := mc.AdjustFrom(5, bson.M{"ckpt": int64(9), "version": 2})
	require.NoError(t, err)
	require.True(t, adjusted)
	require.Equal(t, int64(9), mirror.Timestamp)
}

func newAnchorTestManager(t *testing.T, mc *MongoCheckpoint, db string) *CheckpointManager {
	t.Helper()
	old := conf.Options
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = mc.URL
	conf.Options.CheckpointStorageDb = db
	conf.Options.CheckpointStorageCollection = "checkpoint"
	t.Cleanup(func() { conf.Options = old })
	m := NewCheckpointManager("rs0", mc.Timestamp)
	require.NotNil(t, m)
	_, _, err := m.Get()
	require.NoError(t, err)
	return m
}

// T10: CAS preserves another writer's metadata, AND a subsequent normal full-field
// Update must not roll it back (confirmed ctx must be the post-storage mirror).
func TestAdjustAnchorPreservesMetadataAcrossNormalUpdate(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	T := int64(1700000000) << 32
	N1 := T + 100
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": T, "version": 2, "full_finish_ts": int64(0)})
	m := newAnchorTestManager(t, mc, db)
	// another writer advances metadata only, ckpt unchanged
	_, err := conn.Client.Database(db).Collection("checkpoint").UpdateOne(context.Background(),
		bson.M{"name": "rs0"}, bson.M{"$set": bson.M{"full_finish_ts": int64(777)}})
	require.NoError(t, err)

	res, err := m.AdjustAnchor(T, N1)
	require.NoError(t, err)
	require.Equal(t, AnchorAdjusted, res)
	require.Equal(t, int64(777), m.GetInMemory().FullFinishTs, "confirmed mirror must carry 777")

	// a normal incremental ACK update must keep 777
	require.NoError(t, m.Update(N1+1))
	var durable bson.M
	require.NoError(t, conn.Client.Database(db).Collection("checkpoint").FindOne(context.Background(),
		bson.M{"name": "rs0"}).Decode(&durable))
	require.Equal(t, int64(777), durable["full_finish_ts"], "normal Update rolled back metadata")
}

func TestAdjustAnchorConflictDoesNotCommit(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	T := int64(1700000000) << 32
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": T, "version": 2})
	m := newAnchorTestManager(t, mc, db)
	_, err := conn.Client.Database(db).Collection("checkpoint").UpdateOne(context.Background(),
		bson.M{"name": "rs0"}, bson.M{"$set": bson.M{"ckpt": T + 50}})
	require.NoError(t, err)
	res, err := m.AdjustAnchor(T, T+100)
	require.NoError(t, err)
	require.Equal(t, AnchorConflict, res)
	require.Equal(t, T, m.GetInMemory().Timestamp, "conflict must not commit the candidate")
}

// addition 1: a successor advancing the anchor between insert and readback must
// yield Conflict (converge), not a blind OK that streams from the requested value.
type advancingCreateStore struct {
	*MongoCheckpoint
	conn      *utils.MongoCommunityConn
	db        string
	advanceTo int64
}

func (s advancingCreateStore) CreateIfAbsent(doc bson.M) (*CheckpointContext, bool, error) {
	mirror, created, err := s.MongoCheckpoint.CreateIfAbsent(doc)
	if err != nil || !created {
		return mirror, created, err
	}
	// Advance via the SAME majority-confirmed collection the production writer
	// uses (mc.client is configured with majority write concern), and check the
	// write result, so the advanced anchor is in the majority read view before the
	// mirror read (default-write-concern + ignored error let 4.4 read the stale N1
	// mirror and falsely report Adjusted).
	res, werr := s.coll().UpdateOne(context.Background(),
		bson.M{"name": "rs0"}, bson.M{"$set": bson.M{"ckpt": s.advanceTo}})
	if werr != nil || res.MatchedCount != 1 {
		return nil, true, fmt.Errorf("advance write not confirmed: matched=%d err=%v", res.MatchedCount, werr)
	}
	advanced, rerr := s.MongoCheckpoint.readMirror()
	return advanced, true, rerr
}

func TestCreateReadbackAdvanceIsConflict(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	N1 := int64(1700000000) << 32
	N2 := N1 + 500
	require.True(t, mc.ensureNetwork())
	wrapper := advancingCreateStore{MongoCheckpoint: mc, conn: conn, db: db, advanceTo: N2}
	old := conf.Options
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = mc.URL
	conf.Options.CheckpointStorageDb = db
	conf.Options.CheckpointStorageCollection = "checkpoint"
	t.Cleanup(func() { conf.Options = old })
	m := NewCheckpointManager("rs0", InitCheckpoint)
	m.delegate = wrapper
	_, _, err := m.Get()
	require.NoError(t, err)
	res, err := m.AdjustAnchor(InitCheckpoint, N1)
	require.NoError(t, err)
	require.Equal(t, AnchorConflict, res, "readback advanced to N2 must not confirm N1")
}

// G1-G6: index guard coverage / comparison semantics / retryable self-heal.
func TestAnchorGuardIndexCoverage(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string)
		wantErr bool
	}{
		{"G3_correct_full_unique", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetName("name_1")})
		}, false},
		{"G3_hidden_unique_accepted", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetHidden(true).SetName("name_1")})
		}, false},
		{"G1_partial_unique_rejected", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"kind": "guarded"}).SetName("name_1")})
		}, true},
		{"G4_sparse_rejected", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetSparse(true).SetName("name_1")})
		}, true},
		{"G4_ttl_rejected", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetExpireAfterSeconds(3600).SetName("name_1")})
		}, true},
		{"G3_non_unique_rejected_then_created", func(t *testing.T, mc *MongoCheckpoint, conn *utils.MongoCommunityConn, db string) {
			createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetName("name_1")})
			// non-unique same-key index blocks creation (IndexOptionsConflict) -> guard fails
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mc, conn, db := newAnchorTestStore(t)
			require.True(t, mc.ensureNetwork())
			c.setup(t, mc, conn, db)
			err := mc.EnsureAnchorGuard()
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// G2: non-simple collection default collation fails closed even with no name index.
func TestAnchorGuardNonSimpleCollectionCollation(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	require.True(t, mc.ensureNetwork())
	err := conn.Client.Database(db).RunCommand(context.Background(), bson.D{
		{Key: "create", Value: "checkpoint"},
		{Key: "collation", Value: bson.M{"locale": "en", "strength": 2}},
	}).Err()
	require.NoError(t, err)
	require.Error(t, mc.EnsureAnchorGuard(), "non-simple default collation must fail closed before creating any index")
}

// G6: duplicate existing records block guard creation with diagnostics, no auto-cleanup.
func TestAnchorGuardDuplicateRecordsBlock(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": int64(1), "version": 2})
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": int64(2), "version": 2})
	require.True(t, mc.ensureNetwork())
	require.Error(t, mc.EnsureAnchorGuard())
	n, err := conn.Client.Database(db).Collection("checkpoint").CountDocuments(context.Background(), bson.M{"name": "rs0"})
	require.NoError(t, err)
	require.Equal(t, int64(2), n, "guard must not auto-delete duplicate records")
}

// G5: retryable guard — failure is not cached; after the operator fixes the index
// the same manager re-verifies and succeeds.
func TestAnchorGuardRetryableSelfHeal(t *testing.T) {
	mc, _, db := newAnchorTestStore(t)
	require.True(t, mc.ensureNetwork())
	createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"kind": "guarded"}).SetName("name_1")})
	m := newAnchorTestManager(t, mc, db)
	_, err := m.AdjustAnchor(InitCheckpoint, int64(1700000000)<<32)
	require.Error(t, err, "partial index must block the protected path")
	// operator resolves: drop the partial index, create the covering unique index
	_, err = mc.coll().Indexes().DropOne(context.Background(), "name_1")
	require.NoError(t, err)
	createIdx(t, mc, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true).SetName("name_1")})
	res, err := m.AdjustAnchor(InitCheckpoint, int64(1700000000)<<32)
	require.NoError(t, err, "guard must re-verify and self-heal after fix")
	require.Equal(t, AnchorAdjusted, res)
}

// P1 regression: the guard dereferences the delegate client that Get's error
// path closes/nils. AdjustAnchor must run the guard under ctxRecLock so concurrent
// Get/Update share one client lifecycle (no nil-deref panic). Run with -race.
func TestAdjustAnchorConcurrentWithGetNoPanic(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	T := int64(1700000000) << 32
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": T, "version": 2})
	m := newAnchorTestManager(t, mc, db)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_, _, _ = m.Get()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_, _ = m.AdjustAnchor(T, T+int64(j)+1)
			}
		}()
	}
	wg.Wait() // must complete without panic or race
}

func createIdx(t *testing.T, mc *MongoCheckpoint, model mongo.IndexModel) {
	t.Helper()
	_, err := mc.coll().Indexes().CreateOne(context.Background(), model)
	require.NoError(t, err)
}

// nilMirrorStore reports adjusted=true but a nil confirmation mirror (the HTTP
// write-then-read-failure shape). AdjustAnchor must NOT publish Adjusted.
type nilMirrorStore struct {
	recordingCheckpointOperation
}

func (s *nilMirrorStore) EnsureAnchorGuard() error { return nil }
func (s *nilMirrorStore) AdjustFrom(int64, bson.M) (*CheckpointContext, bool, error) {
	return nil, true, nil
}
func (s *nilMirrorStore) CreateIfAbsent(bson.M) (*CheckpointContext, bool, error) {
	return nil, true, nil
}

func TestAdjustAnchorNilMirrorIsNotAdjusted(t *testing.T) {
	base := &CheckpointContext{Name: "rs0", Timestamp: 100}
	store := &nilMirrorStore{recordingCheckpointOperation{getCtx: base, getExist: true}}
	m := &CheckpointManager{delegate: store, ctx: base}
	res, err := m.AdjustAnchor(100, 200)
	require.Error(t, err)
	require.Equal(t, AnchorReadFailed, res, "nil mirror must not be published as Adjusted")
	require.Equal(t, int64(100), m.GetInMemory().Timestamp, "nil mirror must not commit the candidate")
}

// Conflict / uncertain must preserve staged flags (no fake commit, no clearing).
func TestAdjustAnchorConflictPreservesStaged(t *testing.T) {
	mc, conn, db := newAnchorTestStore(t)
	T := int64(1700000000) << 32
	seedAnchor(t, conn, db, bson.M{"name": "rs0", "ckpt": T, "version": 2})
	m := newAnchorTestManager(t, mc, db)
	m.SetFullFinishTs(777)
	_, err := conn.Client.Database(db).Collection("checkpoint").UpdateOne(context.Background(),
		bson.M{"name": "rs0"}, bson.M{"$set": bson.M{"ckpt": T + 50}})
	require.NoError(t, err)
	res, err := m.AdjustAnchor(T, T+100)
	require.NoError(t, err)
	require.Equal(t, AnchorConflict, res)
	require.True(t, m.fullFinSet, "conflict must preserve the staged flag")
	require.NotNil(t, m.ctxRec)
	require.Equal(t, int64(777), m.ctxRec.FullFinishTs)
}
