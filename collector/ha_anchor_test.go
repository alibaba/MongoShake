package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/quorum"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// newHAAnchorSyncer builds a real OplogSyncer (production hook wiring) against an
// isolated checkpoint database. The first-read path exercised is EnsureNetwork,
// exactly what the fetcher loop calls.
func newHAAnchorSyncer(t *testing.T, seed int64, masterQuorum bool) (*OplogSyncer, *utils.MongoCommunityConn, string) {
	t.Helper()
	url := unit_test_common.TestUrl
	conn, err := utils.NewMongoCommunityConn(url, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	db := fmt.Sprintf("ut_ha_anchor_%d", time.Now().UnixNano())
	old := conf.Options
	conf.Options = conf.DefaultConfiguration()
	conf.Options.Id = db
	conf.Options.MasterQuorum = masterQuorum
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.MongoConnectMode = utils.VarMongoConnectModePrimary
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = url
	conf.Options.CheckpointStorageDb = db
	conf.Options.CheckpointStorageCollection = "checkpoint"
	conf.Options.IncrSyncReaderFetchBatchSize = 32
	conf.Options.IncrSyncFetcherBufferCapacity = 32
	conf.Options.IncrSyncShardKey = "collection"
	t.Cleanup(func() {
		conf.Options = old
		_ = conn.Client.Database(db).Drop(context.Background())
		conn.Close()
	})
	if seed > 0 {
		_, err := conn.Client.Database(db).Collection("checkpoint").InsertOne(context.Background(),
			bson.M{"name": "csrs986", "ckpt": seed, "version": 2, "fetch_method": "oplog"})
		require.NoError(t, err)
	}
	s := NewOplogSyncer("csrs986", seed, 0, url, nil, nil)
	require.NotNil(t, s)
	s.newCheckpointManager("csrs986", seed)
	_, _, err = s.ckptManager.Get()
	require.NoError(t, err)
	return s, conn, db
}

func durableCkpt(t *testing.T, conn *utils.MongoCommunityConn, db string) int64 {
	t.Helper()
	var doc bson.M
	err := conn.Client.Database(db).Collection("checkpoint").FindOne(context.Background(),
		bson.M{"name": "csrs986"}).Decode(&doc)
	if err != nil {
		return -1
	}
	switch v := doc["ckpt"].(type) {
	case int64:
		return v
	case primitive.Timestamp:
		return utils.TimeStampToInt64(v)
	}
	return -1
}

// T2: clean storage + future configured start is a legal first boot; the conditional
// creation persists the adjusted anchor and the reader streams (no self-lock).
func TestHAFirstBootNoRecordCreatesAnchor(t *testing.T) {
	quorum.AlwaysMaster()
	future := (time.Now().Unix() + 3600) << 32
	s, conn, db := newHAAnchorSyncer(t, 0, false) // no seed record
	r := s.reader.(*sourceReader.OplogReader)
	r.SetQueryTimestampOnEmpty(future)
	require.NoError(t, r.EnsureNetwork())
	require.True(t, r.HasOpenCursor())
	stored := durableCkpt(t, conn, db)
	require.Greater(t, stored, int64(0))
	require.Less(t, stored, future, "adjusted anchor must be persisted below the future start")
	require.Equal(t, stored, r.CurrentQueryTimestamp())
}

// T1: a successor moving the durable anchor makes the stale decision conflict; the
// reader converges to the successor value instead of overwriting it.
func TestHAConflictConvergesToSuccessor(t *testing.T) {
	quorum.AlwaysMaster()
	future := (time.Now().Unix() + 3600) << 32
	s, conn, db := newHAAnchorSyncer(t, future, false)
	r := s.reader.(*sourceReader.OplogReader)
	r.SetQueryTimestampOnEmpty(future)
	// successor advances durable to N1 while this reader still holds future T
	N1 := (time.Now().Unix() - 10) << 32
	_, err := conn.Client.Database(db).Collection("checkpoint").UpdateOne(context.Background(),
		bson.M{"name": "csrs986"}, bson.M{"$set": bson.M{"ckpt": N1}})
	require.NoError(t, err)
	// first read: query T > newest -> clamp decision expects T, CAS conflicts -> converge to N1
	_ = r.EnsureNetwork()
	require.Equal(t, N1, durableCkpt(t, conn, db), "conflict must not overwrite the successor anchor")
	require.Equal(t, N1, r.CurrentQueryTimestamp(), "reader must converge to the successor durable anchor")
}

// T5a: a demoted former leader with durable still at T must NOT issue a conditional
// write nor open the initial stream (#1052 HA P1; reviewer quorum-red schedule).
func TestHADemotedFormerLeaderDoesNotWrite(t *testing.T) {
	oldMaster := quorum.IsMaster()
	quorum.AlwaysMaster()
	t.Cleanup(func() {
		if oldMaster {
			quorum.AlwaysMaster()
		}
	})
	future := (time.Now().Unix() + 3600) << 32
	s, conn, db := newHAAnchorSyncer(t, future, true)
	r := s.reader.(*sourceReader.OplogReader)
	r.SetQueryTimestampOnEmpty(future)

	// inject a successor election record, then run the real quorum code to demote
	electionID := primitive.NewObjectID()
	quorum.UseElectionObjectId(electionID)
	_, err := conn.Client.Database(db).Collection("election").InsertOne(context.Background(), bson.M{
		"_id": electionID, "pid": int64(-1), "host": "review-successor", "heartbeat": time.Now().Unix() + 600,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- quorum.BecomeMasterWithContext(ctx, unit_test_common.TestUrl, db) }()
	deadline := time.Now().Add(20 * time.Second)
	for quorum.IsMaster() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.False(t, quorum.IsMaster(), "real quorum code must demote after observing the successor")
	cancel()
	<-done // join before restoring globals
	quorum.UseElectionObjectId(primitive.ObjectID{})

	// former leader's first read must not write nor open the stream
	_ = r.EnsureNetwork()
	require.Equal(t, future, durableCkpt(t, conn, db), "demoted leader must not write the shared checkpoint")
	require.False(t, r.HasOpenCursor(), "demoted leader must not open the initial stream")
}

// commitThenFailStore commits the conditional write then reports a lost response,
// injecting the "written but reply failed" fault at the storage boundary while
// the production hook and first-read path run unchanged.
type commitThenFailStore struct {
	*ckpt.MongoCheckpoint
	calls int
}

func (s *commitThenFailStore) AdjustFrom(expected int64, patch bson.M) (*ckpt.CheckpointContext, bool, error) {
	mirror, adjusted, err := s.MongoCheckpoint.AdjustFrom(expected, patch)
	s.calls++
	if err != nil {
		return mirror, adjusted, err
	}
	return mirror, adjusted, errors.New("injected: write committed but response lost")
}

func (s *commitThenFailStore) CreateIfAbsent(doc bson.M) (*ckpt.CheckpointContext, bool, error) {
	return s.MongoCheckpoint.CreateIfAbsent(doc)
}

func (s *commitThenFailStore) EnsureAnchorGuard() error {
	return s.MongoCheckpoint.EnsureAnchorGuard()
}

// T4: a committed-but-reply-failed adjustment must be recovered by the SAME
// running fetcher (self-retry) and deliver a real payload, not by a test thread
// calling EnsureNetwork again (#1052 review P2). Stopping the fetcher after the
// first error must fail this test (no payload).
type committedSignalStore struct {
	*commitThenFailStore
	committed chan int64
}

func (s *committedSignalStore) AdjustFrom(expected int64, patch bson.M) (*ckpt.CheckpointContext, bool, error) {
	mirror, adjusted, err := s.commitThenFailStore.AdjustFrom(expected, patch)
	if adjusted && mirror != nil {
		s.committed <- mirror.Timestamp
	}
	return mirror, adjusted, err
}

func TestHACommittedButResponseFailedConverges(t *testing.T) {
	quorum.AlwaysMaster()
	future := (time.Now().Unix() + 3600) << 32
	s, conn, db := newHAAnchorSyncer(t, future, false)
	mc := s.ckptManager.Delegate().(*ckpt.MongoCheckpoint)
	committed := make(chan int64, 1)
	store := &committedSignalStore{commitThenFailStore: &commitThenFailStore{MongoCheckpoint: mc}, committed: committed}
	s.ckptManager.SetDelegate(store)
	r := s.reader.(*sourceReader.OplogReader)
	r.SetQueryTimestampOnEmpty(future)

	stop, done := make(chan struct{}), make(chan struct{})
	sourceReader.SetFetcherLifecycle(stop, done)
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fetcher shutdown was not joined")
		}
		sourceReader.SetFetcherLifecycle(nil, nil)
	})
	r.StartFetcher()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("conditional write never committed")
	}
	marker := fmt.Sprintf("ha_payload_%d", time.Now().UnixNano())
	_, err := conn.Client.Database(db).Collection("marker").InsertOne(context.Background(), bson.M{"_id": marker})
	require.NoError(t, err)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := r.Next()
		if bytes.Contains(raw, []byte(marker)) {
			return // running fetcher recovered the committed anchor and delivered payload
		}
	}
	t.Fatal("committed response loss stranded the running fetcher (no payload delivered)")
}
