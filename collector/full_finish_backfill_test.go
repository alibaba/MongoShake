package collector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// The parallel backfill runs on the coordinator goroutine while Start()
// creates the checkpoint manager on its own goroutine. BackfillFullFinishTs
// must wait for the readiness signal instead of racing on ckptManager, and
// an unavailable manager after readiness must be a loud error, never a
// silent skip (#1050 review B1).
func TestBackfillWaitsForCheckpointReadiness(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	sync := &OplogSyncer{
		batcher:   &Batcher{},
		ckptReady: make(chan struct{}),
	}

	done := make(chan error, 1)
	go func() { done <- sync.BackfillFullFinishTs(123) }()

	select {
	case <-done:
		t.Fatal("backfill completed before the checkpoint readiness signal")
	case <-time.After(100 * time.Millisecond):
	}

	close(sync.ckptReady)
	select {
	case err := <-done:
		// ckptManager is nil here: after readiness that is a broken invariant
		// and must surface as an error, not a silent nil
		require.Error(t, err, "unavailable manager must not degrade silently")
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not complete after the readiness signal")
	}
}

// MaxInt64 ("always in window", serverless/mongos-only) must stay in memory:
// persisting it would keep the window permanently open across restarts and
// silently skip real conflicts on non-empty targets (#1050 review M1).
func TestBackfillMaxInt64NotPersisted(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = unit_test_common.TestUrl
	conf.Options.CheckpointStorageDb = "mongoshake"
	conf.Options.CheckpointStorageCollection = "ckpt_fft_backfill_probe"

	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()
	require.NoError(t, conn.Client.Database("mongoshake").Collection("ckpt_fft_backfill_probe").Drop(ctx))

	manager := ckpt.NewCheckpointManager("fft_backfill_probe", 1)
	_, _, err = manager.Get()
	require.NoError(t, err)

	ready := make(chan struct{})
	close(ready)
	sync := &OplogSyncer{batcher: &Batcher{}, ckptReady: ready, ckptManager: manager}

	require.NoError(t, sync.BackfillFullFinishTs(math.MaxInt64))
	var doc bson.M
	err = conn.Client.Database("mongoshake").Collection("ckpt_fft_backfill_probe").
		FindOne(ctx, bson.M{"name": "fft_backfill_probe"}).Decode(&doc)
	if err != nil {
		// nothing written at all is the expected outcome; any OTHER read
		// error must fail the test instead of masking a regression (#1050 R4)
		require.True(t, errors.Is(err, mongo.ErrNoDocuments), "unexpected read error: %v", err)
	} else {
		v, ok := doc["full_finish_ts"].(int64)
		require.True(t, ok, "full_finish_ts has unexpected BSON type: %T", doc["full_finish_ts"])
		assert.Equal(t, int64(0), v, "MaxInt64 must not be persisted")
	}

	realTs := int64(1794904896)<<32 | 7
	require.NoError(t, sync.BackfillFullFinishTs(realTs))
	require.NoError(t, conn.Client.Database("mongoshake").Collection("ckpt_fft_backfill_probe").
		FindOne(ctx, bson.M{"name": "fft_backfill_probe"}).Decode(&doc))
	assert.Equal(t, realTs, doc["full_finish_ts"], "a finite bound must be persisted")
}

// Faithful -race anchor for #1050 review B1: one goroutine runs initCheckpoint
// — the EXACT sequence Start() runs — while the coordinator goroutine
// backfills. Before the readiness gate this pair was a data race; the channel
// close must establish happens-before. Calling the real helper (not a
// hand-copied sequence) means moving the close earlier in initCheckpoint
// reddens this test under -race (#1050 review R1).
func TestBackfillRacesWithStartInitialization(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = unit_test_common.TestUrl
	conf.Options.CheckpointStorageDb = "mongoshake"
	conf.Options.CheckpointStorageCollection = "ckpt_fft_race_probe"
	conf.Options.FullSyncReaderOplogStoreDisk = false

	sync := &OplogSyncer{
		Replset:       "fft_race_probe",
		startPosition: int64(1),
		batcher:       &Batcher{},
		ckptReady:     make(chan struct{}),
	}
	sync.persister = NewPersister(sync.Replset, sync)

	done := make(chan error, 1)
	go func() { sync.initCheckpoint() }()
	go func() { done <- sync.BackfillFullFinishTs(int64(1794904896)<<32 | 7) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("backfill deadlocked against Start initialization")
	}
}

// The serialize-path gate in loadCheckpoint must ALSO refuse to persist
// MaxInt64 (#1050 review R2: the Backfill gate alone had coverage, this one
// had none). Uses an httptest API checkpoint — no real server needed.
func TestLoadCheckpointMaxInt64NotPersisted(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	var posts int64
	var lastPost map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// UseNumber: float64 would lose precision on packed timestamps
			dec := json.NewDecoder(r.Body)
			dec.UseNumber()
			_ = dec.Decode(&lastPost)
			atomic.AddInt64(&posts, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"name": "fft_lc_probe", "ckpt": 100, "version": 2, "fetch_method": "oplog",
		})
	}))
	defer srv.Close()

	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = srv.URL
	conf.Options.FullSyncReaderOplogStoreDisk = false

	newSyncer := func(pos int64) *OplogSyncer {
		s := &OplogSyncer{
			Replset:                "fft_lc_probe",
			startPosition:          int64(100),
			fullSyncFinishPosition: utils.Int64ToTimestamp(pos),
			batcher:                &Batcher{},
			ckptReady:              make(chan struct{}),
		}
		s.persister = NewPersister(s.Replset, s)
		s.ckptManager = ckpt.NewCheckpointManager(s.Replset, 100)
		return s
	}

	// Must go through Int64ToTimestamp: primitive.Timestamp{MaxUint32,
	// MaxUint32} converts to -1, not MaxInt64.
	require.NoError(t, newSyncer(math.MaxInt64).loadCheckpoint())
	assert.Equal(t, int64(0), atomic.LoadInt64(&posts),
		"MaxInt64 must not be persisted on the serialize path")

	realTs := int64(1794904896)<<32 | 7
	require.NoError(t, newSyncer(realTs).loadCheckpoint())
	assert.Equal(t, int64(1), atomic.LoadInt64(&posts),
		"a finite bound must be persisted")
	require.NotNil(t, lastPost["full_finish_ts"])
	assert.Equal(t, strconv.FormatInt(realTs, 10), lastPost["full_finish_ts"].(json.Number).String())
}

// A flush failure on the parallel backfill path must NOT abort the completed
// full sync (#1050 review F1): the workers already hold the window, and the
// staged value must ride along on the next successful checkpoint update.
func TestBackfillFlushFailureIsNotFatalAndSelfHeals(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	var postFail atomic.Bool
	var posts int64
	var lastPost map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if postFail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			dec := json.NewDecoder(r.Body)
			dec.UseNumber()
			_ = dec.Decode(&lastPost)
			atomic.AddInt64(&posts, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"name": "fft_flushfail_probe", "ckpt": 100, "version": 2, "fetch_method": "oplog",
		})
	}))
	defer srv.Close()

	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = srv.URL
	conf.Options.FullSyncReaderOplogStoreDisk = false

	sync := &OplogSyncer{
		Replset:       "fft_flushfail_probe",
		startPosition: int64(100),
		batcher:       &Batcher{},
		ckptReady:     make(chan struct{}),
	}
	sync.persister = NewPersister(sync.Replset, sync)
	sync.ckptManager = ckpt.NewCheckpointManager(sync.Replset, 100)
	close(sync.ckptReady)

	realTs := int64(1794904896)<<32 | 7
	postFail.Store(true)
	require.NoError(t, sync.BackfillFullFinishTs(realTs),
		"a flush failure must not abort the completed full sync")
	assert.Equal(t, int64(0), atomic.LoadInt64(&posts))

	// the staged value self-heals on the next successful checkpoint update
	postFail.Store(false)
	require.NoError(t, sync.ckptManager.Update(200))
	assert.Equal(t, int64(1), atomic.LoadInt64(&posts))
	require.NotNil(t, lastPost["full_finish_ts"])
	assert.Equal(t, strconv.FormatInt(realTs, 10), lastPost["full_finish_ts"].(json.Number).String())
}
