package collector

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

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
	if err == nil {
		v, _ := doc["full_finish_ts"].(int64)
		assert.Equal(t, int64(0), v, "MaxInt64 must not be persisted")
	} // ErrNoDocuments is fine too: nothing was written at all

	realTs := int64(1794904896)<<32 | 7
	require.NoError(t, sync.BackfillFullFinishTs(realTs))
	require.NoError(t, conn.Client.Database("mongoshake").Collection("ckpt_fft_backfill_probe").
		FindOne(ctx, bson.M{"name": "fft_backfill_probe"}).Decode(&doc))
	assert.Equal(t, realTs, doc["full_finish_ts"], "a finite bound must be persisted")
}

// Faithful -race anchor for #1050 review B1: one goroutine runs Start()'s
// checkpoint initialization sequence (newCheckpointManager + Get) while the
// coordinator goroutine backfills. Before the readiness gate this pair was a
// data race; the channel close must establish happens-before.
func TestBackfillRacesWithStartInitialization(t *testing.T) {
	_ = utils.InitialLogger("", "", "error", true, 1)
	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	conf.Options.CheckpointStorageUrl = unit_test_common.TestUrl
	conf.Options.CheckpointStorageDb = "mongoshake"
	conf.Options.CheckpointStorageCollection = "ckpt_fft_race_probe"

	sync := &OplogSyncer{
		Replset:   "fft_race_probe",
		batcher:   &Batcher{},
		ckptReady: make(chan struct{}),
	}

	done := make(chan error, 1)
	go func() {
		// the exact sequence Start() runs before closing ckptReady
		sync.newCheckpointManager(sync.Replset, int64(1))
		_, _, _ = sync.ckptManager.Get()
		close(sync.ckptReady)
	}()
	go func() { done <- sync.BackfillFullFinishTs(int64(1794904896)<<32 | 7) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("backfill deadlocked against Start initialization")
	}
}
