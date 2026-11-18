package ckpt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// recordingCheckpointOperation captures the last inserted context so tests can
// assert which fields actually reach persistence.
type recordingCheckpointOperation struct {
	inserted *CheckpointContext
	getCtx   *CheckpointContext
	getExist bool
}

func (s *recordingCheckpointOperation) Get() (*CheckpointContext, bool) {
	return s.getCtx, s.getExist
}

func (s *recordingCheckpointOperation) Insert(ctx *CheckpointContext) error {
	copied := *ctx
	s.inserted = &copied
	return nil
}

func (s *recordingCheckpointOperation) String() string {
	return "recording"
}

// #1047: the full-sync finish timestamp must survive the BSON round trip,
// including through the custom UnmarshalBSON whose aux struct enumerates every
// field explicitly.
func TestCheckpointContextFullFinishTsRoundTrip(t *testing.T) {
	want := CheckpointContext{
		Name:                   "rs0",
		Timestamp:              int64(1794476555)<<32 | 11,
		Version:                2,
		FetchMethod:            "oplog",
		OplogDiskQueue:         "queue-name",
		OplogDiskQueueFinishTs: int64(1794476556)<<32 | 1,
		FullFinishTs:           int64(1794476560)<<32 | 3,
	}
	data, err := bson.Marshal(want)
	require.NoError(t, err)

	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, want, got)
}

// A checkpoint written by a release without #1047 has no full_finish_ts field.
// It must decode to the zero value (window optimization disabled), not error.
func TestCheckpointContextLegacyDocWithoutFullFinishTs(t *testing.T) {
	ts := primitive.Timestamp{T: 1794476555, I: 11}
	data, err := bson.Marshal(legacyCheckpointDoc(ts))
	require.NoError(t, err)

	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, int64(0), got.FullFinishTs)
	assert.Equal(t, int64(ts.T)<<32|int64(ts.I), got.Timestamp)
	assert.Equal(t, "queue-name", got.OplogDiskQueue)
}

// The staged value must be picked up by the next Update and then cleared,
// mirroring SetOplogDiskFinishTs/SetFetchMethod semantics.
func TestCheckpointManagerStagesFullFinishTs(t *testing.T) {
	delegate := &recordingCheckpointOperation{}
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:      "rs-stage",
			Timestamp: 100,
			Version:   2,
		},
		delegate: delegate,
	}

	manager.SetFullFinishTs(555)
	require.NoError(t, manager.Update(300))
	require.NotNil(t, delegate.inserted)
	assert.Equal(t, int64(555), delegate.inserted.FullFinishTs)
	assert.Equal(t, int64(300), delegate.inserted.Timestamp)

	// staging is consumed: a later Update without a new Set keeps the value
	// persisted in ctx but must not re-apply a stale staged value
	require.NoError(t, manager.Update(400))
	assert.Equal(t, int64(555), delegate.inserted.FullFinishTs)
}

// Without oplog traffic the checkpoint timestamp never advances, so a staged
// field would never be persisted. FlushStaged forces a write at the current
// timestamp (no advancement) so the window survives a restart.
func TestCheckpointManagerFlushStagedPersistsWithoutAdvancing(t *testing.T) {
	delegate := &recordingCheckpointOperation{
		getCtx:   &CheckpointContext{Name: "rs-flush", Timestamp: 100, Version: 2},
		getExist: true,
	}
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:      "rs-flush",
			Timestamp: 100,
			Version:   2,
		},
		delegate: delegate,
	}

	manager.SetFullFinishTs(777)
	require.NoError(t, manager.FlushStaged())
	require.NotNil(t, delegate.inserted)
	assert.Equal(t, int64(100), delegate.inserted.Timestamp, "must not advance the checkpoint")
	assert.Equal(t, int64(777), delegate.inserted.FullFinishTs)
}

// Update already copies the whole ctx; the new field must ride along untouched
// when nothing was staged.
func TestCheckpointManagerUpdatePreservesFullFinishTs(t *testing.T) {
	delegate := &recordingCheckpointOperation{}
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:         "rs-preserve",
			Timestamp:    100,
			Version:      2,
			FullFinishTs: 999,
		},
		delegate: delegate,
	}

	require.NoError(t, manager.Update(300))
	require.NotNil(t, delegate.inserted)
	assert.Equal(t, int64(999), delegate.inserted.FullFinishTs)
}

// Another writer (docsyncer.Checkpoint at the full-sync handover) may advance
// the durable document beyond this manager's in-memory view. FlushStaged must
// refresh from storage and never write the checkpoint backwards
// (#1050 review B2).
func TestCheckpointManagerFlushStagedNeverRegressesDurable(t *testing.T) {
	delegate := &recordingCheckpointOperation{
		getCtx: &CheckpointContext{
			Name:      "rs-regress",
			Timestamp: 500,
			Version:   2,
		},
		getExist: true,
	}
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:      "rs-regress",
			Timestamp: 100, // stale in-memory view
			Version:   2,
		},
		delegate: delegate,
	}

	manager.SetFullFinishTs(777)
	require.NoError(t, manager.FlushStaged())
	require.NotNil(t, delegate.inserted)
	assert.Equal(t, int64(500), delegate.inserted.Timestamp,
		"FlushStaged must adopt the newer durable timestamp, not rewrite the stale one")
	assert.Equal(t, int64(777), delegate.inserted.FullFinishTs)
	assert.Equal(t, int64(500), manager.GetInMemory().Timestamp,
		"the in-memory view must be refreshed as well")
}

// A failed refresh read (nil context — the shape both Mongo and HTTP
// delegates return on error) must surface as an error, never silently write
// the stale in-memory timestamp over a newer durable value (#1050 review R3).
func TestCheckpointManagerFlushStagedReadFailureIsLoud(t *testing.T) {
	delegate := &recordingCheckpointOperation{} // Get returns nil,false: read failure
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:      "rs-readfail",
			Timestamp: 100,
			Version:   2,
		},
		delegate: delegate,
	}

	manager.SetFullFinishTs(777)
	err := manager.FlushStaged()
	require.Error(t, err, "a failed refresh read must not degrade silently")
	assert.Nil(t, delegate.inserted, "nothing may be written when the refresh read failed")
}
