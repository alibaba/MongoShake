package collector

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"
	"go.mongodb.org/mongo-driver/bson"
)

// #1051: a durable checkpoint ahead of the barrier target is not by itself proof
// the preceding DML is durable (e.g. a future/migrated checkpoint). The fast path
// must require worker-ack confirmation; otherwise it waits for real progress.
func TestBarrierLeadingCheckpointWaitsForWorkerAck(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{ack: 0, unack: 200, syncer: syncer}
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 500}, nil // ahead of target 100
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	// checkpoint 500 >= target 100 but worker ack 0 < 100: must NOT complete
	requireBarrierPending(t, startBarrierTestWait(t, syncer, 100))
}

// With worker ack confirming progress, the leading checkpoint fast path still
// completes (normal operation: checkpoint = persisted min ack, ack only advances).
func TestBarrierLeadingCheckpointCompletesWithWorkerAck(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{ack: 150, unack: 200, syncer: syncer}
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 500}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	requireBarrierResult(t, startBarrierTestWait(t, syncer, 100), true)
}

// batcher nil (no workers): the durable checkpoint remains the only evidence.
func TestBarrierLeadingCheckpointNoBatcherTrustsCheckpoint(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 500}, nil
	}
	requireBarrierResult(t, startBarrierTestWait(t, syncer, 100), true)
}

// #1051 M1 regression: filter/exit barriers (ackRequired=false) use a filtered
// newestTs force-written to the checkpoint via inputTs, never dispatched to
// workers, so ack can never reach it. The checkpoint is authoritative there and
// the barrier must still complete (the ack-confirmation hardening only applies to
// dispatched DML barriers).
func TestBarrierFilterCheckpointCompletesWithoutAck(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{ack: 0, unack: 200, syncer: syncer}
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 500}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	requireBarrierResult(t, startBarrierTestWaitAck(t, syncer, 100, false), true)
}

// #1051 P1: ack is timestamp-granular. A first batch A@100 acked (ack=unack=100)
// must not let a barrier pass while a second same-timestamp batch B@100 is still
// offered-but-not-transferred (pending>0). The barrier requires all offered
// batches applied, not just ack >= newestTs.
func TestBarrierSameTimestampPendingBatchBlocks(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{ack: 100, unack: 100, syncer: syncer}
	atomic.StoreInt64(&worker.pending, 1) // B@100 offered, not yet transferred
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 100}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	requireBarrierPending(t, startBarrierTestWait(t, syncer, 100))
}

// Once the same-timestamp batch is transferred (pending==0), the barrier completes.
func TestBarrierSameTimestampTransferredCompletes(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{ack: 100, unack: 100, syncer: syncer}
	atomic.StoreInt64(&worker.pending, 0) // all offered batches applied
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: 100}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	requireBarrierResult(t, startBarrierTestWait(t, syncer, 100), true)
}

// Offer increments pending; transfer completion decrements it (batch identity
// survives same-timestamp merging because the count is per offered batch).
func TestWorkerPendingTracksOfferAndTransfer(t *testing.T) {
	worker := &Worker{queue: make(chan []*oplog.GenericOplog, 4)}
	require.True(t, worker.Idle())
	batch := []*oplog.GenericOplog{{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Timestamp: primitive.Timestamp{T: 100, I: 1}}}}}
	worker.Offer(batch)
	require.False(t, worker.Idle())
	require.Equal(t, int64(1), atomic.LoadInt64(&worker.pending))
	// simulate transfer completion releasing the pending count
	atomic.AddInt64(&worker.pending, -1)
	require.True(t, worker.Idle())
}

// #1051 M1 wiring regression: exercise the REAL transfer path (not a manual
// pending decrement) so removing the transfer-side decrement is caught. Offer
// holds pending=1; transfer applies the batch via the tunnel and must release it.
func TestWorkerTransferReleasesPending(t *testing.T) {
	syncer := &OplogSyncer{Replset: "t", workerAckNotify: make(chan struct{}, 1), replMetric: &utils.ReplicationMetric{}}
	worker := &Worker{syncer: syncer, queue: make(chan []*oplog.GenericOplog, 4)}
	worker.writeController = &WriteController{worker: worker, tunnel: &tunnel.MockWriter{}}
	batch := []*oplog.GenericOplog{{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: primitive.Timestamp{T: 100, I: 1}, Operation: "i", Namespace: "test.documents",
		Object: bson.D{{Key: "_id", Value: 1}},
	}}}}
	worker.Offer(batch)
	require.False(t, worker.Idle(), "Offer must hold a pending count")
	worker.transfer(batch)
	require.True(t, worker.Idle(), "transfer must release the pending count via the real path")
}

// #1051 P1 regression: a worker with pending batches and zero ack must BLOCK the
// checkpoint, not be excluded (which let a dual-worker setup advance past it).
func TestCheckpointPendingZeroAckBlocks(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	w0 := &Worker{ack: 0, unack: 100, syncer: syncer}
	atomic.StoreInt64(&w0.pending, 1) // 100 still queued, no ack
	w1 := &Worker{ack: 150, unack: 200, syncer: syncer}
	atomic.StoreInt64(&w1.pending, 1)
	syncer.batcher = &Batcher{workerGroup: []*Worker{w0, w1}, txnPins: make(map[oplog.TxnID]*txnPin)}
	_, err := syncer.calculateWorkerLowestCheckpoint()
	require.Error(t, err, "zero-ack pending worker must block the checkpoint, not be excluded")
}

// Same-timestamp pending batch: contribution is ack-1 (strictly below the pending
// batch ts), protecting both the batch head and same-ts batches.
func TestCheckpointPendingSameTimestampConservative(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	ack := utils.TimeStampToInt64(primitive.Timestamp{T: 100, I: 5})
	w := &Worker{ack: ack, unack: ack, syncer: syncer}
	atomic.StoreInt64(&w.pending, 1) // same-ts batch still pending
	syncer.batcher = &Batcher{workerGroup: []*Worker{w}, txnPins: make(map[oplog.TxnID]*txnPin)}
	lowest, err := syncer.calculateWorkerLowestCheckpoint()
	require.NoError(t, err)
	require.Equal(t, ack-1, lowest, "checkpoint must stay strictly below the pending batch ts")
}
