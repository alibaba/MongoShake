package collector

import (
	"testing"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	"github.com/alibaba/MongoShake/v2/oplog"
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
