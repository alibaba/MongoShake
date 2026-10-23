package collector

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func newBarrierTestSyncer(t *testing.T) *OplogSyncer {
	t.Helper()
	if l.Logger == nil {
		require.NoError(t, utils.InitialLogger("", "", "error", true, 2))
	}
	get, flush, interval, retry := barrierCkptGetFunc, barrierCkptFlushFunc, BarrierCheckpointInterval, BarrierCheckpointRetryInterval
	sentinel := utils.IncrSentinelOptions
	t.Cleanup(func() {
		barrierCkptGetFunc, barrierCkptFlushFunc, BarrierCheckpointInterval, BarrierCheckpointRetryInterval = get, flush, interval, retry
		utils.IncrSentinelOptions = sentinel
	})
	utils.IncrSentinelOptions.Pause = false
	utils.IncrSentinelOptions.Shutdown = false
	return &OplogSyncer{Replset: t.Name(), workerAckNotify: make(chan struct{}, 1)}
}

// All test control crosses goroutines through channels/atomics. In particular,
// only the waiting goroutine changes sentinel flags while a wait is running.
func startBarrierTestWait(t *testing.T, syncer *OplogSyncer, target int64) <-chan bool {
	t.Helper()
	get := barrierCkptGetFunc
	var cancel atomic.Bool
	barrierCkptGetFunc = func(syncer *OplogSyncer) (*ckpt.CheckpointContext, error) {
		if cancel.Load() {
			utils.IncrSentinelOptions.Shutdown = true
		}
		return get(syncer)
	}
	result := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- syncer.checkCheckpointUpdate(true, target)
	}()
	t.Cleanup(func() {
		cancel.Store(true)
		select {
		case syncer.workerAckNotify <- struct{}{}:
		default:
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("barrier waiter did not stop")
		}
	})
	return result
}

func requireBarrierResult(t *testing.T, result <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-result:
		require.Equal(t, want, got)
	case <-time.After(time.Second):
		t.Fatal("barrier did not react without its polling timer")
	}
}

func requireBarrierPending(t *testing.T, result <-chan bool) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("barrier returned %v before persistent checkpoint reached the target", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestBarrierCheckpointFlushesBeforeWaiting(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	// A successful flush must be read back immediately, regardless of the timer.
	BarrierCheckpointInterval = time.Hour
	var persisted int64
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: persisted}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool {
		persisted = 100
		return true
	}
	requireBarrierResult(t, startBarrierTestWait(t, syncer, 100), true)
}

func TestBarrierCheckpointFlushesReadyWorkersBeforeAckFallback(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	syncer.batcher = &Batcher{workerGroup: []*Worker{{ack: 100, unack: 100}}}
	persisted := int64(50)
	reads := 0
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		reads++
		return &ckpt.CheckpointContext{Timestamp: persisted}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool {
		persisted = 100
		return true
	}
	require.True(t, syncer.checkCheckpointUpdate(true, 100))
	require.Equal(t, int64(100), persisted, "persist ready ACKs before using the upstream pinned-checkpoint fallback")
	require.Equal(t, 2, reads, "read back a successful flush immediately")
}

func TestBarrierCheckpointFlushesAckThatAdvancesDuringFlush(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{syncer: syncer, ack: 50, unack: 100}
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}}
	persisted := int64(25)
	flushes := 0
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: persisted}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool {
		flushes++
		if flushes == 1 {
			worker.setAck(100)
			return false
		}
		persisted = worker.GetAck()
		return true
	}
	require.True(t, syncer.checkCheckpointUpdate(true, 100))
	require.Equal(t, 2, flushes)
	require.Equal(t, int64(100), persisted, "a final barrier must persist the ACK that arrived during its initial flush")
}

func TestBarrierCheckpointWakesOnWorkerACK(t *testing.T) {
	for _, timing := range []string{"before select", "after flush"} {
		t.Run(timing, func(t *testing.T) {
			syncer := newBarrierTestSyncer(t)
			BarrierCheckpointInterval = time.Hour
			worker := &Worker{syncer: syncer, unack: 100}
			var persisted int64
			flushed := make(chan struct{}, 1)
			barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
				return &ckpt.CheckpointContext{Timestamp: persisted}, nil
			}
			barrierCkptFlushFunc = func(*OplogSyncer) bool {
				if worker.GetAck() >= 100 {
					persisted = 100
					return true
				}
				if timing == "before select" {
					// Cover an ACK arriving between the state check and the wait.
					worker.setAck(100)
				}
				select {
				case flushed <- struct{}{}:
				default:
				}
				return false
			}
			result := startBarrierTestWait(t, syncer, 100)
			if timing == "after flush" {
				select {
				case <-flushed:
				case <-time.After(time.Second):
					t.Fatal("barrier did not attempt to flush")
				}
				requireBarrierPending(t, result)
				worker.setAck(100)
			}
			requireBarrierResult(t, result, true)
		})
	}
}

func TestBarrierCheckpointRequiresReadbackWithoutWorkerAckFallback(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	BarrierCheckpointInterval = time.Hour
	var visible atomic.Bool
	var reads atomic.Int32
	var flushes atomic.Int32
	readback := make(chan struct{}, 1)
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		reads.Add(1)
		if flushes.Load() > 0 {
			select {
			case readback <- struct{}{}:
			default:
			}
		}
		if visible.Load() {
			return &ckpt.CheckpointContext{Timestamp: 100}, nil
		}
		return &ckpt.CheckpointContext{Timestamp: 50}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool {
		flushes.Add(1)
		return true
	}
	result := startBarrierTestWait(t, syncer, 100)
	select {
	case <-readback:
	case <-time.After(time.Second):
		t.Fatal("successful flush was not read back")
	}
	requireBarrierPending(t, result)
	require.Equal(t, int32(1), flushes.Load(), "stale readback must not cause a busy read/write loop")
	visible.Store(true)
	syncer.workerAckNotify <- struct{}{}
	requireBarrierResult(t, result, true)
	require.GreaterOrEqual(t, reads.Load(), int32(3))
}

func TestBarrierCheckpointRetriesFailedPersistence(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	worker := &Worker{syncer: syncer, ack: 100, unack: 100}
	var allowWrite atomic.Bool
	var persisted int64
	attempted := make(chan struct{}, 1)
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: persisted}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool {
		if allowWrite.Load() {
			persisted = worker.GetAck()
			return true
		}
		select {
		case attempted <- struct{}{}:
		default:
		}
		return false
	}
	result := startBarrierTestWait(t, syncer, 100)
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write was not attempted")
	}
	requireBarrierPending(t, result)
	allowWrite.Store(true)
	requireBarrierResult(t, result, true)
}

func TestBarrierCheckpointReadErrorDoesNotRelease(t *testing.T) {
	syncer := newBarrierTestSyncer(t)
	reads := 0
	barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
		reads++
		if reads == 1 {
			return nil, errors.New("checkpoint unavailable")
		}
		utils.IncrSentinelOptions.Shutdown = true
		return &ckpt.CheckpointContext{Timestamp: 50}, nil
	}
	barrierCkptFlushFunc = func(*OplogSyncer) bool { return false }
	require.False(t, syncer.checkCheckpointUpdate(true, 100))
	require.Equal(t, 2, reads)
}

func TestBarrierCheckpointResumeReconfirmsProgress(t *testing.T) {
	for _, signal := range []string{"Pause", "Shutdown"} {
		t.Run(signal, func(t *testing.T) {
			syncer := newBarrierTestSyncer(t)
			persisted := int64(50)
			barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
				return &ckpt.CheckpointContext{Timestamp: persisted}, nil
			}
			barrierCkptFlushFunc = func(*OplogSyncer) bool {
				if signal == "Pause" {
					utils.IncrSentinelOptions.Pause = true
				} else {
					utils.IncrSentinelOptions.Shutdown = true
				}
				return false
			}
			require.False(t, syncer.checkCheckpointUpdate(true, 100))
			utils.IncrSentinelOptions.Pause = false
			utils.IncrSentinelOptions.Shutdown = false
			// Resume must flush and confirm the same target before continuing.
			barrierCkptFlushFunc = func(*OplogSyncer) bool {
				persisted = 100
				return true
			}
			require.True(t, syncer.checkCheckpointUpdate(true, 100))
		})
	}
}

func newBarrierOrderingTestBatcher(t *testing.T) *Batcher {
	t.Helper()
	oldOptions := conf.Options
	oldSentinel := utils.IncrSentinelOptions
	t.Cleanup(func() { conf.Options = oldOptions; utils.IncrSentinelOptions = oldSentinel })
	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
	conf.Options.FilterDDLEnable = true
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true
	conf.Options.IncrSyncTargetDelay = 0
	utils.IncrSentinelOptions.TargetDelay = -1
	utils.IncrSentinelOptions.ExitPoint = -1
	syncer := &OplogSyncer{replMetric: &utils.ReplicationMetric{}}
	batcher := NewBatcher(syncer, filter.OplogFilterChain{
		new(filter.AutologousFilter), filter.NewNamespaceFilter(nil, nil), new(filter.NoopFilter),
	}, syncer, []*Worker{new(Worker)})
	t.Cleanup(batcher.Fini)
	return batcher
}

func TestBarrierPreservesDDLAndPreparedTransactionOrder(t *testing.T) {
	for _, kind := range []string{"DDL", "prepared transaction"} {
		t.Run(kind, func(t *testing.T) {
			batcher := newBarrierOrderingTestBatcher(t)
			syncer := newBarrierTestSyncer(t)
			syncer.batcher = batcher
			batcher.syncer = syncer
			syncer.logsQueue = []chan []*oplog.GenericOplog{make(chan []*oplog.GenericOplog, 1)}
			syncer.hasher = &oplog.PrimaryKeyHasher{}
			syncer.replMetric = &utils.ReplicationMetric{}
			batcher.handler = syncer
			batcher.workerGroup = []*Worker{
				{syncer: syncer, queue: make(chan []*oplog.GenericOplog, 4)},
				{syncer: syncer, queue: make(chan []*oplog.GenericOplog, 4)},
			}
			insert := func(second uint32) *oplog.GenericOplog {
				return &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
					Timestamp: primitive.Timestamp{T: second, I: 1}, Operation: "i",
					Namespace: "test.documents", Object: bson.D{{Key: "_id", Value: int(second)}},
				}}}
			}
			before, after := insert(100), insert(103)
			barrierOp := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
				Timestamp: primitive.Timestamp{T: 101, I: 1}, Operation: "c",
				Namespace: "test.$cmd", Object: bson.D{{Key: "drop", Value: "documents"}},
			}}}
			input := []*oplog.GenericOplog{before, barrierOp, after}
			if kind == "prepared transaction" {
				txn := int64(1)
				barrierOp.Parsed.Namespace = "admin.$cmd"
				barrierOp.Parsed.LSID = utils.MarshalData(bson.D{{Key: "id", Value: primitive.NewObjectID()}})
				barrierOp.Parsed.TxnNumber = &txn
				barrierOp.Parsed.PrevOpTime = append(bson.Raw(nil), emptyPrevRaw...)
				barrierOp.Parsed.Object = bson.D{
					{Key: "applyOps", Value: bson.A{bson.D{
						{Key: "op", Value: "i"}, {Key: "ns", Value: "test.documents"},
						{Key: "o", Value: bson.D{{Key: "_id", Value: 101}}},
					}}}, {Key: "prepare", Value: true},
				}
				commit := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
					Timestamp: primitive.Timestamp{T: 102, I: 1}, Operation: "c", Namespace: "admin.$cmd",
					LSID: barrierOp.Parsed.LSID, TxnNumber: &txn,
					PrevOpTime: utils.MarshalData(bson.D{{Key: "ts", Value: barrierOp.Parsed.Timestamp}, {Key: "t", Value: int64(1)}}),
					Object:     bson.D{{Key: "commitTransaction", Value: 1}, {Key: "commitTimestamp", Value: barrierOp.Parsed.Timestamp}},
				}}}
				input = []*oplog.GenericOplog{before, barrierOp, commit, after}
			}
			syncer.logsQueue[0] <- input
			var persisted int64
			barrierCkptGetFunc = func(*OplogSyncer) (*ckpt.CheckpointContext, error) {
				return &ckpt.CheckpointContext{Timestamp: persisted}, nil
			}
			barrierCkptFlushFunc = func(*OplogSyncer) bool {
				lowest, err := syncer.calculateWorkerLowestCheckpoint()
				if err != nil || lowest <= persisted {
					return false
				}
				persisted = lowest
				return true
			}
			for phase := 0; phase < 3; phase++ {
				groups, barrier, empty, exit := batcher.BatchMore()
				require.False(t, empty)
				require.False(t, exit)
				require.Equal(t, phase < 2, barrier)
				var ops []*oplog.GenericOplog
				for _, group := range groups {
					ops = append(ops, group...)
				}
				require.Len(t, ops, 1, "operations on either side of a barrier must stay separate")
				if phase == 0 {
					require.Same(t, before, ops[0])
				} else if phase == 1 {
					require.Len(t, groups[0], 1, "barrier operations must still use worker group 0")
					if kind == "DDL" {
						require.Same(t, barrierOp, ops[0])
					} else {
						require.Equal(t, "i", ops[0].Parsed.Operation)
						require.Equal(t, 101, oplog.GetKey(ops[0].Parsed.Object, "_id"))
					}
				} else {
					require.Same(t, after, ops[0])
					break
				}
				batcher.dispatchBatches(groups)
				// Keep an additional worker behind, even if its batch was empty.
				target := utils.TimeStampToInt64(ops[0].Parsed.Timestamp)
				for _, worker := range batcher.workerGroup {
					<-worker.queue
					atomic.StoreInt64(&worker.unack, target)
					worker.AllAcked(false)
				}
				result := startBarrierTestWait(t, syncer, target)
				batcher.workerGroup[0].setAck(target)
				requireBarrierPending(t, result)
				batcher.workerGroup[1].setAck(target)
				requireBarrierResult(t, result, true)
			}
		})
	}
}
