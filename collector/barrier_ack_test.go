package collector

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestBarrierCheckpointACKSurvivesPersistenceFailure(t *testing.T) {
	for _, tc := range []struct {
		name         string
		store        *barrierCheckpointStore
		pin, release int64
		persisted    int64
	}{
		{name: "write_failure_without_pin", store: &barrierCheckpointStore{failWrite: true}, persisted: 100},
		{name: "write_failure_after_completed_pin", store: &barrierCheckpointStore{failWrite: true}, pin: 150, release: 200, persisted: 100},
		{name: "write_failure_before_later_pin", store: &barrierCheckpointStore{failWrite: true}, pin: 300, persisted: 100},
		{name: "stale_readback", store: &barrierCheckpointStore{readbackTimestamp: 100}, persisted: 200},
		{name: "failed_readback", store: &barrierCheckpointStore{failReadback: true}, persisted: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.store
			store.timestamp.Store(100)
			worker := &Worker{ack: 200, unack: 200}
			syncer := newBarrierCheckpointIOSyncer(t, store, worker)
			if tc.pin > 0 {
				syncer.batcher.pinTxn(oplog.TxnID{}, tc.pin)
				if tc.release > 0 {
					syncer.batcher.stageTxnPinRelease(oplog.TxnID{}, tc.release)
				}
			}
			requireBarrierResult(t, startBarrierTestWait(t, syncer, 200), true)
			require.Equal(t, int64(1), store.writes.Load(), "try persistence once without coupling ACK completion to its outcome")
			require.Equal(t, tc.persisted, store.timestamp.Load(), "ACK completion must not fabricate a durable checkpoint")
			if tc.store.failWrite || tc.store.readbackTimestamp > 0 {
				require.Equal(t, int64(100), syncer.ckptManager.GetInMemory().Timestamp)
			}
			wantPin := int64(0)
			if tc.pin > 200 {
				wantPin = tc.pin
			}
			require.Equal(t, wantPin, syncer.batcher.OldestUnackedTxnPinTs(worker.GetAck()))
		})
	}
}

func TestBarrierCheckpointPersistenceRecoversAfterACKCompletion(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	store := &barrierCheckpointStore{rejectWrites: &reject}
	store.timestamp.Store(100)
	worker := &Worker{ack: 200, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	requireBarrierResult(t, startBarrierTestWait(t, syncer, 200), true)
	require.Equal(t, int64(100), store.timestamp.Load())
	require.Equal(t, int64(100), syncer.ckptManager.GetInMemory().Timestamp)
	reject.Store(false)
	conf.Options.CheckpointInterval = 0
	require.True(t, syncer.checkpoint(false, 0), "normal checkpoint processing must persist the completed ACK after recovery")
	checkpoint, _, err := syncer.ckptManager.Get()
	require.NoError(t, err)
	require.Equal(t, int64(200), checkpoint.Timestamp)
	require.Equal(t, int64(200), store.timestamp.Load())
}

func TestBarrierCheckpointIntermediateACKDoesNotBypassIORetry(t *testing.T) {
	store := &barrierCheckpointStore{}
	store.timestamp.Store(100)
	worker := &Worker{ack: 120, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	for i := 1; i < 8; i++ {
		syncer.batcher.workerGroup = append(syncer.batcher.workerGroup, &Worker{syncer: syncer, ack: 120, unack: 200})
	}
	BarrierCheckpointInterval, BarrierCheckpointRetryInterval = 5*time.Second, 5*time.Second
	// Unbuffered handshakes let the test observe that each ACK was processed,
	// without relying on sleeps or allowing several updates to coalesce.
	syncer.workerAckNotify = make(chan struct{})
	initialReadback := make(chan struct{}, 1)
	get := barrierCkptGetFunc
	barrierCkptGetFunc = func(syncer *OplogSyncer) (*ckpt.CheckpointContext, error) {
		checkpoint, err := get(syncer)
		if err == nil && checkpoint.Timestamp == 120 {
			select {
			case initialReadback <- struct{}{}:
			default:
			}
		}
		return checkpoint, err
	}
	result := startBarrierTestWait(t, syncer, 200)
	t.Cleanup(func() {
		// Also terminate an old implementation after a failed I/O assertion.
		for _, worker := range syncer.batcher.workerGroup {
			worker.setAck(200)
		}
		select {
		case <-result:
		case syncer.workerAckNotify <- struct{}{}:
		case <-time.After(time.Second):
		}
	})
	select {
	case <-initialReadback:
	case <-time.After(time.Second):
		t.Fatal("initial checkpoint was not flushed and read back")
	}
	for ack := int64(130); ack < 200; ack += 10 {
		for _, worker := range syncer.batcher.workerGroup {
			worker.setAck(ack)
		}
		for hint := 0; hint < 2; hint++ {
			select {
			case syncer.workerAckNotify <- struct{}{}:
			case got := <-result:
				t.Fatalf("barrier returned %v at incomplete ACK %d", got, ack)
			case <-time.After(time.Second):
				t.Fatal("barrier did not process ACK notification")
			}
		}
		require.Equal(t, int64(1), store.writes.Load(), "intermediate ACK must respect checkpoint I/O deadline")
		require.Equal(t, int64(2), store.reads.Load())
		require.Equal(t, int64(120), store.timestamp.Load())
	}
	for _, worker := range syncer.batcher.workerGroup {
		worker.setAck(200)
	}
	select {
	case got := <-result:
		require.True(t, got)
	case syncer.workerAckNotify <- struct{}{}:
		requireBarrierResult(t, result, true)
	case <-time.After(time.Second):
		t.Fatal("final ACK did not complete the barrier")
	}
	require.Equal(t, int64(2), store.writes.Load(), "final ACK may bypass a successful intermediate flush's deadline")
	require.Equal(t, int64(3), store.reads.Load())
	require.Equal(t, int64(200), store.timestamp.Load())
}

func TestBarrierCheckpointIOHelperRestoresIntervals(t *testing.T) {
	interval, retry := BarrierCheckpointInterval, BarrierCheckpointRetryInterval
	t.Run("modify", func(t *testing.T) {
		store := &barrierCheckpointStore{}
		store.timestamp.Store(100)
		newBarrierCheckpointIOSyncer(t, store, &Worker{ack: 100, unack: 100})
		BarrierCheckpointInterval = interval + time.Second
		BarrierCheckpointRetryInterval = retry + time.Second
	})
	require.Equal(t, interval, BarrierCheckpointInterval)
	require.Equal(t, retry, BarrierCheckpointRetryInterval)
}

func TestBarrierCheckpointACKDuringFailedWriteKeepsBackoff(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	store := &barrierCheckpointStore{rejectWrites: &reject, entered: make(chan struct{}, 1), release: release}
	store.timestamp.Store(100)
	worker := &Worker{ack: 150, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	BarrierCheckpointRetryInterval = time.Second
	result := startBarrierTestWait(t, syncer, 200)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("intermediate ACK did not enter checkpoint write")
	}
	worker.setAck(200)
	once.Do(func() { close(release) })
	requireBarrierResult(t, result, true)
	require.Equal(t, int64(1), store.writes.Load(), "final ACK must not bypass the failed write's backoff")
	require.Equal(t, int64(100), store.timestamp.Load())
	require.Equal(t, int64(100), syncer.ckptManager.GetInMemory().Timestamp)
	reject.Store(false)
	conf.Options.CheckpointInterval = 0
	require.True(t, syncer.checkpoint(false, 0))
	checkpoint, _, err := syncer.ckptManager.Get()
	require.NoError(t, err)
	require.Equal(t, int64(200), checkpoint.Timestamp)
}
