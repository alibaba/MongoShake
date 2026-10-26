package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

type barrierCheckpointStore struct {
	timestamp         atomic.Int64
	reads             atomic.Int64
	writes            atomic.Int64
	failWrite         bool
	rejectWrites      *atomic.Bool
	readbackTimestamp int64
	failReadback      bool
	entered           chan struct{}
	release           <-chan struct{}
}

func (store *barrierCheckpointStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		store.reads.Add(1)
		if store.failReadback && store.writes.Load() > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		timestamp := store.timestamp.Load()
		if store.readbackTimestamp > 0 && store.writes.Load() > 0 {
			timestamp = store.readbackTimestamp
		}
		_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
			Name: "barrier-io", Timestamp: timestamp, Version: utils.FcvCheckpoint.CurrentVersion,
		})
		return
	}
	store.writes.Add(1)
	if store.entered != nil {
		select {
		case store.entered <- struct{}{}:
		default:
		}
		<-store.release
	}
	if store.failWrite || (store.rejectWrites != nil && store.rejectWrites.Load()) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var checkpoint ckpt.CheckpointContext
	if err := json.NewDecoder(r.Body).Decode(&checkpoint); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	store.timestamp.Store(checkpoint.Timestamp)
}

func newBarrierCheckpointIOSyncer(t *testing.T, store *barrierCheckpointStore, worker *Worker) *OplogSyncer {
	t.Helper()
	syncer := newBarrierTestSyncer(t)
	originalOptions, originalRetry := conf.Options, BarrierCheckpointRetryInterval
	t.Cleanup(func() { conf.Options = originalOptions; BarrierCheckpointRetryInterval = originalRetry })
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = false
	conf.Options.Tunnel = utils.VarTunnelDirect
	manager := ckpt.NewCheckpointManager("barrier-io", store.timestamp.Load())
	_, _, err := manager.Get()
	require.NoError(t, err)
	store.reads.Store(0)
	syncer.ckptManager, syncer.replMetric = manager, &utils.ReplicationMetric{}
	syncer.batcher = &Batcher{workerGroup: []*Worker{worker}, txnPins: make(map[oplog.TxnID]*txnPin)}
	worker.syncer = syncer
	barrierCkptGetFunc = func(syncer *OplogSyncer) (*ckpt.CheckpointContext, error) {
		checkpoint, _, err := syncer.ckptManager.Get()
		return checkpoint, err
	}
	barrierCkptFlushFunc = func(syncer *OplogSyncer) bool { return syncer.checkpoint(true, 0) }
	return syncer
}

func TestBarrierCheckpointIORetriesAreRateLimited(t *testing.T) {
	for _, tc := range []struct {
		name string
		ack  int64
	}{
		{name: "no_ack_progress", ack: 100},
		{name: "write_failure_with_fresh_ack_notifications", ack: 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &barrierCheckpointStore{failWrite: true}
			store.timestamp.Store(100)
			worker := &Worker{ack: tc.ack, unack: 200}
			syncer := newBarrierCheckpointIOSyncer(t, store, worker)
			// Polling can be arbitrarily fast without changing checkpoint I/O
			// pacing. Actual ACK changes must not bypass failed-write backoff.
			BarrierCheckpointInterval = time.Millisecond
			BarrierCheckpointRetryInterval = 80 * time.Millisecond
			get := barrierCkptGetFunc
			deadline := time.Now().Add(180 * time.Millisecond)
			barrierCkptGetFunc = func(syncer *OplogSyncer) (*ckpt.CheckpointContext, error) {
				if !time.Now().Before(deadline) {
					utils.IncrSentinelOptions.Shutdown = true
				}
				return get(syncer)
			}
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				ack := tc.ack
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if tc.ack > 100 && ack < 199 {
							ack++
							worker.setAck(ack)
						}
					}
				}
			}()
			result := syncer.checkCheckpointUpdate(true, 200)
			close(stop)
			<-stopped
			require.False(t, result, "an incomplete worker must not release the barrier")
			require.LessOrEqual(t, store.reads.Load(), int64(5), "fallback/ACK hints must not amplify remote reads")
			require.LessOrEqual(t, store.writes.Load(), int64(5), "failed writes require independent backoff")
			if tc.ack == 100 {
				require.Zero(t, store.writes.Load(), "unchanged ACK needs no persistence attempt")
			} else {
				require.Positive(t, store.writes.Load(), "the rejected-write path must really execute")
			}
			require.Equal(t, int64(100), store.timestamp.Load())
			require.Equal(t, int64(100), syncer.ckptManager.GetInMemory().Timestamp)
		})
	}
}

func TestBarrierCheckpointPinnedACKDoesNotWrite(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	store := &barrierCheckpointStore{failWrite: true, entered: make(chan struct{}, 1), release: release}
	store.timestamp.Store(100)
	worker := &Worker{ack: 200, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	syncer.batcher.pinTxn(oplog.TxnID{}, 150)
	result := startBarrierTestWait(t, syncer, 200)
	select {
	case got := <-result:
		require.True(t, got)
	case <-store.entered:
		once.Do(func() { close(release) })
		requireBarrierResult(t, result, true)
		t.Fatal("a genuine pending pin must use completed worker ACKs without waiting for a checkpoint write")
	case <-time.After(time.Second):
		t.Fatal("completed pinned barrier did not return")
	}
	require.Zero(t, store.writes.Load())
	require.Equal(t, int64(100), store.timestamp.Load())
	require.Equal(t, int64(150), syncer.batcher.OldestUnackedTxnPinTs(worker.GetAck()), "the unrelated transaction must remain pinned")
}

func TestBarrierCheckpointFreshACKDoesNotWaitForIORetry(t *testing.T) {
	store := &barrierCheckpointStore{}
	store.timestamp.Store(100)
	worker := &Worker{ack: 150, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	BarrierCheckpointInterval = time.Second
	BarrierCheckpointRetryInterval = 500 * time.Millisecond
	readback := make(chan struct{}, 1)
	get := barrierCkptGetFunc
	barrierCkptGetFunc = func(syncer *OplogSyncer) (*ckpt.CheckpointContext, error) {
		checkpoint, err := get(syncer)
		if err == nil && checkpoint.Timestamp == 150 {
			select {
			case readback <- struct{}{}:
			default:
			}
		}
		return checkpoint, err
	}
	result := startBarrierTestWait(t, syncer, 200)
	select {
	case <-readback:
	case <-time.After(time.Second):
		t.Fatal("initial ACK was not persisted and read back")
	}
	worker.setAck(200)
	select {
	case got := <-result:
		require.True(t, got)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fresh ACK waited for the 500ms checkpoint retry deadline")
	}
	require.Equal(t, int64(200), store.timestamp.Load())
	require.Equal(t, int64(2), store.writes.Load())
}

func TestBarrierCheckpointCompletedPinStillPersists(t *testing.T) {
	store := &barrierCheckpointStore{}
	store.timestamp.Store(100)
	worker := &Worker{ack: 200, unack: 200}
	syncer := newBarrierCheckpointIOSyncer(t, store, worker)
	syncer.batcher.pinTxn(oplog.TxnID{}, 150)
	syncer.batcher.stageTxnPinRelease(oplog.TxnID{}, 200)
	require.True(t, syncer.checkCheckpointUpdate(true, 200))
	require.Equal(t, int64(200), store.timestamp.Load(), "a completed pin is not a reason to skip final persistence")
	require.Equal(t, int64(200), syncer.ckptManager.GetInMemory().Timestamp)
	require.Equal(t, int64(1), store.writes.Load())
	require.Zero(t, syncer.batcher.OldestUnackedTxnPinTs(worker.GetAck()))
}
