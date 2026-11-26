package collector

import (
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/quorum"
)

// #1051 P2 regression: a pure-filter stream (only noop oplogs, lastOplog is the
// startup fakeOplog with ts=0) must still advance the checkpoint. Waiting on the
// zero-ts fakeOplog returns false immediately and must not be treated as a
// Pause/Shutdown interrupt that skips the reader update and checkpoint write.
func TestPureFilterStreamAdvancesCheckpoint(t *testing.T) {
	quorum.AlwaysMaster()
	store := &barrierCheckpointStore{}
	syncer := newBarrierCheckpointIOSyncer(t, store, &Worker{})
	syncer.batcher = NewBatcher(syncer, filter.OplogFilterChain{
		new(filter.AutologousFilter), filter.NewNamespaceFilter(nil, nil), new(filter.NoopFilter),
	}, syncer, []*Worker{new(Worker)})
	syncer.logsQueue = []chan []*oplog.GenericOplog{make(chan []*oplog.GenericOplog, 16)}
	syncer.hasher = &oplog.PrimaryKeyHasher{}
	syncer.reader = sourceReader.NewOplogReader("mongodb://127.0.0.1:1", "test")
	oldInterval, oldGap, oldGids := FilterCheckpointCheckInterval, FilterCheckpointGap, conf.Options.FilterOplogGids
	FilterCheckpointCheckInterval, FilterCheckpointGap, conf.Options.FilterOplogGids = 0, 0, false
	t.Cleanup(func() {
		FilterCheckpointCheckInterval, FilterCheckpointGap, conf.Options.FilterOplogGids = oldInterval, oldGap, oldGids
		syncer.batcher.Fini()
	})

	stop := make(chan struct{})
	var stopped atomic.Bool
	go func() {
		defer stopped.Store(true)
		for {
			select {
			case <-stop:
				return
			default:
			}
			noop := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
				Timestamp: primitive.Timestamp{T: 100, I: 1}, Operation: "n", Namespace: "test.documents",
			}}}
			select {
			case syncer.logsQueue[0] <- []*oplog.GenericOplog{noop}:
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	syncer.startBatcher()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if store.writes.Load() >= 1 {
			close(stop)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	t.Fatal("pure-filter stream did not advance checkpoint (zero-ts fakeOplog wait treated as interrupt)")
}
