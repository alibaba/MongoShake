package collector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/quorum"
)

// #1051 P2 regression: a pure-filter stream (only noop oplogs, lastOplog is the
// startup fakeOplog with ts=0) must still advance the persisted checkpoint to the
// noop timestamp. Waiting on the zero-ts fakeOplog returns false immediately and
// must not be treated as a Pause/Shutdown interrupt that skips the write.
//
// Drives batcherOnce synchronously for a bounded number of iterations (no leaked
// goroutine, no cleanup race) and asserts the persisted value, not just a write
// request count (a store that rejects writes must fail this test).
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

	noopTs := primitive.Timestamp{T: 100, I: 1}
	noop := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: noopTs, Operation: "n", Namespace: "test.documents",
	}}}
	state := &batcherLoopState{filterCheckTs: time.Now()}
	// First iteration arms the filter flag; subsequent ones advance the checkpoint.
	for i := 0; i < 4; i++ {
		syncer.logsQueue[0] <- []*oplog.GenericOplog{noop}
		syncer.batcherOnce(state)
	}
	require.Equal(t, utils.TimeStampToInt64(noopTs), store.timestamp.Load(),
		"pure-filter stream must persist the noop timestamp (a write-rejecting store must fail this)")
}
