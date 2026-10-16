package collector

import (
	"testing"

	"github.com/alibaba/MongoShake/v2/collector/filter"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"

	"github.com/stretchr/testify/assert"
)

// A transaction's data lives only in memory between its first buffered entry
// (prepare/partialTxn) and the worker-ack of its inner ops. The persisted
// checkpoint must never reach the transaction's start ts during that window,
// otherwise a restart will skip the prepare oplog and silently lose the
// transaction.
func TestTxnPinLifecycle(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
	conf.Options.FilterDDLEnable = true

	ts := func(sec int64) int64 { return utils.TimeStampToInt64(utils.TimeToTimestamp(sec)) }

	// committed prepared transaction: prepare@100, normal@101, commit@102
	{
		syncer := mockSyncer()
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
		logs := mockDisTxnOplogs(100, true, true) // [prepare@100, normal@101, commit@102]

		// no transaction buffered yet -> no pin
		assert.Equal(t, int64(0), batcher.OldestUnackedTxnPinTs(0), "should be equal")

		// prepare buffered -> pinned at prepare ts
		prepareMeta, ok := batcher.isTransaction(logs[0].Parsed)
		assert.Equal(t, true, ok, "should be equal")
		batcher.handleTransaction(prepareMeta, logs[0])
		assert.Equal(t, ts(100), batcher.OldestUnackedTxnPinTs(0), "should be equal")

		// normal oplog acked (ack=101) must NOT release the pin
		assert.Equal(t, ts(100), batcher.OldestUnackedTxnPinTs(ts(101)), "should be equal")

		// commit processed: inner ops staged, pin must survive until acked
		commitMeta, ok := batcher.isTransaction(logs[2].Parsed)
		assert.Equal(t, true, ok, "should be equal")
		isRet, mustIndividual, deliveredOps := batcher.handleTransaction(commitMeta, logs[2])
		assert.Equal(t, true, isRet, "should be equal")
		assert.Equal(t, true, mustIndividual, "should be equal")
		assert.Equal(t, 3, len(deliveredOps), "should be equal")
		assert.Equal(t, ts(100), batcher.OldestUnackedTxnPinTs(ts(101)), "should be equal")

		// workers acked the inner ops (ack >= commit ts) -> pin released
		assert.Equal(t, int64(0), batcher.OldestUnackedTxnPinTs(ts(102)), "should be equal")
		// release is permanent
		assert.Equal(t, int64(0), batcher.OldestUnackedTxnPinTs(0), "should be equal")
	}

	// aborted transaction: prepare@200, abort@201 -> pin dropped at abort
	{
		syncer := mockSyncer()
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
		logs := mockDisTxnOplogs(200, false, false) // [prepare@200, abort@201]

		prepareMeta, _ := batcher.isTransaction(logs[0].Parsed)
		batcher.handleTransaction(prepareMeta, logs[0])
		assert.Equal(t, ts(200), batcher.OldestUnackedTxnPinTs(0), "should be equal")

		abortMeta, _ := batcher.isTransaction(logs[1].Parsed)
		isRet, _, _ := batcher.handleTransaction(abortMeta, logs[1])
		assert.Equal(t, false, isRet, "should be equal")
		assert.Equal(t, int64(0), batcher.OldestUnackedTxnPinTs(0), "should be equal")
	}

	// two overlapping transactions -> oldest pin wins, released independently
	{
		syncer := mockSyncer()
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
		logsA := mockDisTxnOplogs(300, false, true) // [prepareA@300, commitA@301]
		logsB := mockDisTxnOplogs(310, false, true) // [prepareB@310, commitB@311]

		metaA, _ := batcher.isTransaction(logsA[0].Parsed)
		batcher.handleTransaction(metaA, logsA[0])
		metaB, _ := batcher.isTransaction(logsB[0].Parsed)
		batcher.handleTransaction(metaB, logsB[0])
		assert.Equal(t, ts(300), batcher.OldestUnackedTxnPinTs(0), "should be equal")

		// commit A; ack reaches A's commit ts -> only B's pin remains
		commitA, _ := batcher.isTransaction(logsA[1].Parsed)
		batcher.handleTransaction(commitA, logsA[1])
		assert.Equal(t, ts(310), batcher.OldestUnackedTxnPinTs(ts(301)), "should be equal")
	}
}

// The value persisted by checkpoint() must be clamped below the oldest
// unacked transaction pin.
func TestClampCheckpointByTxnPin(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	ts := func(sec int64) int64 { return utils.TimeStampToInt64(utils.TimeToTimestamp(sec)) }

	syncer := mockSyncer()
	batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
	syncer.batcher = batcher

	// no pin -> passthrough
	assert.Equal(t, ts(101), syncer.clampCheckpointByTxnPin(ts(101), ts(101)), "should be equal")

	// buffer prepare@100 -> checkpoint candidate 101 must be clamped below 100
	logs := mockDisTxnOplogs(100, true, true)
	prepareMeta, _ := batcher.isTransaction(logs[0].Parsed)
	batcher.handleTransaction(prepareMeta, logs[0])

	clamped := syncer.clampCheckpointByTxnPin(ts(101), ts(101))
	assert.Equal(t, ts(100)-1, clamped, "should be equal")

	// candidate below the pin is untouched
	assert.Equal(t, ts(99), syncer.clampCheckpointByTxnPin(ts(99), ts(99)), "should be equal")

	// commit and ack inner ops -> clamp disappears
	commitMeta, _ := batcher.isTransaction(logs[2].Parsed)
	batcher.handleTransaction(commitMeta, logs[2])
	assert.Equal(t, ts(102), syncer.clampCheckpointByTxnPin(ts(102), ts(102)), "should be equal")
}
