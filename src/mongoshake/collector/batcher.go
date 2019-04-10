package collector

import (
	"mongoshake/oplog"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

/*
 * as we mentioned in syncer.go, Batcher is used to batch oplog before sending in order to
 * improve performance.
 */
type Batcher struct {
	// related oplog syncer. not owned
	syncer *OplogSyncer

	// filter functionality by gid
	filterList filter.OplogFilterChain
	// oplog handler
	handler OplogHandler

	// current queue cursor
	nextQueue uint64
	// related tunnel workerGroup. not owned
	workerGroup []*Worker

	lastOplog *oplog.PartialLog
}

func (batcher *Batcher) getLastOplog() *oplog.PartialLog {
	return batcher.lastOplog
}
func (batcher *Batcher) filter(log *oplog.PartialLog) bool {
	// filter oplog suchlike Noop or Gid-filtered
	if batcher.filterList.IterateFilter(log) {
		LOG.Debug("Oplog is filtered. %v", log)
		batcher.syncer.replMetric.AddFilter(1)
		return true
	}
	return false
}

func (batcher *Batcher) dispatchBatches(batchGroup [][]*oplog.GenericOplog) (work bool) {
	for i, batch := range batchGroup {
		// we still push logs even if length is zero. so without length check
		if batch != nil {
			work = true
			batcher.workerGroup[i].AllAcked(false)
		}
		batcher.workerGroup[i].Offer(batch)
	}
	return
}

func (batcher *Batcher) batchMore() [][]*oplog.GenericOplog {
	// picked raw oplogs and batching in sequence
	batchGroup := make([][]*oplog.GenericOplog, len(batcher.workerGroup))
	syncer := batcher.syncer

	// first part of merge batch is from current logs queue.
	// It's allowed to be blocked !
	mergeBatch := <-syncer.logsQueue[batcher.currentQueue()]
	// move to next available logs queue
	batcher.moveToNextQueue()
	for len(mergeBatch) < conf.Options.AdaptiveBatchingMaxSize &&
		len(syncer.logsQueue[batcher.currentQueue()]) > 0 {
		// there has more pushed oplogs in next logs queue (read can't to be block)
		// Hence, we fetch them by the way. and merge together
		mergeBatch = append(mergeBatch, <-syncer.logsQueue[batcher.nextQueue]...)
		batcher.moveToNextQueue()
	}
	nimo.AssertTrue(len(mergeBatch) != 0, "logs queue batch logs has zero length")

	for _, genericLog := range mergeBatch {
		// filter oplog such like Noop or Gid-filtered
		if batcher.filter(genericLog.Parsed) {
			// doesn't push to worker
			continue
		}
		batcher.handler.Handle(genericLog.Parsed)

		which := syncer.hasher.DistributeOplogByMod(genericLog.Parsed, len(batcher.workerGroup))
		batchGroup[which] = append(batchGroup[which], genericLog)
		batcher.lastOplog = genericLog.Parsed
	}
	return batchGroup
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}
