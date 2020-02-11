package collector

import (
	"mongoshake/collector/filter"
	"mongoshake/common"
	"mongoshake/oplog"
	"mongoshake/collector/configure"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

var (
	moveChunkFilter filter.MigrateFilter
	ddlFilter       filter.DDLFilter
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

	// the last oplog in the batch
	lastOplog *oplog.PartialLog

	// the last filtered oplog in the batch
	lastFilterOplog *oplog.PartialLog

	// remainLogs store the logs that split by barrier and haven't been consumed yet.
	remainLogs []*oplog.GenericOplog
}

func NewBatcher(syncer *OplogSyncer, filterList filter.OplogFilterChain,
	handler OplogHandler, workerGroup []*Worker) *Batcher {
	return &Batcher{
		syncer:      syncer,
		filterList:  filterList,
		handler:     handler,
		workerGroup: workerGroup,
	}
}

/*
 * return the last oplog, if the current batch is empty(first oplog in this batch is ddl),
 * just return the last oplog in the previous batch.
 * if just start, this is nil.
 */
func (batcher *Batcher) getLastOplog() (*oplog.PartialLog, *oplog.PartialLog) {
	return batcher.lastOplog, batcher.lastFilterOplog
}

func (batcher *Batcher) filter(log *oplog.PartialLog) bool {
	// filter oplog such like Noop or Gid-filtered
	if batcher.filterList.IterateFilter(log) {
		LOG.Debug("Oplog is filtered. %v", log)
		if batcher.syncer.replMetric != nil {
			batcher.syncer.replMetric.AddFilter(1)
		}
		return true
	}

	if moveChunkFilter.Filter(log) {
		LOG.Crashf("move chunk oplog found[%v]", log)
		return false
	}

	// DDL is disable when timestamp <= fullSyncFinishPosition
	if ddlFilter.Filter(log) && utils.TimestampToInt64(log.Timestamp) <= batcher.syncer.fullSyncFinishPosition {
		LOG.Crashf("ddl oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
			log, log.Timestamp, batcher.syncer.fullSyncFinishPosition)
		return false
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

/**
 * return batched oplogs and barrier flag.
 * set barrier if find DDL.
 * i d i c u i
 *      | |
 */
func (batcher *Batcher) batchMore() ([][]*oplog.GenericOplog, bool, bool) {
	// picked raw oplogs and batching in sequence
	batchGroup := make([][]*oplog.GenericOplog, len(batcher.workerGroup))
	syncer := batcher.syncer

	// first part of merge batch is from current logs queue.
	// It's allowed to be blocked !
	var mergeBatch []*oplog.GenericOplog
	barrier := false
	if len(batcher.remainLogs) == 0 {
		// remainLogs is empty
		mergeBatch = <-syncer.logsQueue[batcher.currentQueue()]
		// move to next available logs queue
		batcher.moveToNextQueue()
		for len(mergeBatch) < conf.Options.AdaptiveBatchingMaxSize &&
			len(syncer.logsQueue[batcher.currentQueue()]) > 0 {
			// there has more pushed oplogs in next logs queue (read can't to be block)
			// Hence, we fetch them by the way. and merge together
			mergeBatch = append(mergeBatch, <-syncer.logsQueue[batcher.nextQueue]...)
			batcher.moveToNextQueue()
		}
	} else {
		// remainLogs isn't empty
		mergeBatch = batcher.remainLogs
		batcher.remainLogs = make([]*oplog.GenericOplog, 0)
		barrier = true
	}

	nimo.AssertTrue(len(mergeBatch) != 0, "logs queue batch logs has zero length")

	// split batch if has DDL
	allEmpty := true
	for i, genericLog := range mergeBatch {
		// TODO, 测试发现，这里可能有问题，导致勿以为是有DDL操作进行checkpoint的强刷barrier，2.4版本写完这里要再看看。
		LOG.Info("xxxxxxxxx %s %v", genericLog.Parsed.Operation, genericLog.Parsed.Timestamp >> 32)
		// filter oplog such like Noop or Gid-filtered
		if batcher.filter(genericLog.Parsed) {
			// doesn't push to worker, set lastFilterOplog
			batcher.lastFilterOplog = genericLog.Parsed
			continue
		}

		allEmpty = false

		// current is ddl and barrier == false
		if !conf.Options.ReplayerDMLOnly && ddlFilter.Filter(genericLog.Parsed) && !barrier {
			// store and handle in the next call
			batcher.remainLogs = mergeBatch[i:]
			barrier = true
			break
		}
		// current is not ddl but barrier == true
		if !ddlFilter.Filter(genericLog.Parsed) && barrier {
			barrier = false
		}
		batcher.handler.Handle(genericLog.Parsed)

		which := syncer.hasher.DistributeOplogByMod(genericLog.Parsed, len(batcher.workerGroup))
		batchGroup[which] = append(batchGroup[which], genericLog)
		batcher.lastOplog = genericLog.Parsed

		// barrier == true which means the current must be ddl so we should return only 1 oplog and then do split
		if barrier {
			if i+1 < len(mergeBatch) {
				batcher.remainLogs = mergeBatch[i+1:]
			}
			break
		}
	}
	return batchGroup, barrier, allEmpty
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}
