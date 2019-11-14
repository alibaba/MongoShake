package collector

import (
	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/common"
	"mongoshake/oplog"
	"sync/atomic"
	"time"
)

const (
	WaitAckIntervalMS = 1000
)

var (
	moveChunkFilter filter.MigrateFilter
	ddlFilter       filter.DDLFilter
	noopFilter      filter.NoopFilter
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

	// timestamp of prepare to sync oplog at collector, including filtered oplog
	unsyncTs bson.MongoTimestamp
	// timestamp of already synced oplog at collector, including filtered oplog
	syncTs bson.MongoTimestamp

	lastResponseTime time.Time

	// remainLogs store the logs that split by barrier and haven't been consumed yet.
	remainLogs []*oplog.GenericOplog
}

func NewBatcher(syncer *OplogSyncer, filterList filter.OplogFilterChain,
	handler OplogHandler, workerGroup []*Worker) *Batcher {
	return &Batcher{
		syncer:           syncer,
		filterList:       filterList,
		handler:          handler,
		workerGroup:      workerGroup,
		lastResponseTime: time.Now(),
	}
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

	// move chunk is disable when timestamp <= fullSyncFinishPosition
	if moveChunkFilter.Filter(log) && utils.TimestampToInt64(log.Timestamp) <= batcher.syncer.fullSyncFinishPosition {
		LOG.Crashf("move chunk oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
			log, log.Timestamp, batcher.syncer.fullSyncFinishPosition)
	}

	// DDL is disable when timestamp <= fullSyncFinishPosition
	if ddlFilter.Filter(log) && utils.TimestampToInt64(log.Timestamp) <= batcher.syncer.fullSyncFinishPosition {
		LOG.Crashf("ddl oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
			log, log.Timestamp, batcher.syncer.fullSyncFinishPosition)
		return false
	}
	return false
}

func (batcher *Batcher) dispatchBatch(nextBatch []*oplog.GenericOplog) (work bool) {
	batchGroup := make([][]*oplog.GenericOplog, len(batcher.workerGroup))

	for _, genericLog := range nextBatch {
		which := batcher.syncer.hasher.DistributeOplogByMod(genericLog.Parsed, len(batcher.workerGroup))
		batchGroup[which] = append(batchGroup[which], genericLog)
	}

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

/*
 * return batched oplogs and barrier flag
 * return the last oplog, if the current batch is empty(first oplog in this batch is ddl),
 * just return the last oplog in the previous batch.
 * if just start, this is nil.
 */
func (batcher *Batcher) Next() []*oplog.GenericOplog {
	// picked raw oplogs and batching in sequence
	syncer := batcher.syncer
	// first part of merge batch is from current logs queue.
	// It's allowed to be blocked !
	var nextBatch []*oplog.GenericOplog
	if len(batcher.remainLogs) == 0 {
		// remainLogs is empty
		nextBatch = <-syncer.logsQueue[batcher.currentQueue()]
		// move to next available logs queue
		batcher.moveToNextQueue()
		for len(nextBatch) < conf.Options.AdaptiveBatchingMaxSize &&
			len(syncer.logsQueue[batcher.currentQueue()]) > 0 {
			// there has more pushed oplogs in next logs queue (read can't to be block)
			// Hence, we fetch them by the way. and merge together
			nextBatch = append(nextBatch, <-syncer.logsQueue[batcher.nextQueue]...)
			batcher.moveToNextQueue()
		}
	} else {
		// remainLogs isn't empty
		nextBatch = batcher.remainLogs
		batcher.remainLogs = make([]*oplog.GenericOplog, 0)
	}
	nimo.AssertTrue(len(nextBatch) != 0, "logs queue batch logs has zero length")
	batcher.lastResponseTime = time.Now()
	return nextBatch
}

func (batcher *Batcher) filterAndBlockMoveChunk(nextBatch []*oplog.GenericOplog, barrier bool) ([]*oplog.GenericOplog, bool, bool, *oplog.PartialLog) {
	syncer := batcher.syncer
	var lastOplog *oplog.PartialLog
	flushCheckpoint := false
	var filteredNextBatch []*oplog.GenericOplog
	lastUnSyncTs := batcher.unsyncTs
	for i, genericLog := range nextBatch {
		if genericLog.Parsed.Timestamp > batcher.unsyncTs {
			lastUnSyncTs = batcher.unsyncTs
			batcher.unsyncTs = genericLog.Parsed.Timestamp
		}

		// filter oplog such like Autologous or Gid-filtered
		if batcher.filter(genericLog.Parsed) {
			continue
		}
		// ensure the oplog order when moveChunk occurs if enabled move chunk at source sharding db
		// need noop oplog to update OfferTs of move chunk when no valid oplog occur in shard db
		if conf.Options.MoveChunkEnable {
			mcBarrier, resend := moveChunkBarrier(batcher.syncer, genericLog.Parsed)
			if mcBarrier {
				if resend {
					// the oplog need to resend again, so rollback unsyncTs
					batcher.unsyncTs = lastUnSyncTs
					batcher.remainLogs = nextBatch[i:]
				} else {
					batcher.remainLogs = nextBatch[i+1:]
				}
				barrier = true
				break
			}
		}
		if noopFilter.Filter(genericLog.Parsed) || moveChunkFilter.Filter(genericLog.Parsed) {
			continue
		}
		// current is ddl and barrier == false
		if !conf.Options.ReplayerDMLOnly && ddlFilter.Filter(genericLog.Parsed) && !barrier {
			batcher.unsyncTs = lastUnSyncTs
			batcher.remainLogs = nextBatch[i:]
			barrier = true
			flushCheckpoint = true
			LOG.Info("oplog syncer %v batch more with ddl oplog type[%v] ns[%v] object[%v]. lastOplog[%v]",
				syncer.replset, genericLog.Parsed.Operation, genericLog.Parsed.Namespace, genericLog.Parsed.Object, lastOplog)
			break
		}
		// current is not ddl but barrier == true
		if !ddlFilter.Filter(genericLog.Parsed) && barrier {
			barrier = false
		}
		batcher.handler.Handle(genericLog.Parsed)
		lastOplog = genericLog.Parsed

		filteredNextBatch = append(filteredNextBatch, genericLog)

		// barrier == true which means the current must be ddl so we should return only 1 oplog and then do split
		if barrier {
			batcher.remainLogs = nextBatch[i+1:]
			break
		}
	}
	return filteredNextBatch, barrier, flushCheckpoint, lastOplog
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}

func (batcher *Batcher) isAllAcked() bool {
	var maxAck int64 = 0
	for _, worker := range batcher.workerGroup {
		ack := atomic.LoadInt64(&worker.ack)
		unack := atomic.LoadInt64(&worker.unack)
		// all oplogs have been acked for right now or previous status
		if ack != unack && !worker.IsAllAcked() {
			LOG.Info("oplog syncer %v not acked. unack[%v] ack[%v]",
				batcher.syncer.replset, utils.TimestampToLog(unack), utils.TimestampToLog(ack))
			return false
		}
		if ack > maxAck {
			maxAck = ack
		}
	}
	LOG.Info("oplog syncer %v all acked. ack[%v]", batcher.syncer.replset, utils.TimestampToLog(maxAck))
	return true
}

func (batcher *Batcher) WaitAllAck() {
	for {
		if batcher.isAllAcked() {
			break
		}
		utils.YieldInMs(WaitAckIntervalMS)
	}
}

// operations for _id record before migrate delete at node A must start ahead of operations after migrate insert at B
func moveChunkBarrier(syncer *OplogSyncer, partialLog *oplog.PartialLog) (bool, bool) {
	if syncer.batcher.syncTs >= partialLog.Timestamp {
		return false, false
	}
	if ddlFilter.Filter(partialLog) {
		return false, false
	}
	barrier, resend, _ := syncer.mvckManager.BarrierOplog(syncer.replset, partialLog)
	return barrier, resend
}
