package collector

import (
	"mongoshake/collector/filter"
	"mongoshake/oplog"
	"mongoshake/collector/configure"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
	"time"
)


const (
	noopInterval = 10 // s
)

var (
	moveChunkFilter filter.MigrateFilter
	ddlFilter       filter.DDLFilter
	fakeOplog = &oplog.GenericOplog {
		Raw: nil,
		Parsed: &oplog.PartialLog { // initial fake oplog only used in comparison
			ParsedLog: oplog.ParsedLog{
				Timestamp: bson.MongoTimestamp(-2), // fake timestamp,
				Operation: "meaningless operation",
			},
		},
	}

)

func getTargetDelay() int64 {
	if utils.IncrSentinelOptions.TargetDelay < 0 {
		return conf.Options.IncrSyncTargetDelay
	} else {
		return utils.IncrSentinelOptions.TargetDelay
	}
}

func getExitPoint() bson.MongoTimestamp {
	if utils.IncrSentinelOptions.ExitPoint <= 0 {
		return 0
	}
	// change to timestamp
	return bson.MongoTimestamp(utils.IncrSentinelOptions.ExitPoint << 32)
}

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
	lastOplog *oplog.GenericOplog
	// first oplog in the next batch
	previousOplog *oplog.GenericOplog
	// the last filtered oplog in the batch
	lastFilterOplog *oplog.PartialLog

	// remainLogs store the logs that split by barrier and haven't been consumed yet.
	remainLogs []*oplog.GenericOplog
	// we have already flush on the previous oplog?
	previousFlush bool
	// need flush barrier next generation
	needBarrier bool

	// batchMore inner usage
	batchGroup        [][]*oplog.GenericOplog
	transactionOplogs []*oplog.PartialLog

	// for ut only
	utBatchesDelay struct {
		flag        bool                  // ut enable?
		injectBatch []*oplog.GenericOplog // input batched oplog
		delay       int                   // the delay times
	}
}

func NewBatcher(syncer *OplogSyncer, filterList filter.OplogFilterChain,
	handler OplogHandler, workerGroup []*Worker) *Batcher {
	return &Batcher{
		syncer:          syncer,
		filterList:      filterList,
		handler:         handler,
		workerGroup:     workerGroup,
		previousOplog:   fakeOplog, // initial fake oplog only used in comparison
		lastOplog:       fakeOplog,
		lastFilterOplog: fakeOplog.Parsed,
		previousFlush:   false,
	}
}

/*
 * return the last oplog, if the current batch is empty(first oplog in this batch is ddl),
 * just return the last oplog in the previous batch.
 * if just start, this is nil.
 */
func (batcher *Batcher) getLastOplog() (*oplog.PartialLog, *oplog.PartialLog) {
	return batcher.lastOplog.Parsed, batcher.lastFilterOplog
}

func (batcher *Batcher) filter(log *oplog.PartialLog) bool {
	// filter oplog such like Noop or Gid-filtered
	if batcher.filterList.IterateFilter(log) {
		LOG.Debug("%s oplog is filtered. %v", batcher.syncer, log)
		batcher.syncer.replMetric.AddFilter(1)
		return true
	}

	if moveChunkFilter.Filter(log) {
		LOG.Crashf("move chunk oplog found[%v]", log)
		return false
	}

	// DDL is disable when timestamp <= fullSyncFinishPosition
	if ddlFilter.Filter(log) && log.Timestamp <= batcher.syncer.fullSyncFinishPosition {
		LOG.Crashf("%s ddl oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
			batcher.syncer, log, utils.ExtractTimestampForLog(log.Timestamp),
			utils.ExtractTimestampForLog(batcher.syncer.fullSyncFinishPosition))
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

// get a batch
func (batcher *Batcher) getBatch() []*oplog.GenericOplog {
	syncer := batcher.syncer
	var mergeBatch []*oplog.GenericOplog
	if len(batcher.remainLogs) == 0 {
		// remainLogs is empty.
		/*
		 * first part of merge batch is from current logs queue.
		 * we have 3 judgements:
		 * 1. if logs queue isn't empty, we return immediately. if not, goto 2 or 3.
		 * 2. if the previous log isn't empty which means this log needs to be flushed
		 * as soon as possible, so we wait at most 1 second.
		 * 3. if the previous log is empty, we wait 10s same as the noop oplog interval
		 */
		if batcher.previousOplog == fakeOplog {
			// previous oplog is empty. rule 1, 3
			select {
			case mergeBatch = <-syncer.logsQueue[batcher.currentQueue()]:
				break
			case <-time.After(noopInterval * time.Second):
				// return nil if timeout
				return nil
			}
		} else {
			// previous oplog isn't empty. rule 1, 2
			select {
			case mergeBatch = <-syncer.logsQueue[batcher.currentQueue()]:
				break
			case <-time.After(1 * time.Second):
				// return nil if timeout
				return nil
			}
		}

		// move to next available logs queue
		batcher.moveToNextQueue()
		for len(mergeBatch) < conf.Options.IncrSyncAdaptiveBatchingMaxSize &&
			len(syncer.logsQueue[batcher.currentQueue()]) > 0 {
			// there has more pushed oplogs in next logs queue (read can't to be block)
			// Hence, we fetch them by the way. and merge together
			mergeBatch = append(mergeBatch, <-syncer.logsQueue[batcher.nextQueue]...)
			batcher.moveToNextQueue()
		}
	} else {
		// remainLogs isn't empty
		mergeBatch = batcher.remainLogs
		// we can't use "batcher.remainLogs = batcher.remainLogs[:0]" here
		batcher.remainLogs = make([]*oplog.GenericOplog, 0)
	}

	nimo.AssertTrue(len(mergeBatch) != 0, "logs queue batch logs has zero length")

	return mergeBatch
}

/*
 * if delay > 0, this function wait till delay timeout.
 * However, if the mergeBatch contain may oplogs, the delay time will depends on the first
 * oplog timestamp. So, if the time span of the included batched oplog is too large, the
 * delay time is inaccurate.
 * the second return value marks whether should exit.
 */
func (batcher *Batcher) getBatchWithDelay() ([]*oplog.GenericOplog, bool) {
	var mergeBatch []*oplog.GenericOplog
	if !batcher.utBatchesDelay.flag {
		mergeBatch = batcher.getBatch()
	} else { // for ut only
		mergeBatch = batcher.utBatchesDelay.injectBatch
	}
	if mergeBatch == nil {
		return mergeBatch, false
	}

	// judge should exit
	exitPoint := getExitPoint()
	lastOplog := mergeBatch[len(mergeBatch) - 1].Parsed
	if exitPoint > 0 && lastOplog.Timestamp > batcher.syncer.fullSyncFinishPosition && exitPoint < lastOplog.Timestamp {
		// only run detail judgement when exit point is bigger than the last one
		LOG.Info("%s exitPoint[%v] < lastOplog.Timestamp[%v]", batcher.syncer,
			utils.ExtractTimestampForLog(exitPoint), utils.ExtractTimestampForLog(lastOplog.Timestamp))
		var i int
		for i = range mergeBatch {
			// fmt.Println(exitPoint, mergeBatch[i].Parsed.Timestamp)
			if exitPoint < mergeBatch[i].Parsed.Timestamp {
				LOG.Info("%s exitPoint[%v] < current.Timestamp[%v]", batcher.syncer,
					utils.ExtractTimestampForLog(exitPoint), utils.ExtractTimestampForLog(mergeBatch[i].Parsed.Timestamp))
				break
			}
		}
		return mergeBatch[: i], true
	}

	// judge whether should delay
	delay := getTargetDelay()
	if delay > 0 {
		firstOplog := mergeBatch[0].Parsed
		// do not wait delay when oplog time less than fullSyncFinishPosition
		if firstOplog.Timestamp > batcher.syncer.fullSyncFinishPosition {
			for {
				// only run sleep if delay > 0
				// re-fetch delay in every round
				delay = getTargetDelay()
				delayBoundary := time.Now().Unix() - delay + 3 // 3 is for NTP drift

				if utils.ExtractMongoTimestamp(firstOplog.Timestamp) > delayBoundary {
					LOG.Info("%s --- wait target delay[%v seconds]: first oplog timestamp[%v] > delayBoundary[%v], fullSyncFinishPosition[%v]",
						batcher.syncer, delay, utils.ExtractTimestampForLog(firstOplog.Timestamp), delayBoundary,
						utils.ExtractTimestampForLog(batcher.syncer.fullSyncFinishPosition))
					time.Sleep(5 * time.Second)

					// for ut only
					batcher.utBatchesDelay.delay++
				} else {
					break
				}
			}
		}
	}

	return mergeBatch, false
}

/**
 * this function is used to gather oplogs together.
 * honestly speaking, it's complicate so that reading unit tests may help you
 * to make it more clear. The reason this function is so complicate is there're
 * too much corner cases here.
 * return batched oplogs and barrier flag.
 * set barrier if find DDL.
 * i d i c u i
 *      | |
 */
func (batcher *Batcher) BatchMore() ([][]*oplog.GenericOplog, bool, bool, bool) {
	// picked raw oplogs and batching in sequence
	batcher.batchGroup = make([][]*oplog.GenericOplog, len(batcher.workerGroup))

	if batcher.transactionOplogs == nil {
		batcher.transactionOplogs = make([]*oplog.PartialLog, 0)
	}
	barrier := false

	// try to get batch
	mergeBatch, exit := batcher.getBatchWithDelay()

	// heartbeat
	if mergeBatch == nil {
		// we can't fetch any data currently
		if batcher.previousOplog == fakeOplog {
			// no cached data, set filterOplog and break.
			// filterOplog is fake when == lastOplog.
			// this is for flushing checkpoint only.
			if batcher.lastOplog.Parsed.Timestamp > batcher.lastFilterOplog.Timestamp {
				batcher.lastFilterOplog = batcher.lastOplog.Parsed
			}
			batcher.previousFlush = false
			return batcher.batchGroup, false, batcher.setLastOplog(), exit
		}

		LOG.Info("%s batcher flushes cached oplog", batcher.syncer)
		// we have cached previous data that need to flush
		if batcher.flushBufferOplogs() {
			barrier = true
		}
		batcher.previousFlush = barrier
		return batcher.batchGroup, barrier, batcher.setLastOplog(), exit
	}

	// we have data
	for i, genericLog := range mergeBatch {
		// filter oplog such like Noop or Gid-filtered
		// PAY ATTENTION: we can't handle the oplog in transaction that has been filtered
		if batcher.filter(genericLog.Parsed) {
			// don't push to worker, set lastFilterOplog
			batcher.lastFilterOplog = genericLog.Parsed
			if batcher.flushBufferOplogs() {
				barrier = true
				batcher.remainLogs = mergeBatch[i + 1:]
				batcher.previousFlush = true
				return batcher.batchGroup, true, batcher.setLastOplog(), exit
			}
			batcher.previousOplog = fakeOplog
			continue
		}

		// current is ddl
		if ddlFilter.Filter(genericLog.Parsed) {
			// enable ddl?
			if conf.Options.FilterDDLEnable {
				// store and handle in the next call
				if batcher.previousFlush == true {
					batcher.addIntoBatchGroup(batcher.previousOplog)
					// we have flush before, add barrier after
					batcher.addIntoBatchGroup(genericLog)
					batcher.remainLogs = mergeBatch[i + 1:]
				} else {
					// add barrier before, current oplog should be handled on the
					// next iteration
					batcher.flushBufferOplogs()
					batcher.remainLogs = mergeBatch[i:]
				}

				barrier = true
				batcher.previousOplog = fakeOplog
				batcher.previousFlush = true
				return batcher.batchGroup, true, batcher.setLastOplog(), exit
			} else {
				// filter
				batcher.syncer.replMetric.AddFilter(1)
				// doesn't push to worker, set lastFilterOplog
				batcher.lastFilterOplog = genericLog.Parsed
				if batcher.flushBufferOplogs() {
					barrier = true
					batcher.remainLogs = mergeBatch[i + 1:]
					return batcher.batchGroup, true, batcher.setLastOplog(), exit
				}
				batcher.previousOplog = fakeOplog
				continue
			}
		}

		// need merge transaction?
		if genericLog.Parsed.Timestamp == batcher.previousOplog.Parsed.Timestamp {
			if len(batcher.transactionOplogs) == 0 && batcher.previousFlush == false {
				// no transaction before, flush batchGroup
				batcher.transactionOplogs = append(batcher.transactionOplogs, batcher.previousOplog.Parsed)
				batcher.previousOplog = genericLog
				batcher.remainLogs = mergeBatch[i + 1:]
				batcher.previousFlush = false
				return batcher.batchGroup, true, batcher.setLastOplog(), exit
			} else {
				// have transaction before, add previous
				batcher.transactionOplogs = append(batcher.transactionOplogs, batcher.previousOplog.Parsed)
			}
		} else if len(batcher.transactionOplogs) != 0 {
			batcher.transactionOplogs = append(batcher.transactionOplogs, batcher.previousOplog.Parsed)
			gathered := batcher.gatherTransaction()

			batcher.addIntoBatchGroup(gathered)
			batcher.remainLogs = mergeBatch[i:]
			batcher.previousOplog = fakeOplog

			barrier = true
			batcher.transactionOplogs = nil
			batcher.previousFlush = true
			return batcher.batchGroup, true, batcher.setLastOplog(), exit
		} else {
			batcher.addIntoBatchGroup(batcher.previousOplog)
			batcher.previousFlush = false
		}

		batcher.previousOplog = genericLog
	}

	batcher.previousFlush = barrier
	return batcher.batchGroup, barrier, batcher.setLastOplog(), exit
}

func (batcher *Batcher) setLastOplog() bool {
	// all oplogs are filtered?
	allEmpty := true
	for _, ele := range batcher.batchGroup {
		if ele != nil && len(ele) > 0 {
			allEmpty = false
			rawLast := ele[len(ele) - 1]
			if rawLast.Parsed.Timestamp > batcher.lastOplog.Parsed.Timestamp {
				batcher.lastOplog = rawLast
			}
		}
	}
	return allEmpty
}

func (batcher *Batcher) addIntoBatchGroup(genericLog *oplog.GenericOplog) {
	if genericLog == fakeOplog {
		return
	}

	batcher.handler.Handle(genericLog.Parsed)
	which := batcher.syncer.hasher.DistributeOplogByMod(genericLog.Parsed, len(batcher.workerGroup))
	batcher.batchGroup[which] = append(batcher.batchGroup[which], genericLog)
}

func (batcher *Batcher) gatherTransaction() *oplog.GenericOplog {
	// transaction oplogs should gather into an applyOps operation and add barrier here
	gathered, err := oplog.GatherApplyOps(batcher.transactionOplogs)
	if err != nil {
		LOG.Crashf("%s gather applyOps failed[%v]", batcher.syncer, err)
	}
	return gathered
}

// flush previous buffered oplog, true means should add barrier
func (batcher *Batcher) flushBufferOplogs() bool {
	if batcher.previousOplog == fakeOplog {
		return false
	}

	txLength := len(batcher.transactionOplogs)
	if txLength > 0 {
		if batcher.previousOplog == fakeOplog {
			LOG.Crashf("%s previous is fakeOplog when transaction oplogs is empty", batcher.syncer)
		}
		if batcher.previousOplog.Parsed.Timestamp != batcher.transactionOplogs[txLength - 1].Timestamp {
			LOG.Crashf("%s previous oplog timestamp[%v] != transaction oplog timestamp[%v]",
				batcher.syncer, batcher.previousOplog.Parsed.Timestamp, batcher.transactionOplogs[txLength - 1].Timestamp)
		}

		batcher.transactionOplogs = append(batcher.transactionOplogs, batcher.previousOplog.Parsed)
		gathered := batcher.gatherTransaction()

		batcher.addIntoBatchGroup(gathered)
		batcher.previousOplog = fakeOplog

		batcher.transactionOplogs = nil
		return true
	}

	batcher.addIntoBatchGroup(batcher.previousOplog)
	batcher.previousOplog = fakeOplog
	return false
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}
