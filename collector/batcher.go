package collector

import (
	"sync/atomic"
	"time"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	noopInterval = 10 // seconds
)

var (
	moveChunkFilter filter.MigrateFilter
	ddlFilter       filter.DDLFilter
	fakeOplog       = &oplog.GenericOplog{
		Raw: nil,
		Parsed: &oplog.PartialLog{ // initial fake oplog only used in comparison
			ParsedLog: oplog.ParsedLog{
				// fake timestamp that doesn't appear in reality, must be the smallest ts for compare in SetLastLog
				Timestamp: primitive.Timestamp{T: 0, I: 0},
				Operation: "meaningless operation",
			},
		},
	}
	emptyPrevRaw = bson.Raw{
		28, 0, 0, 0, 17, 116, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		18, 116, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0,
	}
)

func getTargetDelay() int64 {
	if utils.IncrSentinelOptions.TargetDelay < 0 {
		return conf.Options.IncrSyncTargetDelay
	} else {
		return utils.IncrSentinelOptions.TargetDelay
	}
}

func getExitPoint() primitive.Timestamp {
	if utils.IncrSentinelOptions.ExitPoint <= 0 {
		return primitive.Timestamp{}
	}

	// change to timestamp
	return utils.Int64ToTimestamp(utils.IncrSentinelOptions.ExitPoint)
}

// Batcher is used to batch oplog before sending in order to improve performance.
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
	// the last filtered oplog in the batch
	lastFilterOplog *oplog.PartialLog

	// remainLogs store the logs that split by barrier and haven't been consumed yet.
	remainLogs []*oplog.GenericOplog

	// batchMore inner usage
	batchGroup [][]*oplog.GenericOplog
	// Oplogs that need to be performed separately, like DDL(only one oplogs)、Transaction(some oplogs)
	barrierOplogs []*oplog.GenericOplog

	// transaction buffer
	txnBuffer *oplog.TxnBuffer

	// ingestGate is the barrier-quiesce gate. It is set by the poll loop, right before a DDL it has
	// just read is injected (see OplogSyncer.next), and released by that same loop once the pipeline
	// is quiescent again. While it is set, next() reads nothing further from the reader. nil means
	// open; the channel is only a marker -- the waiter loads the pointer in a loop and never
	// receives on it, so closing it is just the release step.
	ingestGate atomic.Pointer[chan struct{}]

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
		lastOplog:       fakeOplog,
		lastFilterOplog: fakeOplog.Parsed,
		txnBuffer:       oplog.NewBuffer(),
	}
}

func (batcher *Batcher) Fini() {
	_ = batcher.txnBuffer.Stop()
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
		l.Logger.Debugf("%s oplog is filtered. %v", batcher.syncer, log)
		batcher.syncer.replMetric.AddFilter(1)
		return true
	}

	if moveChunkFilter.Filter(log) {
		l.Logger.Criticalf("shake exit, must close balancer in sharding + oplog")
		l.Logger.Panicf("move chunk oplog found, must close balancer in sharding + oplog [%v]", log)
		return false
	}

	// DDL is disabled when timestamp <= fullSyncFinishPosition
	// v2.4.10: do not crash when "fetch_method" == "change_stream"
	if ddlFilter.Filter(log) &&
		primitive.CompareTimestamp(log.Timestamp, batcher.syncer.fullSyncFinishPosition) <= 0 &&
		conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
		l.Logger.Panicf("%s ddl oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
			batcher.syncer, log, log.Timestamp,
			batcher.syncer.fullSyncFinishPosition)
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
		queueIndex := batcher.currentQueue()
		select {
		case mergeBatch = <-syncer.logsQueue[queueIndex]:
			syncer.updateLogsQueueMetric(int(queueIndex))
			break
		case <-time.After(noopInterval * time.Second):
			// return nil if timeout
			return nil
		}

		// move to next available logs queue
		batcher.moveToNextQueue()
		for len(mergeBatch) < conf.Options.IncrSyncAdaptiveBatchingMaxSize &&
			len(syncer.logsQueue[batcher.currentQueue()]) > 0 {
			// there has more pushed oplogs in next logs queue (read can't to be blocked)
			// Hence, we fetch them by the way. and merge together
			queueIndex := batcher.nextQueue
			mergeBatch = append(mergeBatch, <-syncer.logsQueue[queueIndex]...)
			syncer.updateLogsQueueMetric(int(queueIndex))
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
 * However, if the mergeBatch contain may oplogs, the delay time will depend on the first
 * oplog timestamp. So, if the time span of the included batched oplog is too large, the
 * delay time is inaccurate.
 * the second return value marks if we should exit.
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
	lastOplog := mergeBatch[len(mergeBatch)-1].Parsed

	if !exitPoint.IsZero() &&
		primitive.CompareTimestamp(lastOplog.Timestamp, batcher.syncer.fullSyncFinishPosition) > 0 &&
		primitive.CompareTimestamp(exitPoint, lastOplog.Timestamp) < 0 {
		// only run detail judgement when exit point is bigger than the last one
		l.Logger.Infof("%s exitPoint[%v] < lastOplog.Timestamp[%v]", batcher.syncer, exitPoint, lastOplog.Timestamp)
		var i int
		for i = range mergeBatch {
			// fmt.Println(exitPoint, mergeBatch[i].Parsed.Timestamp)
			if primitive.CompareTimestamp(exitPoint, mergeBatch[i].Parsed.Timestamp) < 0 {
				l.Logger.Infof("%s exitPoint[%v] < current.Timestamp[%v]", batcher.syncer,
					exitPoint, mergeBatch[i].Parsed.Timestamp)
				break
			}
		}
		return mergeBatch[:i], true
	}

	// judge if we should delay
	delay := getTargetDelay()
	if delay > 0 {
		firstOplog := mergeBatch[0].Parsed
		// do not wait delay when oplog time less than fullSyncFinishPosition
		if primitive.CompareTimestamp(firstOplog.Timestamp, batcher.syncer.fullSyncFinishPosition) > 0 {
			for {
				// only run sleep if delay > 0
				// re-fetch delay in every round
				delay = getTargetDelay()
				delayBoundary := time.Now().Unix() - delay + 3 // 3 is for NTP drift

				if utils.ExtractMongoTimestamp(firstOplog.Timestamp) > delayBoundary {
					l.Logger.Infof("%s --- wait target delay[%v seconds]: "+
						"first oplog timestamp[%v] > delayBoundary[%v], fullSyncFinishPosition[%v]",
						batcher.syncer, delay, firstOplog.Timestamp, delayBoundary,
						batcher.syncer.fullSyncFinishPosition)
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

// BatchMore
/**
 * This function is used to gather oplogs together.
 * Honestly speaking, it's complicate so that reading unit tests may help you to make it more clear.
 * The reason this function is so complicated is that there are too many corner cases involved.
 * Return batched oplogs and barrier flag, set barrier if meet DDL.
 * i d i c u i
 *      | |
 */
func (batcher *Batcher) BatchMore() (genericOplogs [][]*oplog.GenericOplog, barrier bool, allEmpty bool, exit bool) {
	// picked raw oplogs and batching in sequence
	batcher.batchGroup = make([][]*oplog.GenericOplog, len(batcher.workerGroup))
	if batcher.barrierOplogs == nil {
		batcher.barrierOplogs = make([]*oplog.GenericOplog, 0)
	}

	// Have barrier Oplogs to performed
	if len(batcher.barrierOplogs) > 0 {
		for _, v := range batcher.barrierOplogs {
			if batcher.filter(v.Parsed) {
				batcher.lastFilterOplog = v.Parsed
				continue
			}
			if ddlFilter.Filter(v.Parsed) && !conf.Options.FilterDDLEnable {
				batcher.lastFilterOplog = v.Parsed
				continue
			}

			batcher.addIntoBatchGroup(v, true)
			//l.Logger.Infof("%s transfer barrierOplogs into batchGroup, i[%d], oplog[%v]", batcher.syncer, i, v.Parsed)
		}
		batcher.barrierOplogs = nil

		if !incrOrderingGuarantee() {
			// Otherwise allEmpty may be true here, and the caller's `!allEmpty` guard is then
			// allowed to skip the barrier branch.
			return batcher.batchGroup, true, batcher.setLastOplog(), false
		}

		// direct: lastOplog still has to advance past the barrier oplogs just placed in batchGroup,
		// and the round reports allEmpty=false even if every barrier oplog was dropped by the
		// filters above, so the caller's `!allEmpty` guard can never swallow the barrier wait.
		batcher.setLastOplog()
		return batcher.batchGroup, true, false, false
	}

	// try to get batch
	mergeBatch, exit := batcher.getBatchWithDelay()

	if mergeBatch == nil {
		return batcher.batchGroup, false, batcher.setLastOplog(), exit
	}

	stopAt := batcher.processMergeBatch(mergeBatch)
	if stopAt >= 0 {
		// the oplogs behind the barrier wait for it to be done
		batcher.remainLogs = mergeBatch[stopAt+1:]

		if !incrOrderingGuarantee() {
			// Otherwise the round keeps its original verdict: setLastOplog() supplies allEmpty.
			return batcher.batchGroup, true, batcher.setLastOplog(), false
		}

		// lastOplog still has to advance past the oplogs that preceded the barrier: processMergeBatch
		// put them in batchGroup before it stopped at the barrier.
		batcher.setLastOplog()

		// A barrier round always reports allEmpty=false, regardless of batchGroupEmpty(). When the
		// barrier is mergeBatch[0] -- or every oplog before it was filtered -- batchGroup ends up
		// empty, and reporting true lets startBatcher's `!allEmpty` guard skip the entire barrier
		// branch: the wait for the oplogs dispatched ahead of the barrier and the forced checkpoint
		// are both swallowed, and the barrier is handed to worker[0] on the next round while earlier
		// DML is still queued in other workers -- a guaranteed reorder.
		return batcher.batchGroup, true, false, false
	}

	return batcher.batchGroup, false, batcher.setLastOplog(), exit
}

// processMergeBatch filters mergeBatch and distributes it into batcher.batchGroup. It returns
// the index of the barrier oplog that must run separately, or -1 when mergeBatch holds none.
func (batcher *Batcher) processMergeBatch(mergeBatch []*oplog.GenericOplog) int {
	for i, genericLog := range mergeBatch {
		// filter oplog such like Noop or with gid
		// PAY ATTENTION: we can't handle the oplog in transaction that has been filtered
		if batcher.filter(genericLog.Parsed) {
			// don't push to worker, set lastFilterOplog
			batcher.lastFilterOplog = genericLog.Parsed
			//l.Logger.Debugf("~~~~~~~~~filter %v %v", i, genericLog.Parsed)
			continue
		}

		// Transaction
		if txnMeta, txnOk := batcher.isTransaction(genericLog.Parsed); txnOk {
			//l.Logger.Debugf("~~~~~~~~~transaction %v %v", i, genericLog.Parsed)
			isRet, mustIndividual, deliveredOps := batcher.handleTransaction(txnMeta, genericLog)
			if !isRet {
				continue
			}
			if mustIndividual {
				batcher.barrierOplogs = deliveredOps

				nimo.AssertTrue(batcher.batchGroupEmpty(), "batcher.batchGroup don't be empty")
				return i
			} else {
				for _, ele := range deliveredOps {
					batcher.addIntoBatchGroup(ele, false)
				}
				continue
			}
		}

		// no transaction applyOps
		if genericLog.Parsed.Operation == "c" {
			operation, _ := oplog.ExtraCommandName(genericLog.Parsed.Object)
			if operation == "applyOps" {
				deliveredOps, err := oplog.ExtractInnerOps(&genericLog.Parsed.ParsedLog)
				if err != nil {
					l.Logger.Panicf("applyOps extract failed. err[%v] oplog[%v]",
						err, genericLog.Parsed.ParsedLog)
				}

				for _, ele := range deliveredOps {
					batcher.addIntoBatchGroup(&oplog.GenericOplog{
						Raw:        nil,
						SourceTime: genericLog.SourceTime,
						Parsed: &oplog.PartialLog{
							ParsedLog: ele,
						},
					}, false)
				}
				continue
			}
		}

		// current is ddl
		if ddlFilter.Filter(genericLog.Parsed) {

			if conf.Options.FilterDDLEnable {
				// DDL is executed as a barrier: it is handed to worker[0] on its own round
				batcher.barrierOplogs = append(batcher.barrierOplogs, genericLog)

				return i
			} else {
				// filter
				batcher.syncer.replMetric.AddFilter(1)
				// doesn't push to worker, set lastFilterOplog
				batcher.lastFilterOplog = genericLog.Parsed

				continue
			}
		}

		batcher.addIntoBatchGroup(genericLog, false)
	}

	return -1
}

func (batcher *Batcher) setLastOplog() bool {
	// all oplogs are filtered?
	allEmpty := batcher.batchGroupEmpty()
	for _, ele := range batcher.batchGroup {
		if ele != nil && len(ele) > 0 {
			rawLast := ele[len(ele)-1]
			if primitive.CompareTimestamp(rawLast.Parsed.Timestamp, batcher.lastOplog.Parsed.Timestamp) > 0 {
				batcher.lastOplog = rawLast
			}
		}
	}
	return allEmpty
}

// batchGroupEmpty reports whether batchGroup holds no oplog yet.
func (batcher *Batcher) batchGroupEmpty() bool {
	for _, ele := range batcher.batchGroup {
		if ele != nil && len(ele) > 0 {
			return false
		}
	}
	return true
}

// setIngestGate closes the gate, leaving an already-closed gate untouched so a release can never
// be missed. The poll loop is the only writer; the compare-and-swap only keeps a stray second
// caller from clobbering a live gate.
func (batcher *Batcher) setIngestGate() {
	if batcher.ingestGate.Load() == nil {
		gate := make(chan struct{})
		if !batcher.ingestGate.CompareAndSwap(nil, &gate) {
			close(gate) // a live gate won: this marker has no owner
		}
	}
}

// clearIngestGate releases the gate set by setIngestGate. The pointer is taken out by the same
// compare-and-swap that authorizes the close, so the same channel is never closed twice.
// Releasing an already open gate is a no-op.
func (batcher *Batcher) clearIngestGate() {
	for {
		p := batcher.ingestGate.Load()
		if p == nil {
			return // already open
		}
		if batcher.ingestGate.CompareAndSwap(p, nil) {
			close(*p)
			return
		}
	}
}

// addIntoBatchGroup
// isBarrier
//
//	Barrier Oplogs(like DDL or Transaction) must execute sequentially and separately, send to batchGroup[0]
func (batcher *Batcher) addIntoBatchGroup(genericLog *oplog.GenericOplog, isBarrier bool) {
	if genericLog == fakeOplog {
		return
	}

	batcher.handler.Handle(genericLog.Parsed)

	var which uint32
	if isBarrier {
		which = 0
	} else {
		which = batcher.syncer.hasher.DistributeOplogByMod(genericLog.Parsed, len(batcher.workerGroup))
	}
	batcher.batchGroup[which] = append(batcher.batchGroup[which], genericLog)

	// l.Logger.Debugf("add into worker[%v]: %v", which, genericLog.Parsed.ParsedLog)
}

func (batcher *Batcher) isTransaction(partialLog *oplog.PartialLog) (oplog.TxnMeta, bool) {
	//l.Logger.Infof("isTransaction input oplog:%v lsid[%v] TxnNumber[%v] Object[%v]", partialLog,
	//	partialLog.ParsedLog.LSID, partialLog.ParsedLog.TxnNumber, partialLog.ParsedLog.Object)
	if partialLog.Operation == "c" {
		txnMeta, err := oplog.NewTxnMeta(partialLog.ParsedLog)
		if err != nil {
			return oplog.TxnMeta{}, false
		}

		return txnMeta, txnMeta.IsTxn()
	}

	return oplog.TxnMeta{}, false
}

func (batcher *Batcher) handleTransaction(txnMeta oplog.TxnMeta,
	genericLog *oplog.GenericOplog) (isRet bool, mustIndividual bool,
	deliveredOps []*oplog.GenericOplog) {
	err := batcher.txnBuffer.AddOp(txnMeta, genericLog)
	if err != nil {
		l.Logger.Panicf("%s add oplog to txnbuffer failed, err[%v] oplog[%v]",
			batcher.syncer, err, genericLog.Parsed.ParsedLog)
	}

	// distributed transaction is abort, ignore these Oplogs and clear buffer
	if txnMeta.IsAbort() {
		err := batcher.txnBuffer.PurgeTxn(txnMeta)
		if err != nil {
			l.Logger.Panicf("%s cleaning up txnBuffer failed, err[%v] oplog[%v]",
				batcher.syncer, err, genericLog.Parsed.ParsedLog)
		}

		batcher.syncer.replMetric.AddFilter(1)
		batcher.lastFilterOplog = genericLog.Parsed
		return false, false, nil
	}

	if !txnMeta.IsCommit() {
		// transaction can not be committed
		return false, false, nil
	}

	haveCommandInTransaction := false
	mustIndividual = true
	// transaction can be commit now
	ops, errs := batcher.txnBuffer.GetTxnStream(txnMeta)
Loop:
	for {
		select {
		case o, ok := <-ops:
			if !ok {
				break Loop
			}
			if o.Parsed.Operation == "c" {
				haveCommandInTransaction = true
			}

			// Raw will be filling in Send->LogEntryEncode
			deliveredOps = append(deliveredOps, o)
		case err := <-errs:
			if err != nil {
				l.Logger.Panicf("error replaying transaction, err[%v]", err)
			}
			break Loop
		}
	}

	// Individual transaction that do not have command can run with other CURD oplog
	if !txnMeta.IsCommitOp() && !haveCommandInTransaction &&
		genericLog.Parsed.PrevOpTime.String() == emptyPrevRaw.String() {
		mustIndividual = false
	}
	// transaction applyOps that do not have command can run in parallel
	if haveCommandInTransaction {
		mustIndividual = true
	}

	err = batcher.txnBuffer.PurgeTxn(txnMeta)
	if err != nil {
		l.Logger.Panicf("error cleaning up transaction buffer, err[%v]", err)
	}

	return true, mustIndividual, deliveredOps
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}

// waitForAllWorkersIdle waits until all workers have empty queues and all dispatched
// oplogs have been acknowledged (ack == unack), i.e. until every oplog handed to a worker
// has been applied on the target. Only meaningful for the direct tunnel, whose Send() is a
// synchronous write; the callers gate on incrOrderingGuarantee(). The Pause/Shutdown escape
// mirrors the one in checkCheckpointUpdate's barrier wait.
func (batcher *Batcher) waitForAllWorkersIdle() bool {
	l.Logger.Info("%s waiting for all workers to be idle (barrier)", batcher.syncer)

	for i := 0; ; i++ {
		if utils.IncrSentinelOptions.Pause || utils.IncrSentinelOptions.Shutdown {
			l.Logger.Warn("%s barrier wait interrupted by sentinel (Pause=%v, Shutdown=%v)",
				batcher.syncer, utils.IncrSentinelOptions.Pause, utils.IncrSentinelOptions.Shutdown)
			return false
		}

		allIdle := true
		for _, worker := range batcher.workerGroup {
			queueLen := len(worker.queue)
			ack := atomic.LoadInt64(&worker.ack)
			unack := atomic.LoadInt64(&worker.unack)
			if queueLen > 0 || (unack != ack) {
				allIdle = false
				break
			}
		}

		if allIdle {
			l.Logger.Info("%s all workers are idle, barrier satisfied", batcher.syncer)
			return true
		}

		if i%CheckCheckpointUpdateTimes == 0 {
			l.Logger.Info("%s[%d] waiting for workers to be idle", batcher.syncer, i)
		}

		utils.YieldInMs(DDLCheckpointInterval)
	}
}
