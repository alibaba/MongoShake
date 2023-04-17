package collector

import (
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"time"

	utils "github.com/alibaba/MongoShake/v2/common"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

const (
	noopInterval = 10 // s
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
	batcher.txnBuffer.Stop()
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
		LOG.Critical("shake exit, must close balancer in sharding + oplog")
		LOG.Crashf("move chunk oplog found, must close balancer in sharding + oplog [%v]", log)
		return false
	}

	// DDL is disable when timestamp <= fullSyncFinishPosition
	// v2.4.10: do not crash when "fetch_method" == "change_stream"
	if ddlFilter.Filter(log) &&
		primitive.CompareTimestamp(log.Timestamp, batcher.syncer.fullSyncFinishPosition) <= 0 &&
		conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
		LOG.Crashf("%s ddl oplog found[%v] when oplog timestamp[%v] less than fullSyncFinishPosition[%v]",
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
		select {
		case mergeBatch = <-syncer.logsQueue[batcher.currentQueue()]:
			break
		case <-time.After(noopInterval * time.Second):
			// return nil if timeout
			return nil
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
	lastOplog := mergeBatch[len(mergeBatch)-1].Parsed

	if !exitPoint.IsZero() &&
		primitive.CompareTimestamp(lastOplog.Timestamp, batcher.syncer.fullSyncFinishPosition) > 0 &&
		primitive.CompareTimestamp(exitPoint, lastOplog.Timestamp) < 0 {
		// only run detail judgement when exit point is bigger than the last one
		LOG.Info("%s exitPoint[%v] < lastOplog.Timestamp[%v]", batcher.syncer, exitPoint, lastOplog.Timestamp)
		var i int
		for i = range mergeBatch {
			// fmt.Println(exitPoint, mergeBatch[i].Parsed.Timestamp)
			if primitive.CompareTimestamp(exitPoint, mergeBatch[i].Parsed.Timestamp) < 0 {
				LOG.Info("%s exitPoint[%v] < current.Timestamp[%v]", batcher.syncer,
					exitPoint, mergeBatch[i].Parsed.Timestamp)
				break
			}
		}
		return mergeBatch[:i], true
	}

	// judge whether should delay
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
					LOG.Info("%s --- wait target delay[%v seconds]: "+
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
 * this function is used to gather oplogs together.
 * honestly speaking, it's complicate so that reading unit tests may help you
 * to make it more clear. The reason this function is so complicate is there're
 * too much corner cases here.
 * return batched oplogs and barrier flag.
 * set barrier if find DDL.
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
			//LOG.Info("%s transfer barrierOplogs into batchGroup, i[%d], oplog[%v]", batcher.syncer, i, v.Parsed)
		}
		batcher.barrierOplogs = nil

		return batcher.batchGroup, true, batcher.setLastOplog(), false
	}

	// try to get batch
	mergeBatch, exit := batcher.getBatchWithDelay()

	if mergeBatch == nil {
		return batcher.batchGroup, false, batcher.setLastOplog(), exit
	}

	for i, genericLog := range mergeBatch {
		//LOG.Info("~~~~~~~~~enter_input %v %v\n", i, genericLog.Parsed)

		// filter oplog such like Noop or Gid-filtered
		// PAY ATTENTION: we can't handle the oplog in transaction that has been filtered
		if batcher.filter(genericLog.Parsed) {
			// don't push to worker, set lastFilterOplog
			batcher.lastFilterOplog = genericLog.Parsed
			//LOG.Info("~~~~~~~~~filter %v %v", i, genericLog.Parsed)
			continue
		}

		// Transactoin
		if txnMeta, txnOk := batcher.isTransaction(genericLog.Parsed); txnOk {
			//LOG.Info("~~~~~~~~~transaction %v %v", i, genericLog.Parsed)
			isRet, mustIndividual, _, deliveredOps := batcher.handleTransaction(txnMeta, genericLog)
			if !isRet {
				continue
			}
			if mustIndividual {
				batcher.barrierOplogs = deliveredOps

				batcher.remainLogs = mergeBatch[i+1:]

				allEmpty := batcher.setLastOplog()
				nimo.AssertTrue(allEmpty == true, "batcher.batchGroup don't be empty")
				return batcher.batchGroup, true, allEmpty, false
			} else {

				// TODO need do filter
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
					LOG.Crashf("applyOps extract failed. err[%v] oplog[%v]",
						err, genericLog.Parsed.ParsedLog)
				}

				// TODO need do filter
				for _, ele := range deliveredOps {
					batcher.addIntoBatchGroup(&oplog.GenericOplog{
						Raw: nil,
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
				batcher.barrierOplogs = append(batcher.barrierOplogs, genericLog)

				batcher.remainLogs = mergeBatch[i+1:]

				return batcher.batchGroup, true, batcher.setLastOplog(), false
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

	return batcher.batchGroup, false, batcher.setLastOplog(), exit
}

func (batcher *Batcher) setLastOplog() bool {
	// all oplogs are filtered?
	allEmpty := true
	for _, ele := range batcher.batchGroup {
		if ele != nil && len(ele) > 0 {
			allEmpty = false
			rawLast := ele[len(ele)-1]
			if primitive.CompareTimestamp(rawLast.Parsed.Timestamp, batcher.lastOplog.Parsed.Timestamp) > 0 {
				batcher.lastOplog = rawLast
			}
		}
	}
	return allEmpty
}

// addIntoBatchGroup
// isBarrier
//     Barrier Oplogs(like DDL or Transaction) must execute sequentially and separately, send to batchGroup[0]
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

	// LOG.Debug("add into worker[%v]: %v", which, genericLog.Parsed.ParsedLog)
}

func (batcher *Batcher) isTransaction(partialLog *oplog.PartialLog) (oplog.TxnMeta, bool) {
	//LOG.Info("isTransaction input oplog:%v lsid[%v] TxnNumber[%v] Object[%v]", partialLog,
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
	genericLog *oplog.GenericOplog) (isRet bool, mustIndividual bool, mustSerial bool,
	deliveredOps []*oplog.GenericOplog) {
	err := batcher.txnBuffer.AddOp(txnMeta, genericLog.Parsed.ParsedLog)
	if err != nil {
		LOG.Crashf("%s add oplog to txnbuffer failed, err[%v] oplog[%v]",
			batcher.syncer, err, genericLog.Parsed.ParsedLog)
	}

	// distributed transaction is abort, ignore these Oplogs and clear buffer
	if txnMeta.IsAbort() {
		err := batcher.txnBuffer.PurgeTxn(txnMeta)
		if err != nil {
			LOG.Crashf("%s cleaning up txnBuffer failed, err[%v] oplog[%v]",
				batcher.syncer, err, genericLog.Parsed.ParsedLog)
		}

		batcher.syncer.replMetric.AddFilter(1)
		batcher.lastFilterOplog = genericLog.Parsed
		return false, false, false, nil
	}

	if !txnMeta.IsCommit() {
		// transaction can not be commit
		return false, false, false, nil
	}

	haveCommandInTransaction := false
	mustIndividual = true
	mustSerial = false
	// transaction can be commit now
	ops, errs := batcher.txnBuffer.GetTxnStream(txnMeta)
Loop:
	for {
		select {
		case o, ok := <-ops:
			if !ok {
				break Loop
			}
			newOplog := &oplog.PartialLog{
				ParsedLog: o,
			}

			if newOplog.Operation == "c" {
				haveCommandInTransaction = true
			}

			// Raw will be filling in Send->LogEntryEncode
			deliveredOps = append(deliveredOps,
				&oplog.GenericOplog{
					Raw:    nil,
					Parsed: newOplog,
				})
		case err := <-errs:
			if err != nil {
				LOG.Crashf("error replaying transaction, err[%v]", err)
			}
			break Loop
		}
	}

	// Individual transaction that do not have commamnd can run run with other curd oplog
	if !txnMeta.IsCommitOp() && !haveCommandInTransaction &&
		genericLog.Parsed.PrevOpTime.String() == emptyPrevRaw.String() {
		mustIndividual = false
	}
	// transaction applyOps that do not have command can run parallelly
	if haveCommandInTransaction {
		mustSerial = true
	}

	err = batcher.txnBuffer.PurgeTxn(txnMeta)
	if err != nil {
		LOG.Crashf("error cleaning up transaction buffer, err[%v]", err)
	}

	return true, mustIndividual, mustSerial, deliveredOps
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}
