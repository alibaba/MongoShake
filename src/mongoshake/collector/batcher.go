package collector

import (
	"fmt"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/oplog"
	"strings"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"mongoshake/common"
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
func (batcher *Batcher) getLastOplog() *oplog.PartialLog {
	return batcher.lastOplog
}

func (batcher *Batcher) filter(log *oplog.PartialLog) bool {
	// filter oplog suchlike Noop or Gid-filtered
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

// return batched oplogs and barrier flag
func (batcher *Batcher) batchMore() ([][]*oplog.GenericOplog, bool) {
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
	for i, genericLog := range mergeBatch {
		// filter oplog such like Noop or Gid-filtered
		if filterPartialLog(genericLog.Parsed, batcher) {
			// doesn't push to worker
			continue
		}

		// current is ddl and barrier == false
		if !conf.Options.ReplayerDMLOnly && ddlFilter.Filter(genericLog.Parsed) && !barrier {
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
	return batchGroup, barrier
}

func (batcher *Batcher) moveToNextQueue() {
	batcher.nextQueue++
	batcher.nextQueue = batcher.nextQueue % uint64(len(batcher.syncer.logsQueue))
}

func (batcher *Batcher) currentQueue() uint64 {
	return batcher.nextQueue
}


func filterPartialLog(partialLog *oplog.PartialLog, batcher *Batcher) bool {
	var result bool
	db := strings.SplitN(partialLog.Namespace, ".", 2)[0]
	if partialLog.Operation != "c" {
		// {"op" : "i", "ns" : "my.system.indexes", "o" : { "v" : 2, "key" : { "date" : 1 }, "name" : "date_1", "ns" : "my.tbl", "expireAfterSeconds" : 3600 }
		if strings.HasSuffix(partialLog.Namespace, "system.indexes") {
			// change partialLog.Namespace to ns of object, in order to do filter with real namespace
			ns := partialLog.Namespace
			partialLog.Namespace = oplog.GetKey(partialLog.Object, "ns").(string)
			result = batcher.filter(partialLog)
			partialLog.Namespace = ns
		} else {
			result = batcher.filter(partialLog)
		}
		return result
	} else {
		operation, found := oplog.ExtraCommandName(partialLog.Object)
		if !found {
			LOG.Warn("extraCommandName meets type[%s] which is not implemented, ignore!", operation)
			return false
		}
		switch operation {
		case "create":
			fallthrough
		case "createIndexes":
			fallthrough
		case "collMod":
			fallthrough
		case "drop":
			fallthrough
		case "deleteIndex":
			fallthrough
		case "deleteIndexes":
			fallthrough
		case "dropIndex":
			fallthrough
		case "dropIndexes":
			fallthrough
		case "convertToCapped":
			fallthrough
		case "emptycapped":
			col, ok := oplog.GetKey(partialLog.Object, operation).(string)
			if !ok {
				LOG.Warn("extraCommandName meets illegal %v oplog %v, ignore!", operation, partialLog.Object)
				return false
			}
			partialLog.Namespace = fmt.Sprintf("%s.%s", db, col)
			return batcher.filter(partialLog)
		case "renameCollection":
			// { "renameCollection" : "my.tbl", "to" : "my.my", "stayTemp" : false, "dropTarget" : false }
			ns, ok := oplog.GetKey(partialLog.Object, operation).(string)
			if !ok {
				LOG.Warn("extraCommandName meets illegal %v oplog %v, ignore!", operation, partialLog.Object)
				return false
			}
			partialLog.Namespace = ns
			return batcher.filter(partialLog)
		case "applyOps":
			var filterOps []bson.D
			if ops := oplog.GetKey(partialLog.Object, "applyOps").([]bson.D); ops != nil {
				for _, ele := range ops {
					m, _ := oplog.ConvertBsonD2M(ele)
					subLog := oplog.NewPartialLog(m)
					if ok := filterPartialLog(subLog, batcher); !ok {
						filterOps = append(filterOps, ele)
					}
				}
				oplog.SetFiled(partialLog.Object, "applyOps", filterOps)
			}
			return len(filterOps) > 0
		default:
			// such as: dropDatabase
			return batcher.filter(partialLog)
		}
	}
}
