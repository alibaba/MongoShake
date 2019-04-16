package executor

import (
	"fmt"
	"sync"
	"sync/atomic"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo"
)

const (
	DumpConflictToDB  = "db"
	DumpConflictToSDK = "sdk"
	NoDumpConflict    = "none"

	ExecuteOrdered = false

	OpInsert = 0x01
	OpUpdate = 0x02

	OplogsMaxGroupNum  = 1000
	OplogsMaxGroupSize = 12 * 1024 * 1024 // MongoDB limits 16MB
)

var (
	GlobalExecutorId int32 = -1
	ThresholdVersion string = "3.2.0"
)

type PartialLogWithCallbak struct {
	partialLog *oplog.PartialLog
	callback   func()
}

type BatchGroupExecutor struct {
	// multi executor
	executors []*Executor
	// worker id
	ReplayerId uint32
	// mongo url
	MongoUrl string
}

func (batchExecutor *BatchGroupExecutor) Start() {
	// max concurrent execute connection sets to 64. And the total
	// conns = number of executor * number of batchExecutor. Normally max
	// is 64. if collector hashed oplogRecords by _id and the number of collector
	// is bigger we will use single executer in respective batchExecutor
	parallel := conf.Options.ReplayerExecutor
	executors := make([]*Executor, parallel)
	for i := 0; i != len(executors); i++ {
		executors[i] = NewExecutor(GenerateExecutorId(), batchExecutor, batchExecutor.MongoUrl)
		go executors[i].start()
	}
	batchExecutor.executors = executors
}

func (batchExecutor *BatchGroupExecutor) Sync(rawLogs []*oplog.PartialLog, callback func()) {
	count := uint64(len(rawLogs))
	if count == 0 {
		// may be probe request
		return
	}

	logs := make([]*PartialLogWithCallbak, len(rawLogs), len(rawLogs))
	// populate the batch buffer first
	for i, rawLog := range rawLogs {
		logs[i] = &PartialLogWithCallbak{partialLog: rawLog}
	}
	// only the last oplog message would be notified
	logs[len(logs)-1].callback = callback

	batchExecutor.replay(logs)
}

func (batchExecutor *BatchGroupExecutor) replay(logs []*PartialLogWithCallbak) {
	// TODO: skip the oplogRecords which has been replayed
	//  lastTs := utils.TimestampToInt64(logs[len(logs)-1].partialLog.Timestamp)
	//	if batchExecutor.replayer.Ack >= lastTs {
	//		// every oplog in buffer have been alread executed in previously
	//		// so discard them simply. Even the smaller timestamp oplogRecords has
	//		// been changed(other collector or other mongos source)
	//		return
	//	}

	// executor needs to check pausing or throttle here.
	batchExecutor.replicateShouldStall(int64(len(logs)))

	// In mongo shard cluster. our request goes into mongos. it's safe for
	// unique index without collision detection
	var matrix CollisionMatrix = &NoopMatrix{}
	if conf.Options.ReplayerCollisionEnable {
		matrix = NewBarrierMatrix()
	}

	// firstly. we split the oplogRecords into segments which are the unit
	// of safety execution. it means there is no any operations
	// on the safe unique index in the single segment.
	var segments = matrix.split(logs)
	// secondly. in each segment, we analyze the dependence between
	// each oplogRecords. And
	for _, segment := range segments {
		toBeExecuted := matrix.convert(segment)
		batchExecutor.executeInParallel(toBeExecuted)
	}
}

// TODO
func (batchExecutor *BatchGroupExecutor) replicateShouldStall(n int64) {
}

func (batchExecutor *BatchGroupExecutor) executeInParallel(logs []*OplogRecord) {
	// prepare execution monitor
	latch := new(sync.WaitGroup)
	latch.Add(len(logs))
	// shard oplogRecords by _id primary key and make up callback chain
	var buffer = make([][]*OplogRecord, len(batchExecutor.executors))
	shardKey := oplog.PrimaryKeyHasher{}
	var completionList []func()
	for _, log := range logs {
		selected := shardKey.DistributeOplogByMod(log.original.partialLog, len(batchExecutor.executors))
		buffer[selected] = append(buffer[selected], log)
		if log.original.callback != nil {
			// should be ordered by the incoming sequence
			completionList = append(completionList, log.original.callback)
		}
	}
	for index, buf := range buffer {
		if len(buf) != 0 {
			nimo.AssertTrue(len(batchExecutor.executors[index].batchBlock) == 0, "executors buffer is not empty!")
			nimo.AssertTrue(batchExecutor.executors[index].finisher == nil, "executors await status is wrong!")
			batchExecutor.executors[index].finisher = latch
			// follow the MEMORY MODEL : finisher should be assigned
			// before batchBlock channel. it read after <- batchBlock
			batchExecutor.executors[index].batchBlock <- buf
		}
	}
	// wait for execute completely
	latch.Wait()
	// invoke all callbacks
	for _, callback := range completionList {
		callback()
	}
	// sweep executors' block buffer and await
	for _, exec := range batchExecutor.executors {
		exec.finisher = nil
	}
}

type Executor struct {
	// sequence index id in each replayer
	id int
	// batchExecutor, not owned
	batchExecutor *BatchGroupExecutor
	// records all oplogRecords into journal files
	journal *utils.Journal
	// mongo url
	MongoUrl string

	batchBlock chan []*OplogRecord
	finisher   *sync.WaitGroup

	// mongo connection
	session *mgo.Session

	// bulk insert or single insert
	bulkInsert bool
}

func GenerateExecutorId() int {
	return int(atomic.AddInt32(&GlobalExecutorId, 1))
}

func NewExecutor(id int, batchExecutor *BatchGroupExecutor, MongoUrl string) *Executor {
	return &Executor{
		id:            id,
		batchExecutor: batchExecutor,
		journal:       utils.NewJournal(utils.JournalFileName(fmt.Sprintf("direct.%03d", id))),
		MongoUrl:      MongoUrl,
		batchBlock:    make(chan []*OplogRecord, 1),
	}
}

func (exec *Executor) start() {
	for toBeExecuted := range exec.batchBlock {
		nimo.AssertTrue(len(toBeExecuted) != 0, "the size of being executed batch oplogRecords could not be zero")
		for exec.doSync(toBeExecuted) != nil {
		}
		// acknowledge all oplogRecords have been successfully executed
		exec.finisher.Add(-len(toBeExecuted))

		// records all oplogRecords if enabled. After write successfully
		for _, log := range toBeExecuted {
			exec.journal.WriteRecord(log.original.partialLog)
		}
	}
}

func (exec *Executor) doSync(logs []*OplogRecord) error {
	count := len(logs)

	// split batched oplogRecords into (ns, op) groups. individual group
	// can be accomplished in single MongoDB request. groups
	// in this executor will be sequential
	oplogGroups := LogsGroupCombiner{maxGroupNr: OplogsMaxGroupNum,
		maxGroupSize: OplogsMaxGroupSize}.mergeToGroups(logs)
	for _, group := range oplogGroups {
		if err := exec.execute(group); err != nil {
			return err
		}
	}

	LOG.Info("Replayer-%d Executor-%d doSync oplogRecords received[%d] merged[%d]. merge to %.2f%% chunks",
		exec.batchExecutor.ReplayerId, exec.id, count, len(oplogGroups), float32(len(oplogGroups))*100.00/float32(count))
	return nil
}

type OplogsGroup struct {
	ns           string
	op           string
	oplogRecords []*OplogRecord

	completionList []func()
}

func (group *OplogsGroup) completion() {
	for _, cb := range group.completionList {
		cb()
	}
}

type LogsGroupCombiner struct {
	maxGroupNr   int
	maxGroupSize int
}

func (combiner LogsGroupCombiner) mergeToGroups(logs []*OplogRecord) (groups []*OplogsGroup) {
	forceSplit := false
	sizeInGroup := 0

	for _, log := range logs {
		op := log.original.partialLog.Operation
		ns := log.original.partialLog.Namespace
		// the equivalent oplog.op and oplog.ns can be merged together
		last := len(groups) - 1

		if !forceSplit && // force split by log's wait function
				len(groups) > 0 && // have one group existing at least
				len(groups[last].oplogRecords) < combiner.maxGroupNr && // no more than max group number
				sizeInGroup + log.original.partialLog.RawSize < combiner.maxGroupSize && // no more than one group size
				groups[last].op == op && groups[last].ns == ns { // same op and ns
			// we can merge this oplog into the latest batched oplogRecords group
			combiner.merge(groups[len(groups)-1], log)
			sizeInGroup += log.original.partialLog.RawSize // add size
		} else {
			// new start of a group
			groups = append(groups, combiner.startNewGroup(log))
			sizeInGroup = log.original.partialLog.RawSize
		}

		// can't be merge more oplogRecords further. this log should be the end in this group
		forceSplit = log.wait != nil
	}
	return
}

func (combiner *LogsGroupCombiner) merge(group *OplogsGroup, log *OplogRecord) {
	group.oplogRecords = append(group.oplogRecords, log)
	if log.original.callback != nil {
		group.completionList = append(group.completionList, log.original.callback)
	}
}

func (combiner *LogsGroupCombiner) startNewGroup(log *OplogRecord) *OplogsGroup {
	group := &OplogsGroup{
		op:           log.original.partialLog.Operation,
		ns:           log.original.partialLog.Namespace,
		oplogRecords: []*OplogRecord{log},
	}
	if log.original.callback == nil {
		group.completionList = []func(){}
	} else {
		group.completionList = []func(){log.original.callback}
	}
	return group
}
