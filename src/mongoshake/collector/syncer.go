package collector

import (
	"fmt"
	"time"

	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"
	"mongoshake/quorum"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo/bson"
)

const (
	//FetcherBufferCapacity   = 32
	FetcherBufferCapacity   = 256
	AdaptiveBatchingMaxSize = 16384 // 16k

	// bson deserialize workload is CPU-intensive task
	PipelineQueueMaxNr = 4
	PipelineQueueMinNr = 1
	PipelineQueueLen   = 64

	DurationTime = 6000
)

type OplogHandler interface {
	// invocation on every oplog consumed
	Handle(log *oplog.PartialLog)
}

// OplogSyncer poll oplogs from original source MongoDB.
type OplogSyncer struct {
	OplogHandler

	// global replicate coordinator
	coordinator *ReplicationCoordinator
	// source mongodb replica set name
	replset string

	ckptManager *ckpt.CheckpointManager

	// oplog hash strategy
	hasher oplog.Hasher

	// pending queue. used by rawlog parsing. we buffered the
	// target raw oplogs in buffer and push them to pending queue
	// when buffer is filled in. and transfer to log queue
	buffer            []*bson.Raw
	pendingQueue      []chan []*bson.Raw
	logsQueue         []chan []*oplog.GenericOplog
	nextQueuePosition uint64

	// source mongo oplog reader
	reader *OplogReader
	// journal log that records all oplogs
	journal *utils.Journal
	// oplogs dispatcher
	batcher *Batcher

	// timers for inner event
	startTime time.Time
	ckptTime  time.Time

	replMetric *utils.ReplicationMetric
}

func NewOplogSyncer(
	coordinator *ReplicationCoordinator,
	replset string,
	mongoUrl string,
	gid string) *OplogSyncer {

	syncer := &OplogSyncer{
		coordinator: coordinator,
		replset:     replset,
		journal: utils.NewJournal(utils.JournalFileName(
			fmt.Sprintf("%s.%s", conf.Options.CollectorId, replset))),
		reader: NewOplogReader(mongoUrl),
	}

	// concurrent level hasher
	switch conf.Options.ShardKey {
	case oplog.ShardByNamespace:
		syncer.hasher = &oplog.TableHasher{}
	case oplog.ShardByID:
		syncer.hasher = &oplog.PrimaryKeyHasher{}
	}

	filterList := OplogFilterChain{new(AutologousFilter), new(NoopFilter)}
	if gid != "" {
		filterList = append(filterList, &GidFilter{Gid: gid})
	}
	if len(conf.Options.FilterNamespaceWhite) != 0 || len(conf.Options.FilterNamespaceBlack) != 0 {
		namespaceFilter := NewNamespaceFilter(conf.Options.FilterNamespaceWhite,
			conf.Options.FilterNamespaceBlack)
		filterList = append(filterList, namespaceFilter)
	}

	// oplog filters. drop the oplog if any of the filter
	// list returns true. The order of all filters is not significant
	syncer.batcher = &Batcher{
		syncer:      syncer,
		filterList:  filterList,
		handler:     syncer,
		workerGroup: []*Worker{}, // assign later by syncer.bind()
	}
	return syncer
}

func (sync *OplogSyncer) init() {
	sync.replMetric = utils.NewMetric(sync.replset, utils.METRIC_CKPT_TIMES|
		utils.METRIC_TUNNEL_TRAFFIC|utils.METRIC_LSN_CKPT|utils.METRIC_SUCCESS|
		utils.METRIC_TPS|utils.METRIC_RETRANSIMISSION)
	sync.replMetric.ReplStatus.Update(utils.WorkGood)

	sync.RestAPI()
}

func (sync *OplogSyncer) bind(w *Worker) {
	sync.batcher.workerGroup = append(sync.batcher.workerGroup, w)
}

func (sync *OplogSyncer) start() {
	LOG.Info("Poll oplog syncer start. ckpt_interval[%dms], gid[%s], shard_key[%s]",
		conf.Options.CheckpointInterval, conf.Options.OplogGIDS, conf.Options.ShardKey)

	sync.startTime = time.Now()

	// process about the checkpoint :
	//
	// 1. create checkpoint manager
	// 2. load existing ckpt from remote storage
	// 3. start checkpoint persist routine
	sync.newCheckpointManager(sync.replset)

	// start batcher and deserializer
	sync.startDeserializer()
	sync.startBatcher()

	// for ever fetching next oplog entry
	for {
		sync.poll()

		// error or exception occur
		LOG.Warn("Oplog syncer polling yield. master:%t, yield:%dms", quorum.IsMaster(), DurationTime)
		utils.YieldInMs(DurationTime)
	}
}

func (sync *OplogSyncer) startBatcher() {
	var batcher = sync.batcher

	nimo.GoRoutineInLoop(func() {
		// As much as we can batch more from logs queue. batcher can merge
		// a sort of oplogs from different logs queue one by one. the max number
		// of oplogs in batch is limited by AdaptiveBatchingMaxSize
		if worked := batcher.dispatchBatches(batcher.batchMore()); worked {
			sync.replMetric.SetLSN(utils.TimestampToInt64(batcher.getLastOplog().Timestamp))
			// update latest fetched timestamp in memory
			sync.reader.UpdateQueryTimestamp(batcher.getLastOplog().Timestamp)
		}

		// flush checkpoint value
		sync.checkpoint()
	})
}

func calculatePendingQueueConcurrency() int {
	// single {pending|logs}queue while it'is multi source shard
	if conf.Options.IsShardCluster() {
		return PipelineQueueMinNr
	}
	return PipelineQueueMaxNr
}

func (sync *OplogSyncer) startDeserializer() {
	parallel := calculatePendingQueueConcurrency()
	sync.pendingQueue = make([]chan []*bson.Raw, parallel, parallel)
	sync.logsQueue = make([]chan []*oplog.GenericOplog, parallel, parallel)
	for index := 0; index != len(sync.pendingQueue); index++ {
		sync.pendingQueue[index] = make(chan []*bson.Raw, PipelineQueueLen)
		sync.logsQueue[index] = make(chan []*oplog.GenericOplog, PipelineQueueLen)
		go sync.deserializer(index)
	}
}

func (sync *OplogSyncer) deserializer(index int) {
	for {
		batchRawLogs := <-sync.pendingQueue[index]
		nimo.AssertTrue(len(batchRawLogs) != 0, "pending queue batch logs has zero length")
		var deserializeLogs = make([]*oplog.GenericOplog, 0, len(batchRawLogs))

		for _, rawLog := range batchRawLogs {
			log := new(oplog.PartialLog)
			bson.Unmarshal(rawLog.Data, log)
			deserializeLogs = append(deserializeLogs, &oplog.GenericOplog{Raw: rawLog.Data, Parsed: log})
		}
		sync.logsQueue[index] <- deserializeLogs
	}
}

func (sync *OplogSyncer) poll() {
	// we should reload checkpoint. in case of other collector
	// has fetched oplogs when master quorum leader election
	//	happens frequently. so we simply reload.
	checkpoint := sync.ckptManager.Get()
	if checkpoint == nil {
		// we doesn't continue working on ckpt fetched failed. because we should
		// confirm the exist checkpoint value or exactly knows that it doesn't exist
		LOG.Critical("Acquire the existing checkpoint from remote[%s] failed !", conf.Options.ContextAddress)
		return
	}
	sync.reader.SetQueryTimestampOnEmpty(checkpoint.Timestamp)

	// every syncer should under the control of global rate limiter
	rc := sync.coordinator.rateController

	for quorum.IsMaster() {
		// SimpleRateController is too simple. the TPS flow may represent
		// low -> high -> low.... and centralize to point time in somewhere
		// However. not smooth is make sense in stream processing. This was
		// more effected in request processing programing
		//
		//				    _             _
		//		    	   / |           / |             <- peak
		//			     /   |         /   |
		//   _____/    |____/    |___    <-  controlled
		//
		//
		// WARNING : in current version. we throttle the replicate tps in Receiver
		// rather than limiting in Collector. since the real replication traffic happened
		// in Receiver executor. Apparently it tends to change {SentinelOptions} in
		// Receiver. The follows were kept for compatibility
		if utils.SentinelOptions.TPS != 0 && rc.Control(utils.SentinelOptions.TPS, 1) {
			utils.DelayFor(100)
			continue
		}

		// only get one
		sync.next()
	}
}

func (sync *OplogSyncer) next() bool {
	var log *bson.Raw
	var err error
	if log, err = sync.reader.Next(); log != nil {
		payload := int64(len(log.Data))
		sync.replMetric.AddGet(1)
		sync.replMetric.SetOplogMax(payload)
		sync.replMetric.SetOplogAvg(payload)
		sync.replMetric.ReplStatus.Clear(utils.FetchBad)
	}

	if err != nil && err != TimeoutError {
		LOG.Warn("Oplog syncer internal error : %s", err.Error())
		// error is nil indicate that only timeout incur syncer.next()
		// return false. so we regardless that
		sync.replMetric.ReplStatus.Update(utils.FetchBad)
		utils.YieldInMs(DurationTime)

		// alarm
	}

	// buffered oplog or trigger to flush. log is nil
	// means that we need to flush buffer right now
	return sync.transfer(log)
}

func (sync *OplogSyncer) transfer(log *bson.Raw) bool {
	flush := false
	if log != nil {
		sync.buffer = append(sync.buffer, log)
	} else {
		flush = true
	}

	if len(sync.buffer) >= FetcherBufferCapacity || (flush && len(sync.buffer) != 0) {
		// we could simply ++syncer.resolverIndex. The max uint64 is 9223372036854774807
		// and discard the skip situation. we assume nextQueueCursor couldn't be overflow
		selected := int(sync.nextQueuePosition % uint64(len(sync.pendingQueue)))
		sync.pendingQueue[selected] <- sync.buffer
		sync.buffer = make([]*bson.Raw, 0, FetcherBufferCapacity)

		sync.nextQueuePosition++
		return true
	}
	return false
}

func (sync *OplogSyncer) Handle(log *oplog.PartialLog) {
	// 1. records audit log if need
	sync.journal.WriteRecord(log)
}

func (sync *OplogSyncer) RestAPI() {
	type Time struct {
		TimestampUnix int64  `json:"unix"`
		TimestampTime string `json:"time"`
	}
	type MongoTime struct {
		Time
		TimestampMongo string `json:"ts"`
	}

	type Info struct {
		Who         string     `json:"who"`
		Tag         string     `json:"tag"`
		ReplicaSet  string     `json:"replset"`
		Logs        uint64     `json:"logs_get"`
		LogsRepl    uint64     `json:"logs_repl"`
		LogsSuccess uint64     `json:"logs_success"`
		Lsn         *MongoTime `json:"lsn"`
		LsnAck      *MongoTime `json:"lsn_ack"`
		LsnCkpt     *MongoTime `json:"lsn_ckpt"`
		Now         *Time      `json:"now"`
	}

	utils.HttpApi.RegisterAPI("/repl", nimo.HttpGet, func([]byte) interface{} {
		return &Info{
			Who:         conf.Options.CollectorId,
			Tag:         utils.VERSION,
			ReplicaSet:  sync.replset,
			Logs:        sync.replMetric.Get(),
			LogsRepl:    sync.replMetric.Apply(),
			LogsSuccess: sync.replMetric.Success(),
			Lsn: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSN),
				Time: Time{TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSN),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSN))}},
			LsnCkpt: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSNCheckpoint),
				Time: Time{TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSNCheckpoint),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSNCheckpoint))}},
			LsnAck: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSNAck),
				Time: Time{TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSNAck),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSNAck))}},
			Now: &Time{TimestampUnix: time.Now().Unix(), TimestampTime: utils.TimestampToString(time.Now().Unix())},
		}
	})
}

type Batcher struct {
	// related oplog syncer. not owned
	syncer *OplogSyncer

	// filter functionality by gid
	filterList OplogFilterChain
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
	for len(mergeBatch) < AdaptiveBatchingMaxSize &&
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
