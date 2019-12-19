package collector

import (
	"fmt"
	"mongoshake/collector/oplogsyncer"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/common"
	"mongoshake/oplog"
	"mongoshake/quorum"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	// FetcherBufferCapacity   = 256
	// AdaptiveBatchingMaxSize = 16384 // 16k

	// bson deserialize workload is CPU-intensive task
	PipelineQueueMaxNr = 4
	PipelineQueueMinNr = 1
	PipelineQueueLen   = 64

	WaitBarrierAckLogTimes = 10

	DurationTime        = 6000 // unit: ms.
	DDLCheckpointGap    = 5    // unit: seconds.
	FilterCheckpointGap = 180  // unit: seconds. no checkpoint update, flush checkpoint mandatory

	ShardingWorkerId = 0
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
	// full sync finish position, used to check DDL between full sync and incr sync
	fullSyncFinishPosition int64

	ckptManager *CheckpointManager
	mvckManager *MoveChunkManager
	ddlManager  *DDLManager

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
	reader *oplogsyncer.OplogReader
	// journal log that records all oplogs
	journal *utils.Journal
	// oplogs dispatcher
	batcher *Batcher

	replMetric *utils.ReplicationMetric
}

/*
 * Syncer is used to fetch oplog from source MongoDB and then send to different workers which can be seen as
 * a network sender. There are several syncer coexist to improve the fetching performance.
 * The data flow in syncer is:
 * source mongodb --> reader --> pending queue(raw data) --> logs queue(parsed data) --> worker
 * The reason we split pending queue and logs queue is to improve the performance.
 */
func NewOplogSyncer(
	coordinator *ReplicationCoordinator,
	replset string,
	fullSyncFinishPosition int64,
	mongoUrl string,
	gids []string,
	ckptManager *CheckpointManager,
	mvckManager *MoveChunkManager,
	ddlManager *DDLManager) *OplogSyncer {
	syncer := &OplogSyncer{
		coordinator:            coordinator,
		replset:                replset,
		fullSyncFinishPosition: fullSyncFinishPosition,
		journal: utils.NewJournal(utils.JournalFileName(
			fmt.Sprintf("%s.%s", conf.Options.CollectorId, replset))),
		reader:      oplogsyncer.NewOplogReader(mongoUrl, replset),
		ckptManager: ckptManager,
		mvckManager: mvckManager,
		ddlManager:  ddlManager,
	}

	// concurrent level hasher
	switch conf.Options.ShardKey {
	case oplog.ShardByNamespace:
		syncer.hasher = &oplog.TableHasher{}
	case oplog.ShardByID:
		syncer.hasher = &oplog.PrimaryKeyHasher{}
	}

	filterList := filter.OplogFilterChain{filter.NewAutologousFilter(), filter.NewGidFilter(gids)}

	// DDL filter
	if conf.Options.ReplayerDMLOnly {
		filterList = append(filterList, new(filter.DDLFilter))
	}
	// namespace filter, heavy operation
	if len(conf.Options.FilterNamespaceWhite) != 0 || len(conf.Options.FilterNamespaceBlack) != 0 {
		namespaceFilter := filter.NewNamespaceFilter(conf.Options.FilterNamespaceWhite,
			conf.Options.FilterNamespaceBlack)
		filterList = append(filterList, namespaceFilter)
	}

	// oplog filters. drop the oplog if any of the filter
	// list returns true. The order of all filters is not significant.
	// workerGroup is assigned later by syncer.bind()
	syncer.batcher = NewBatcher(syncer, filterList, syncer, []*Worker{})
	return syncer
}

func (sync *OplogSyncer) init() {
	sync.replMetric = utils.NewMetric(sync.replset, utils.METRIC_CKPT_TIMES|
		utils.METRIC_TUNNEL_TRAFFIC|utils.METRIC_LSN_CKPT|utils.METRIC_SUCCESS|
		utils.METRIC_TPS|utils.METRIC_RETRANSIMISSION)
	sync.replMetric.ReplStatus.Update(utils.WorkGood)

	sync.RestAPI()
}

// bind different worker
func (sync *OplogSyncer) bind(w *Worker) {
	sync.batcher.workerGroup = append(sync.batcher.workerGroup, w)
}

// start to polling oplog
func (sync *OplogSyncer) start() {
	LOG.Info("Poll oplog syncer start. ckpt_interval[%dms], gid[%s], shard_key[%s]",
		conf.Options.CheckpointInterval, conf.Options.OplogGIDS, conf.Options.ShardKey)

	// process about the checkpoint :
	//
	// 1. create checkpoint manager
	// 2. load existing ckpt from remote storage
	// 3. start checkpoint persist routine

	// start deserializer: parse data from pending queue, and then push into logs queue.
	sync.startDeserializer()
	// start batcher: pull oplog from logs queue and then batch together before adding into worker.
	sync.startBatcher()

	// forever fetching oplog from mongodb into oplog_reader
	for {
		sync.poll()
		// error or exception occur
		LOG.Warn("Oplog syncer polling yield. master:%t, yield:%dms", quorum.IsMaster(), DurationTime)
		utils.YieldInMs(DurationTime)
	}
}

// fetch all oplog from logs queue, batched together and then send to different workers.
func (sync *OplogSyncer) startBatcher() {
	var batcher = sync.batcher
	barrier := false
	nimo.GoRoutineInLoop(func() {
		// As much as we can batch more from logs queue. batcher can merge
		// a sort of oplogs from different logs queue one by one. the max number
		// of oplogs in batch is limited by AdaptiveBatchingMaxSize
		nextBatch := batcher.Next()

		// avoid to do checkpoint when syncer update ackTs or syncTs
		sync.ckptManager.mutex.RLock()
		filteredNextBatch, nextBarrier, flushCheckpoint, lastOplog := batcher.filterAndBlockMoveChunk(nextBatch, barrier)
		barrier = nextBarrier

		if lastOplog != nil {
			needDispatch := true
			needUnBlock := false
			// DDL operate at sharded collection of mongodb sharding
			if DDLSupportForSharding() && ddlFilter.Filter(lastOplog) {
				needDispatch = sync.ddlManager.BlockDDL(sync.replset, lastOplog)
				if needDispatch {
					// ddl need to run, when not all but majority oplog syncer received ddl oplog
					LOG.Info("Oplog syncer %v prepare to dispatch ddl log %v", sync.replset, lastOplog)
					// transform ddl to run at mongos of dest sharding
					// number of worker of sharding instance and number of ddl command must be 1
					shardColSpec := utils.GetShardCollectionSpec(sync.ddlManager.FromCsConn.Session, lastOplog)
					if shardColSpec != nil {
						logRaw := filteredNextBatch[0].Raw
						filteredNextBatch = []*oplog.GenericOplog{}
						transOplogs := TransformDDL(sync.replset, lastOplog, shardColSpec, sync.ddlManager.ToIsSharding)
						for _, tlog := range transOplogs {
							filteredNextBatch = append(filteredNextBatch, &oplog.GenericOplog{Raw: logRaw, Parsed: tlog})
						}
					}
					needUnBlock = true
				}
			}
			if needDispatch {
				// push to worker to run
				if worked := batcher.dispatchBatch(filteredNextBatch); worked {
					sync.replMetric.SetLSN(utils.TimestampToInt64(lastOplog.Timestamp))
					// update latest fetched timestamp in memory
					sync.reader.UpdateQueryTimestamp(lastOplog.Timestamp)
				}
				if barrier {
					// wait for ddl operation finish, and flush checkpoint value
					sync.waitAllAck(flushCheckpoint)
					if needUnBlock {
						LOG.Info("Oplog syncer %v Unblock at ddl log %v", sync.replset, lastOplog)
						// unblock other shard nodes when sharding ddl has finished
						sync.ddlManager.UnBlockDDL(sync.replset, lastOplog)
					}
				}
			}
		} else {
			readerQueryTs := int64(sync.reader.GetQueryTimestamp())
			syncTs := sync.batcher.syncTs
			if utils.ExtractTs32(syncTs)-readerQueryTs >= FilterCheckpointGap {
				sync.waitAllAck(false)
				LOG.Info("oplog syncer %v force to update checkpointTs from %v to %v",
					sync.replset, utils.TimestampToLog(readerQueryTs), utils.TimestampToLog(syncTs))
				// update latest fetched timestamp in memory
				sync.reader.UpdateQueryTimestamp(syncTs)
			}
		}
		// update syncTs of batcher
		sync.batcher.syncTs = sync.batcher.unsyncTs
		sync.ckptManager.mutex.RUnlock()
	})
}

func (sync *OplogSyncer) waitAllAck(flushCheckpoint bool) {
	beginTs := time.Now()
	if flushCheckpoint {
		LOG.Info("oplog syncer %v prepare for checkpoint", sync.replset)
		sync.ckptManager.FlushChan <- true
	}
	sync.batcher.WaitAllAck()
	if flushCheckpoint && time.Now().After(beginTs.Add(DDLCheckpointGap*time.Second)) {
		LOG.Info("oplog syncer %v prepare for checkpoint.", sync.replset)
		sync.ckptManager.FlushChan <- true
	}
}

// how many pending queue we create
func calculatePendingQueueConcurrency() int {
	// single {pending|logs}queue while it'is multi source shard
	if conf.Options.IsShardCluster() {
		return PipelineQueueMinNr
	}
	return PipelineQueueMaxNr
}

// deserializer: fetch oplog from pending queue, parsed and then add into logs queue.
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
			log.RawSize = len(rawLog.Data)
			deserializeLogs = append(deserializeLogs, &oplog.GenericOplog{Raw: rawLog.Data, Parsed: log})
		}
		sync.logsQueue[index] <- deserializeLogs
	}
}

// only master(maybe several mongo-shake starts) can poll oplog.
func (sync *OplogSyncer) poll() {
	// we should reload checkpoint. in case of other collector
	//	// has fetched oplogs when master quorum leader election
	//	// happens frequently. so we simply reload.
	checkpointTs := sync.ckptManager.Get(sync.replset)
	if checkpointTs == 0 {
		// we doesn't continue working on ckpt fetched failed. because we should
		// confirm the exist checkpoint value or exactly knows that it doesn't exist
		LOG.Critical("Acquire the existing checkpoint from remote[%s] failed !", conf.Options.ContextStorageCollection)
		return
	}
	sync.reader.SetQueryTimestampOnEmpty(checkpointTs)
	sync.reader.StartFetcher() // start reader fetcher if not exist

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

// fetch oplog from reader.
func (sync *OplogSyncer) next() bool {
	var log *bson.Raw
	var err error
	if log, err = sync.reader.Next(); log != nil {
		payload := int64(len(log.Data))
		sync.replMetric.AddGet(1)
		sync.replMetric.SetOplogMax(payload)
		sync.replMetric.SetOplogAvg(payload)
		sync.replMetric.ReplStatus.Clear(utils.FetchBad)
	} else if err != nil && err != oplogsyncer.TimeoutError {
		LOG.Error("oplog syncer internal error: %v", err)
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

	if len(sync.buffer) >= conf.Options.FetcherBufferCapacity || (flush && len(sync.buffer) != 0) {
		// we could simply ++syncer.resolverIndex. The max uint64 is 9223372036854774807
		// and discard the skip situation. we assume nextQueueCursor couldn't be overflow
		selected := int(sync.nextQueuePosition % uint64(len(sync.pendingQueue)))
		sync.pendingQueue[selected] <- sync.buffer
		sync.buffer = make([]*bson.Raw, 0, conf.Options.FetcherBufferCapacity)

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
		Tps         uint64     `json:"tps"`
		Lsn         *MongoTime `json:"lsn"`
		LsnAck      *MongoTime `json:"lsn_ack"`
		LsnCkpt     *MongoTime `json:"lsn_ckpt"`
		Now         *Time      `json:"now"`
	}

	utils.HttpApi.RegisterAPI("/repl", nimo.HttpGet, func([]byte) interface{} {
		return &Info{
			Who:         conf.Options.CollectorId,
			Tag:         utils.BRANCH,
			ReplicaSet:  sync.replset,
			Logs:        sync.replMetric.Get(),
			LogsRepl:    sync.replMetric.Apply(),
			LogsSuccess: sync.replMetric.Success(),
			Tps:         sync.replMetric.Tps(),
			Lsn: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSN),
				Time: Time{TimestampUnix: utils.ExtractTs32(sync.replMetric.LSN),
					TimestampTime: utils.TimestampToString(utils.ExtractTs32(sync.replMetric.LSN))}},
			LsnCkpt: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSNCheckpoint),
				Time: Time{TimestampUnix: utils.ExtractTs32(sync.replMetric.LSNCheckpoint),
					TimestampTime: utils.TimestampToString(utils.ExtractTs32(sync.replMetric.LSNCheckpoint))}},
			LsnAck: &MongoTime{TimestampMongo: utils.Int64ToString(sync.replMetric.LSNAck),
				Time: Time{TimestampUnix: utils.ExtractTs32(sync.replMetric.LSNAck),
					TimestampTime: utils.TimestampToString(utils.ExtractTs32(sync.replMetric.LSNAck))}},
			Now: &Time{TimestampUnix: time.Now().Unix(), TimestampTime: utils.TimestampToString(time.Now().Unix())},
		}
	})
}
