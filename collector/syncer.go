package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	utils "github.com/alibaba/MongoShake/v2/common"
	journal "github.com/alibaba/MongoShake/v2/journal"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/quorum"
)

const (
	// FetcherBufferCapacity   = 256
	// AdaptiveBatchingMaxSize = 16384 // 16k

	// bson deserialize workload is CPU-intensive task
	PipelineQueueMaxNr    = utils.VarSyncerPipelineQueueMaxNr
	PipelineQueueMiddleNr = utils.VarSyncerPipelineQueueMiddleNr
	PipelineQueueMinNr    = utils.VarSyncerPipelineQueueMinNr
	PipelineQueueLen      = utils.VarSyncerPipelineQueueLen

	SyncerFetchErrorRetryMs       = utils.VarSyncerFetchErrorRetryMs
	FilterCheckpointGap           = utils.VarSyncerFilterCheckpointGap
	FilterCheckpointCheckInterval = utils.VarSyncerFilterCheckpointCheckInterval
	CheckCheckpointUpdateTimes    = utils.VarSyncerCheckCheckpointUpdateTimes
)

var DDLCheckpointInterval int64 = utils.VarSyncerDDLCheckpointIntervalMs

var barrierCkptGetFunc = func(sync *OplogSyncer) (*ckpt.CheckpointContext, error) {
	checkpoint, _, err := sync.ckptManager.Get()
	if err != nil {
		return nil, err
	}
	return checkpoint, nil
}

var barrierCkptFlushFunc = func(sync *OplogSyncer) bool {
	return sync.checkpoint(true, 0)
}

type OplogHandler interface {
	// Handle is called on every oplog consumed
	Handle(log *oplog.PartialLog)
}

// OplogSyncer poll oplogs from original source MongoDB.
type OplogSyncer struct {
	OplogHandler

	// source mongodb replica set name
	Replset string
	// oplog start position of source mongodb
	startPosition interface{}
	// full sync finish position, used to check DDL between full sync and incr sync
	fullSyncFinishPosition primitive.Timestamp

	ckptManager *ckpt.CheckpointManager

	// oplog hash strategy
	hasher oplog.Hasher

	// pending queue. used by raw log parsing. we buffered the
	// target raw oplogs in buffer and push them to pending queue
	// when buffer is filled in. and transfer to log queue
	// buffer            []*bson.Raw // move to persister
	PendingQueue []chan [][]byte
	logsQueue    []chan []*oplog.GenericOplog
	LastFetchTs  primitive.Timestamp // the previous last fetch timestamp
	// nextQueuePosition uint64 // move to persister

	// source mongo oplog/event reader
	reader sourceReader.Reader
	// journal log that records all oplogs
	journal *journal.Journal
	// oplogs dispatcher
	batcher *Batcher
	// data persist handler
	persister *Persister

	// qos
	qos *utils.Qos

	// timers for inner event
	startTime time.Time
	ckptTime  time.Time

	replMetric *utils.ReplicationMetric

	// can be closed
	CanClose        bool
	SyncGroup       []*OplogSyncer
	shutdownWorking bool // shutdown routine starts?

	// fetcher health state: "normal", "degraded", "error"
	fetcherState string
}

// NewOplogSyncer return a new OplogSyncer.
// OplogSyncer is used to fetch oplog from source MongoDB and then send to different workers which can be seen as
// a network sender. There are several syncer coexist to improve the fetching performance.
// The data flow in syncer is:
// source mongodb --> reader --> persister --> pending queue(raw data) --> logs queue(parsed data) --> worker
// The reason why we split pending queue and logs queue is to improve the performance.
func NewOplogSyncer(
	replset string,
	startPosition interface{},
	fullSyncFinishPosition int64,
	mongoUrl string,
	gids []string) *OplogSyncer {

	reader, err := sourceReader.CreateReader(conf.Options.IncrSyncMongoFetchMethod, mongoUrl, replset)
	if err != nil {
		l.Logger.Criticalf("create reader with url[%v] replset[%v] failed[%v]", mongoUrl, replset, err)
		return nil
	}

	syncer := &OplogSyncer{
		Replset:                replset,
		startPosition:          startPosition,
		fullSyncFinishPosition: utils.Int64ToTimestamp(fullSyncFinishPosition),
		journal: journal.NewJournal(journal.FileName(
			fmt.Sprintf("%s.%s", conf.Options.Id, replset))),
		reader:       reader,
		qos:          utils.StartQoS(0, 1, &utils.IncrSentinelOptions.TPS), // default is 0 which means do not limit
		fetcherState: "normal",
	}

	// concurrent level hasher
	switch conf.Options.IncrSyncShardKey {
	case oplog.ShardByNamespace:
		syncer.hasher = &oplog.TableHasher{}
	case oplog.ShardByID:
		syncer.hasher = &oplog.PrimaryKeyHasher{}
	}
	if len(conf.Options.IncrSyncShardByObjectIdWhiteList) != 0 {
		syncer.hasher = oplog.NewWhiteListObjectIdHasher(conf.Options.IncrSyncShardByObjectIdWhiteList)
	}

	filterList := filter.OplogFilterChain{
		new(filter.AutologousFilter),
		new(filter.NoopFilter),
		filter.NewOpTypeFilter(conf.Options.FilterOpTypes),
		filter.NewGidFilter(gids),
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

	// init persist
	syncer.persister = NewPersister(replset, syncer)

	return syncer
}

func (sync *OplogSyncer) Init() {
	var options uint64 = utils.METRIC_CKPT_TIMES | utils.METRIC_LSN | utils.METRIC_SUCCESS |
		utils.METRIC_TPS | utils.METRIC_FILTER
	if conf.Options.Tunnel != utils.VarTunnelDirect {
		options |= utils.METRIC_RETRANSIMISSION
		options |= utils.METRIC_TUNNEL_TRAFFIC
		options |= utils.METRIC_WORKER
	}

	sync.replMetric = utils.NewMetric(sync.Replset, utils.TypeIncr, options)
	sync.replMetric.SetReplStatus(utils.WorkGood)

	sync.RestAPI()
	sync.persister.RestAPI()
}

func (sync *OplogSyncer) Fini() {
	sync.batcher.Fini()
}

func (sync *OplogSyncer) String() string {
	return fmt.Sprintf("Syncer[%s]", sync.Replset)
}

// Bind bind different worker
func (sync *OplogSyncer) Bind(w *Worker) {
	sync.batcher.workerGroup = append(sync.batcher.workerGroup, w)
}

func (sync *OplogSyncer) StartDiskApply() {
	sync.persister.SetFetchStage(utils.FetchStageStoreDiskApply)
}

// Start to polling oplog
func (sync *OplogSyncer) Start() {
	l.Logger.Infof("%s poll oplog syncer start. ckpt_interval[%dms], gid[%s], shard_key[%s]",
		sync, conf.Options.CheckpointInterval, conf.Options.IncrSyncOplogGIDS, conf.Options.IncrSyncShardKey)

	sync.startTime = time.Now()

	// start persister
	sync.persister.Start()

	// TODO, need handle PBRT
	// process about the checkpoint :
	//
	// 1. create checkpoint manager
	// 2. load existing ckpt from remote storage
	// 3. start checkpoint persist routine
	sync.newCheckpointManager(sync.Replset, sync.startPosition)
	if _, ok := sync.startPosition.(int64); !ok {
		// set resumeToken for aliyun_serverless
		sync.reader.SetQueryTimestampOnEmpty(sync.startPosition)
	}

	// load checkpoint and set stage
	if err := sync.loadCheckpoint(); err != nil {
		l.Logger.Panicf("%v", err)
	}

	// start deserializer: parse data from pending queue, and then push into logs queue.
	sync.startDeserializer()
	// start batcher: pull oplog from logs queue and then batch together before adding into worker.
	sync.startBatcher()

	// forever fetching oplog from mongodb into oplog_reader
	for {
		sync.poll()

		// error or exception occur
		l.Logger.Warnf("%s polling yield. master:%t, yield:%dms", sync, quorum.IsMaster(), SyncerFetchErrorRetryMs)
		utils.YieldInMs(SyncerFetchErrorRetryMs)
	}
}

// fetch all oplog from logs queue, batched together and then send to different workers.
func (sync *OplogSyncer) startBatcher() {
	var batcher = sync.batcher
	filterCheckTs := time.Now()
	filterFlag := false // marks whether previous log is filter
	var pendingBarrierTs int64

	nimo.GoRoutineInLoop(func() {
		/*
		 * judge self is master?
		 */
		if !quorum.IsMaster() {
			utils.YieldInMs(SyncerFetchErrorRetryMs)
			return
		}

		// Re-confirm barrier checkpoint if previous wait was interrupted by Pause/Shutdown.
		// Do not process new ops until the DDL execution is confirmed.
		if pendingBarrierTs > 0 {
			if sync.checkCheckpointUpdate(true, pendingBarrierTs) {
				l.Logger.Infof("%s pending barrier checkpoint[%v] confirmed after resume",
					sync, utils.ExtractTimestampForLog(pendingBarrierTs))
				pendingBarrierTs = 0
			} else {
				utils.YieldInMs(DDLCheckpointInterval)
				return
			}
		}

		// As much as we can batch more from logs queue. batcher can merge
		// a sort of oplogs from different logs queue one by one. the max number
		// of oplogs in batch is limited by AdaptiveBatchingMaxSize
		batchedOplog, barrier, allEmpty, exit, ddlOplogs := batcher.BatchMore()

		// it's better to handle filter in BatchMore function, but I don't want to touch this file anymore
		if conf.Options.FilterOplogGids {
			if err := sync.filterOplogGid(batchedOplog); err != nil {
				l.Logger.Panicf("%v", err)
			}
		}

		var newestTs int64
		if exit {
			l.Logger.Infof("%s have reached exit signal", sync)
			// should exit now, make sure the checkpoint is updated before that
			lastLog, lastFilterLog := batcher.getLastOplog()
			newestTs = 1 // default is 1
			if lastLog != nil && utils.TimeStampToInt64(lastLog.Timestamp) > newestTs {
				newestTs = utils.TimeStampToInt64(lastLog.Timestamp)
			} else if newestTs == 1 && lastFilterLog != nil {
				// only set to the lastFilterLog timestamp if all before oplog filtered.
				newestTs = utils.TimeStampToInt64(lastFilterLog.Timestamp)
			}

			if lastLog != nil && !allEmpty {
				// push to worker
				if worked := batcher.dispatchBatches(batchedOplog); worked {
					sync.replMetric.SetLSN(newestTs)
					// update latest fetched timestamp in memory
					sync.reader.UpdateQueryTimestamp(newestTs)
				}
			}

			// Wait for all workers to complete before exiting
			if !batcher.waitForAllWorkersIdle() {
				l.Logger.Warnf("%s exit: barrier wait interrupted, checkpoint may not have reached %v. "+
					"Not setting CanClose to avoid premature exit", sync, utils.ExtractTimestampForLog(newestTs))
				pendingBarrierTs = newestTs
				return
			}
			// flush checkpoint value
			sync.checkpoint(true, newestTs)
			sync.CanClose = true
			l.Logger.Infof("%s blocking and waiting exits, checkpoint: %v", sync, utils.ExtractTimestampForLog(newestTs))
			select {} // block forever, wait outer routine exits
		} else {
			log, filterLog := batcher.getLastOplog()

			// Step 1: DML dispatch (independent of DDL handling)
			if log != nil && !allEmpty {
				newestTs = utils.TimeStampToInt64(log.Timestamp)

				// push to worker
				if worked := batcher.dispatchBatches(batchedOplog); worked {
					sync.replMetric.SetLSN(newestTs)
					// update latest fetched timestamp in memory
					sync.reader.UpdateQueryTimestamp(newestTs)
				}

				filterFlag = false
			}

			// Step 2: DDL barrier (independent of DML, because DDL may have no preceding DML)
			if barrier && len(ddlOplogs) > 0 {
				// Wait for all workers to complete before executing DDL
				if !batcher.waitForAllWorkersIdle() {
					// Wait interrupted by Pause/Shutdown, record barrier and return
					ddlTs := utils.TimeStampToInt64(ddlOplogs[len(ddlOplogs)-1].Parsed.Timestamp)
					pendingBarrierTs = ddlTs
					return
				}
				// Execute DDL directly, bypassing worker pipeline
				if err := batcher.executeDDLDirectly(ddlOplogs); err != nil {
					l.Logger.Criticalf("%s DDL direct execution failed: %v", sync, err)
					return
				}
				// Update checkpoint after DDL execution
				ddlTs := utils.TimeStampToInt64(ddlOplogs[len(ddlOplogs)-1].Parsed.Timestamp)
				sync.checkpoint(true, ddlTs)
			} else if log != nil && !allEmpty {
				// No DDL: normal checkpoint update
				if barrier && conf.Options.Tunnel == utils.VarTunnelDirect {
					// Direct tunnel: Send() is synchronous blocking, ack is updated when data is persisted
					// No need to poll checkpoint
					sync.checkpoint(true, newestTs)
				} else {
					sync.checkpoint(barrier, 0)
					if barrier && !sync.checkCheckpointUpdate(true, newestTs) {
						pendingBarrierTs = newestTs
						return
					}
				}
			} else {
				// if log is nil, check whether filterLog is empty
				if filterLog == nil {
					// no need to update
					l.Logger.Debugf("%s filterLog is nil", sync)
					return
				} else if utils.TimeStampToInt64(filterLog.Timestamp) <= sync.ckptManager.GetInMemory().Timestamp {
					// no need to update
					l.Logger.Debugf("%s filterLogTs[%v] is small than ckptTs[%v], skip this filterLogTs", sync,
						filterLog.Timestamp, utils.ExtractTimestampForLog(sync.ckptManager.GetInMemory().Timestamp))
					return
				} else {
					now := time.Now()

					// return if filterFlag == false
					if filterFlag == false {
						filterFlag = true
						filterCheckTs = now
						return
					}

					// pass only if all received oplog are filtered for {FilterCheckpointCheckInterval} seconds.
					if now.After(filterCheckTs.Add(FilterCheckpointCheckInterval*time.Second)) == false {
						return
					}

					checkpointTs := utils.ExtractMongoTimestamp(sync.ckptManager.GetInMemory().Timestamp)
					filterNewestTs := utils.ExtractMongoTimestamp(filterLog.Timestamp)
					if filterNewestTs-FilterCheckpointGap > checkpointTs {
						// if checkpoint has not been update for {FilterCheckpointGap} seconds, update
						// checkpoint mandatory.
						newestTs = utils.TimeStampToInt64(filterLog.Timestamp)
						l.Logger.Infof("%s try to update checkpoint mandatory from %v to %v", sync,
							utils.ExtractTimestampForLog(sync.ckptManager.GetInMemory().Timestamp),
							filterLog.Timestamp)
					} else {
						l.Logger.Debugf("%s filterLogTs[%v] not bigger than checkpoint[%v]",
							sync, filterLog.Timestamp,
							utils.ExtractTimestampForLog(sync.ckptManager.GetInMemory().Timestamp))
						return
					}
				}

				filterFlag = false

				if log != nil {
					newestTsLog := utils.ExtractTimestampForLog(newestTs)
					if newestTs < utils.TimeStampToInt64(log.Timestamp) {
						l.Logger.Errorf("%s filter newestTs[%v] smaller than previous timestamp[%v]",
							sync, newestTsLog, log.Timestamp)
					}

					l.Logger.Infof("%s waiting last checkpoint[%v] updated", sync, newestTsLog)
					// check last checkpoint updated

					status := sync.checkCheckpointUpdate(true, utils.TimeStampToInt64(log.Timestamp))
					l.Logger.Infof("%s last checkpoint[%v] updated [%v]", sync, newestTsLog, status)
				} else {
					l.Logger.Infof("%s last log is empty, skip waiting checkpoint updated", sync)
				}

				// update latest fetched timestamp in memory
				sync.reader.UpdateQueryTimestamp(newestTs)
				// flush checkpoint by the newest filter oplog value
				sync.checkpoint(false, newestTs)
				return
			}
		}
	})
}

func (sync *OplogSyncer) checkCheckpointUpdate(barrier bool, newestTs int64) bool {
	if barrier && newestTs > 0 {
		l.Logger.Infof("%s checkCheckpointUpdate find barrier", sync)
		var checkpointTs int64
		for i := 0; ; i++ {
			if utils.IncrSentinelOptions.Pause || utils.IncrSentinelOptions.Shutdown {
				l.Logger.Warnf("%s barrier wait interrupted by sentinel (Pause=%v, Shutdown=%v)",
					sync, utils.IncrSentinelOptions.Pause, utils.IncrSentinelOptions.Shutdown)
				return false
			}

			checkpoint, err := barrierCkptGetFunc(sync)
			if err != nil {
				l.Logger.Errorf("%s[%v] get remote checkpoint failed: %v", sync, i, err)
				utils.YieldInMs(DDLCheckpointInterval * 3)
				continue
			}

			checkpointTs = checkpoint.Timestamp

			if i%CheckCheckpointUpdateTimes == 0 {
				l.Logger.Infof("%s[%v] compare remote checkpoint[%v] to local newestTs[%v]", sync, i,
					utils.ExtractTimestampForLog(checkpointTs), utils.ExtractTimestampForLog(newestTs))
			}
			if checkpointTs >= newestTs {
				l.Logger.Infof("%s[%v] barrier checkpoint already updated to newest[%v]",
					sync, i, utils.ExtractTimestampForLog(newestTs))
				return true
			}
			utils.YieldInMs(DDLCheckpointInterval)

			if barrierCkptFlushFunc(sync) {
				l.Logger.Infof("[%v] checkCheckpointUpdate checkpoint update succeed", i)
			}
		}
	}
	return false
}

/********************************deserializer begin**********************************/
// deserializer: pending_queue -> logs_queue

// how many pending queue we create
func calculatePendingQueueConcurrency() int {
	// single {pending|logs}queue while there are multi source shards
	// need more thread when fetching method is change stream, no matter replica or sharding.
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		return PipelineQueueMaxNr
	}

	if conf.Options.IsShardCluster() {
		return PipelineQueueMiddleNr
	}
	return PipelineQueueMaxNr
}

// deserializer: fetch oplog from pending queue, parsed and then add into logs queue.
func (sync *OplogSyncer) startDeserializer() {
	parallel := calculatePendingQueueConcurrency()
	sync.PendingQueue = make([]chan [][]byte, parallel)
	sync.logsQueue = make([]chan []*oplog.GenericOplog, parallel)
	for index := 0; index != len(sync.PendingQueue); index++ {
		sync.PendingQueue[index] = make(chan [][]byte, PipelineQueueLen)
		sync.logsQueue[index] = make(chan []*oplog.GenericOplog, PipelineQueueLen)
		sync.updatePendingQueueMetric(index)
		sync.updateLogsQueueMetric(index)
		go sync.deserializer(index)
	}
}

func (sync *OplogSyncer) deserializer(index int) {
	// parser is used to parse the raw []byte
	var parser func(input []byte) (*oplog.PartialLog, error)
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		// parse []byte (change stream event format) -> oplog
		parser = func(input []byte) (*oplog.PartialLog, error) {
			return oplog.ConvertEvent2Oplog(input, conf.Options.IncrSyncChangeStreamWatchFullDocument)
		}
	} else {
		// parse []byte (oplog format) -> oplog
		parser = func(input []byte) (*oplog.PartialLog, error) {
			log := oplog.ParsedLog{}
			err := bson.Unmarshal(input, &log)
			return &oplog.PartialLog{ParsedLog: log}, err
		}
	}

	// combiner is used to combine data and send to downstream
	var combiner func(raw []byte, log *oplog.PartialLog, sourceTime time.Time) *oplog.GenericOplog
	// change stream && !direct && !(kafka & json)
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream &&
		conf.Options.Tunnel != utils.VarTunnelDirect &&
		!(conf.Options.Tunnel == utils.VarTunnelKafka &&
			conf.Options.TunnelMessage == utils.VarTunnelMessageJson) {
		// very time-consuming!
		combiner = func(raw []byte, log *oplog.PartialLog, sourceTime time.Time) *oplog.GenericOplog {
			if out, err := bson.Marshal(&log.ParsedLog); err != nil {
				l.Logger.Panicf("%s deserializer marshal[%v] failed: %v", sync, log, err)
				return nil
			} else {
				return &oplog.GenericOplog{
					Raw:        out,
					Parsed:     log,
					SourceTime: sourceTime.UTC(),
				}
			}
		}
	} else {
		combiner = func(raw []byte, log *oplog.PartialLog, sourceTime time.Time) *oplog.GenericOplog {
			return &oplog.GenericOplog{
				Raw:        raw,
				Parsed:     log,
				SourceTime: sourceTime.UTC(),
			}
		}
	}

	// run
	for {
		batchRawLogs := <-sync.PendingQueue[index]
		nPending := len(sync.PendingQueue[index])
		sync.updatePendingQueueMetric(index)
		nimo.AssertTrue(len(batchRawLogs) != 0, "pending queue batch logs has zero length")
		var deserializeLogs = make([]*oplog.GenericOplog, 0, len(batchRawLogs))

		for _, rawLog := range batchRawLogs {
			log, err := parser(rawLog)
			if err != nil {
				l.Logger.Panicf("%s deserializer parse data failed[%v]", sync, err)
			}
			sourceTime := extractSourceTime(rawLog, log)
			log.RawSize = len(rawLog)
			deserializeLogs = append(deserializeLogs, combiner(rawLog, log, sourceTime))
		}

		sync.recordLastFetchStats(deserializeLogs, time.Now().UTC())
		sync.logsQueue[index] <- deserializeLogs
		sync.updateLogsQueueMetric(index)
		l.Logger.Debugf("deserializer[%v] send %d to logsQueue, pending: %d", index, len(deserializeLogs), nPending)
	}
}

/********************************deserializer end**********************************/

func sourceTimeFromTimestamp(ts primitive.Timestamp) time.Time {
	return time.Unix(int64(ts.T), 0).UTC()
}

func sourceTimeFromGenericOplog(log *oplog.GenericOplog) time.Time {
	if log == nil || log.Parsed == nil {
		return time.Time{}
	}
	if !log.SourceTime.IsZero() {
		return log.SourceTime.UTC()
	}
	return sourceTimeFromTimestamp(log.Parsed.Timestamp)
}

func extractSourceTime(raw []byte, log *oplog.PartialLog) time.Time {
	if conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodChangeStream {
		return sourceTimeFromTimestamp(log.Timestamp)
	}

	var meta struct {
		WallTime primitive.DateTime `bson:"wallTime,omitempty"`
	}
	if err := bson.Unmarshal(raw, &meta); err == nil && meta.WallTime != 0 {
		return meta.WallTime.Time().UTC()
	}

	return sourceTimeFromTimestamp(log.Timestamp)
}

func (sync *OplogSyncer) updatePendingQueueMetric(index int) {
	if sync == nil || index < 0 || index >= len(sync.PendingQueue) {
		return
	}

	used := len(sync.PendingQueue[index])
	capacity := cap(sync.PendingQueue[index])
	labels := []string{sync.Replset, utils.TypeIncr, strconv.Itoa(index)}
	utils.PendingQueueUsedProm.WithLabelValues(labels...).Set(float64(used))
	utils.PendingQueueCapacityProm.WithLabelValues(labels...).Set(float64(capacity))
	utils.PendingQueueUsedRatioProm.WithLabelValues(labels...).Set(utils.QueueUsedRatio(used, capacity))
}

func (sync *OplogSyncer) updateLogsQueueMetric(index int) {
	if sync == nil || index < 0 || index >= len(sync.logsQueue) {
		return
	}

	used := len(sync.logsQueue[index])
	capacity := cap(sync.logsQueue[index])
	labels := []string{sync.Replset, utils.TypeIncr, strconv.Itoa(index)}
	utils.LogsQueueUsedProm.WithLabelValues(labels...).Set(float64(used))
	utils.LogsQueueCapacityProm.WithLabelValues(labels...).Set(float64(capacity))
	utils.LogsQueueUsedRatioProm.WithLabelValues(labels...).Set(utils.QueueUsedRatio(used, capacity))
}

func (sync *OplogSyncer) recordLastFetchStats(logs []*oplog.GenericOplog, now time.Time) {
	if len(logs) == 0 {
		return
	}

	latestLog := logs[len(logs)-1]
	sync.LastFetchTs = latestLog.Parsed.Timestamp
	if sync.replMetric != nil {
		latestSourceTime := sourceTimeFromGenericOplog(latestLog)
		sync.replMetric.SetOplogGetDelay(now.UTC().UnixMilli() - latestSourceTime.UnixMilli())
	}
}

// only master(maybe several mongo-shake start) can poll oplog.
func (sync *OplogSyncer) poll() {
	// we should reload checkpoint. in case of other collector
	// has fetched oplogs when master quorum leader election
	// happens frequently. so we simply reload.
	checkpoint, _, err := sync.ckptManager.Get()
	if err != nil {
		// we don't continue working on ckpt fetched failed. because we should
		// confirm the exist checkpoint value or exactly knows that it doesn't exist
		l.Logger.Criticalf("%s Acquire the existing checkpoint from remote[%s %s.%s] failed !", sync,
			conf.Options.CheckpointStorage, conf.Options.CheckpointStorageDb,
			conf.Options.CheckpointStorageCollection)
		return
	}
	sync.reader.SetQueryTimestampOnEmpty(checkpoint.Timestamp)
	sync.reader.StartFetcher() // start reader fetcher if not exist

	for quorum.IsMaster() {
		// limit the qps if enabled
		if sync.qos.Limit > 0 {
			sync.qos.FetchBucket()
		}

		// check shutdown
		sync.checkShutdown()

		// only get one
		sync.next()
	}
}

// fetch oplog from reader.
func (sync *OplogSyncer) next() bool {
	var log []byte
	var err error
	if log, err = sync.reader.Next(); log != nil {
		payload := int64(len(log))
		sync.replMetric.AddGet(1)
		sync.replMetric.SetOplogMax(payload)
		sync.replMetric.SetOplogAvg(payload)
		sync.replMetric.ClearReplStatus(utils.FetchBad)
		sync.fetcherState = "normal"
	} else if err != nil && err.Error() == sourceReader.CollectionCappedFatalError.Error() {
		// capped error persists after max retries, crash the process
		sync.fetcherState = "error"
		sync.replMetric.SetReplStatus(utils.FetchBad)
		l.Logger.Crashf("%s oplog collection capped error is fatal after max retries,"+
			" please check oplog window and fix manually!", sync)
		return false
	} else if err != nil && err.Error() == sourceReader.CollectionCappedError.Error() {
		l.Logger.Errorf("%s oplog collection capped error, auto-resetting cursor (retrying)", sync)
		sync.fetcherState = "degraded"
		sync.replMetric.SetReplStatus(utils.FetchBad)
		utils.YieldInMs(SyncerFetchErrorRetryMs)
		return false
	} else if err != nil && err.Error() != sourceReader.TimeoutError.Error() {
		l.Logger.Errorf("%s %s internal error: %v", sync, sync.reader.Name(), err)
		// error is nil indicate that only timeout incur syncer.next()
		// return false. so we regardless that
		if sync.isCrashError(err.Error()) {
			l.Logger.Panicf("%s I can't handle this error, please solve it manually!", sync)
		}

		// alarm
	}

	// buffered oplog or trigger to flush. log is nil
	// means that we need to flush buffer right now

	// inject into persist handler
	sync.persister.Inject(log)
	return true
}

func (sync *OplogSyncer) checkShutdown() {
	// single run, no need to add lock or CAS
	if (!utils.IncrSentinelOptions.Shutdown && utils.IncrSentinelOptions.ExitPoint <= 0) ||
		sync.SyncGroup == nil || sync.shutdownWorking {
		return
	}

	sync.shutdownWorking = true

	nimo.GoRoutine(func() {
		if utils.IncrSentinelOptions.Shutdown {
			utils.IncrSentinelOptions.ExitPoint = utils.TimeStampToInt64(sync.LastFetchTs)
		}

		l.Logger.Infof("%s check shutdown, set exit-point[%v]", sync, utils.IncrSentinelOptions.ExitPoint)
		for range time.NewTicker(500 * time.Millisecond).C {
			exitCount := 0
			for _, syncer := range sync.SyncGroup {
				if syncer.CanClose {
					exitCount++
				} else {
					l.Logger.Infof("%s syncer[%v] wait close, last fetch oplog timestamp[%v], exit-point[%v]",
						sync, syncer.Replset, utils.ExtractMongoTimestamp(syncer.LastFetchTs),
						utils.IncrSentinelOptions.ExitPoint)
				}
			}

			if exitCount == len(sync.SyncGroup) {
				break
			}
		}

		l.Logger.Panicf("%s all syncer shutdown, try exit, don't be panic", sync)
	})
}

func (sync *OplogSyncer) isCrashError(errMsg string) bool {
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream &&
		strings.Contains(errMsg, sourceReader.ErrInvalidStartPosition) {
		return true
	}
	return false
}

func (sync *OplogSyncer) filterOplogGid(batchedOplog [][]*oplog.GenericOplog) error {
	var err error
	for _, batchGroup := range batchedOplog {
		for _, log := range batchGroup {
			if len(log.Parsed.Gid) > 0 {
				log.Parsed.Gid = ""
				log.Raw, err = bson.Marshal(&log.Parsed.ParsedLog)
				if err != nil {
					return fmt.Errorf("marshal gid filtered oplog[%v] failed: %v", log.Parsed, err)
				}
			}
		}
	}

	return nil
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
		Who           string     `json:"who"`
		Tag           string     `json:"tag"`
		ReplicaSet    string     `json:"replset"`
		Logs          uint64     `json:"logs_get"`
		LogsRepl      uint64     `json:"logs_repl"`
		LogsSuccess   uint64     `json:"logs_success"`
		Tps           uint64     `json:"tps"`
		Lsn           *MongoTime `json:"lsn"`
		LsnAck        *MongoTime `json:"lsn_ack"`
		LsnCkpt       *MongoTime `json:"lsn_ckpt"`
		Now           *Time      `json:"now"`
		OplogAvg      string     `json:"log_size_avg"`
		OplogMax      string     `json:"log_size_max"`
		FetcherStatus string     `json:"fetcher_status"`
	}

	// total replication info
	utils.IncrSyncHttpApi.RegisterAPI("/repl", nimo.HttpGet, func([]byte) interface{} {
		return &Info{
			Who:         conf.Options.Id,
			Tag:         utils.BRANCH,
			ReplicaSet:  sync.Replset,
			Logs:        sync.replMetric.Get(),
			LogsRepl:    sync.replMetric.Apply(),
			LogsSuccess: sync.replMetric.Success(),
			Tps:         sync.replMetric.Tps(),
			Lsn: &MongoTime{
				TimestampMongo: utils.Int64ToString(sync.replMetric.LSN),
				Time: Time{
					TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSN),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSN)),
				}},
			LsnCkpt: &MongoTime{
				TimestampMongo: utils.Int64ToString(sync.replMetric.LSNCheckpoint),
				Time: Time{
					TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSNCheckpoint),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSNCheckpoint)),
				}},
			LsnAck: &MongoTime{
				TimestampMongo: utils.Int64ToString(sync.replMetric.LSNAck),
				Time: Time{
					TimestampUnix: utils.ExtractMongoTimestamp(sync.replMetric.LSNAck),
					TimestampTime: utils.TimestampToString(utils.ExtractMongoTimestamp(sync.replMetric.LSNAck)),
				}},
			Now: &Time{
				TimestampUnix: time.Now().Unix(),
				TimestampTime: utils.TimestampToString(time.Now().Unix()),
			},
			OplogAvg:      utils.GetMetricWithSize(sync.replMetric.OplogAvgSize),
			OplogMax:      utils.GetMetricWithSize(sync.replMetric.OplogMaxSize),
			FetcherStatus: sync.fetcherState,
		}
	})

	// queue size info
	type InnerQueue struct {
		Id           uint   `json:"queue_id"`
		PendingQueue uint64 `json:"pending_queue_used"`
		LogsQueue    uint64 `json:"logs_queue_used"`
	}
	type Queue struct {
		SyncerId            string       `json:"syncer_replica_set_name"`
		LogsQueuePerSize    int          `json:"logs_queue_size"`
		PendingQueuePerSize int          `json:"pending_queue_size"`
		InnerQueue          []InnerQueue `json:"syncer_inner_queue"`
		PersisterBufferUsed int          `json:"persister_buffer_used"`
	}

	utils.IncrSyncHttpApi.RegisterAPI("/queue", nimo.HttpGet, func([]byte) interface{} {
		queue := make([]InnerQueue, calculatePendingQueueConcurrency())
		for i := 0; i < len(queue); i++ {
			queue[i] = InnerQueue{
				Id:           uint(i),
				PendingQueue: uint64(len(sync.PendingQueue[i])),
				LogsQueue:    uint64(len(sync.logsQueue[i])),
			}
		}
		return &Queue{
			SyncerId:            sync.Replset,
			LogsQueuePerSize:    cap(sync.logsQueue[0]),
			PendingQueuePerSize: cap(sync.PendingQueue[0]),
			InnerQueue:          queue,
			PersisterBufferUsed: len(sync.persister.Buffer),
		}
	})
}
