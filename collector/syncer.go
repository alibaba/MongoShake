package collector

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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

	// barrierQuiescePollIntervalMs is how long the entrance waits between quiescence reads.
	barrierQuiescePollIntervalMs = 10
	// barrierQuiesceConfirmGapMs separates the two reads that a quiescence decision needs.
	barrierQuiesceConfirmGapMs = 20
	// barrierQuiesceWarnRounds is how many rounds pass before a stuck quiesce is logged (~10s).
	barrierQuiesceWarnRounds = 1000
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

	// rawInFlight counts every raw oplog that has entered the in-memory pipeline
	// (Persister.bufferInput) and has not yet been pushed into a logsQueue by the
	// deserializer. It is incremented on ingest and decremented right after the
	// deserializer's push completes, so rawInFlight == 0 provably means "no oplog
	// is still travelling from the persister to the batcher". The entrance gate
	// reads it to turn "the queues look empty right now" into a real quiescence
	// proof (see pipelineQuiesced).
	rawInFlight int64
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

		// Re-confirm the barrier if the previous wait was interrupted by Pause/Shutdown: the
		// oplogs dispatched ahead of it have to be applied before the barrier round is released.
		// Do not process new ops until that is confirmed.
		//
		// The interrupted round left the ingest gate closed on purpose, and no call here re-opens
		// it: the poll loop releases the gate on its own as soon as the pipeline is quiescent
		// again, so a run with no further DDL ahead is not left with ingest stopped.
		if pendingBarrierTs > 0 {
			confirmed := false
			if incrOrderingGuarantee() {
				// direct: the real worker acks, not the persisted checkpoint
				confirmed = batcher.waitForAllWorkersIdle()
			} else {
				// otherwise: the persisted checkpoint of the oplogs dispatched ahead of the barrier
				confirmed = sync.checkCheckpointUpdate(true, pendingBarrierTs)
			}
			if !confirmed {
				utils.YieldInMs(DDLCheckpointInterval)
				return
			}
			l.Logger.Infof("%s pending barrier[%v] confirmed after resume",
				sync, utils.ExtractTimestampForLog(pendingBarrierTs))
			pendingBarrierTs = 0
		}

		// As much as we can batch more from logs queue. batcher can merge
		// a sort of oplogs from different logs queue one by one. the max number
		// of oplogs in batch is limited by AdaptiveBatchingMaxSize
		batchedOplog, barrier, allEmpty, exit := batcher.BatchMore()

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

			if incrOrderingGuarantee() {
				// Wait for everything dispatched to be applied before flushing the checkpoint, so a
				// restart cannot resume past an oplog that was never written.
				if !batcher.waitForAllWorkersIdle() {
					l.Logger.Warnf("%s exit: barrier wait interrupted, checkpoint may not have reached %v. "+
						"Not setting CanClose to avoid premature exit", sync, utils.ExtractTimestampForLog(newestTs))
					pendingBarrierTs = newestTs
					return
				}
				// flush checkpoint value
				sync.checkpoint(true, 0)
			} else {
				// otherwise: flush the checkpoint, then wait for the persisted value to catch up
				// with it.
				// flush checkpoint value
				sync.checkpoint(true, 0)
				if !sync.checkCheckpointUpdate(true, newestTs) {
					l.Logger.Warnf("%s exit: barrier wait interrupted, checkpoint may not have reached %v. "+
						"Not setting CanClose to avoid premature exit", sync, utils.ExtractTimestampForLog(newestTs))
					pendingBarrierTs = newestTs
					return
				}
			}
			sync.CanClose = true
			l.Logger.Infof("%s blocking and waiting exits, checkpoint: %v", sync, utils.ExtractTimestampForLog(newestTs))
			select {} // block forever, wait outer routine exits
		} else if log, filterLog := batcher.getLastOplog(); log != nil && !allEmpty {
			// if all filtered, still update checkpoint
			newestTs = utils.TimeStampToInt64(log.Timestamp)

			// push to worker
			if worked := batcher.dispatchBatches(batchedOplog); worked {
				sync.replMetric.SetLSN(newestTs)
				// update latest fetched timestamp in memory
				sync.reader.UpdateQueryTimestamp(newestTs)
			}

			filterFlag = false

			if incrOrderingGuarantee() {
				if barrier {
					// The barrier oplogs have just been handed to worker[0], together with the DML
					// that precedes them. Wait for the real acks instead of comparing against the
					// persisted checkpoint: that checkpoint holds the min ack of oplogs already
					// dispatched, and a lagging logs queue can have pushed it past newestTs -- which
					// is exactly what let a DDL run before the DML written ahead of it.
					if !batcher.waitForAllWorkersIdle() {
						pendingBarrierTs = newestTs
						return
					}
				}
				// flush checkpoint value
				sync.checkpoint(barrier, 0)
			} else {
				// otherwise: flush the checkpoint first, then wait for the persisted value to
				// catch up with it.
				// flush checkpoint value
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

func newOplogParser(lazy bool) func([]byte) (*oplog.PartialLog, error) {
	if lazy {
		return oplog.ParseRaw
	}
	return func(input []byte) (*oplog.PartialLog, error) {
		// Note the destination: the flat oplog document maps onto the embedded ParsedLog, not
		// onto PartialLog, which carries extra non-persistent fields next to it.
		log := &oplog.PartialLog{}
		err := bson.Unmarshal(input, &log.ParsedLog)
		return log, err
	}
}

// newRawOplogParser builds the parser that turns one raw oplog into a PartialLog for the configured
// fetch method: a change stream event, or an oplog document parsed lazily or eagerly according to
// incr_sync.lazy_oplog_parse. It is built once per caller -- the deserializer hoists it out of its
// loop -- and it is the single entry point both the deserializer and the entrance's DDL
// classification go through, so a raw oplog is never interpreted two different ways.
func newRawOplogParser() func([]byte) (*oplog.PartialLog, error) {
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		// parse []byte (change stream event format) -> oplog
		return func(input []byte) (*oplog.PartialLog, error) {
			return oplog.ConvertEvent2Oplog(input, conf.Options.IncrSyncChangeStreamWatchFullDocument)
		}
	}
	// parse []byte (oplog format) -> oplog
	return newOplogParser(conf.Options.IncrSyncLazyOplogParse)
}

func (sync *OplogSyncer) deserializer(index int) {
	// parser is used to parse the raw []byte
	parser := newRawOplogParser()

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
		// one raw batch has fully landed in logsQueue, release its share of rawInFlight.
		// The decrement happens only after the push above completes, so a quiesce check that
		// reads rawInFlight == 0 can be sure this batch is already visible to its next sweep.
		atomic.AddInt64(&sync.rawInFlight, -int64(len(deserializeLogs)))
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

// pendingQueueDepths reports the raw batch backlog of every pending queue, for the quiesce warning
// below. len() on a channel is safe to call concurrently with send/receive.
func (sync *OplogSyncer) pendingQueueDepths() []int {
	depths := make([]int, len(sync.PendingQueue))
	for i := range sync.PendingQueue {
		depths[i] = len(sync.PendingQueue[i])
	}
	return depths
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

// incrOrderingGuarantee reports whether this branch's DDL/DML ordering guarantee is in force for
// the configured tunnel. Both conditions are required:
//
//   - tunnel = direct. The guarantee is mongo2mongo only: every other tunnel hands ordering to its
//     own receiver, and an unset tunnel (a config file without the key) counts as non-direct.
//
//   - incr_sync.barrier.ordering_enable = true. It defaults to false, which doubles as the rollback
//     lever: the entrance quiesce can hold the whole pipeline, so switching it off from the config
//     beats rebuilding a binary.
//
// With either condition off, every guarded site below falls through to the pre-existing behaviour.
// The reader package cannot import this one (it would be a cycle), so it carries its own copy of the
// same predicate -- see collector/reader/oplog_reader.go:incrOrderingGuarantee.
func incrOrderingGuarantee() bool {
	return conf.Options.Tunnel == utils.VarTunnelDirect &&
		conf.Options.IncrSyncBarrierOrderingEnable
}

// isEntryBarrier reports whether raw is an oplog that has to be gated at the pipeline entrance --
// a DDL that BatchMore will later turn into a barrier.
//
// Only oplogs that provably cannot be a DDL are skipped: the gate is released on pipeline state (see
// next), so gating one the batcher would not treat as a barrier merely costs a quiesce wait, while
// missing a real barrier loses the guarantee. The verdict is ddlFilter.Filter -- the very predicate
// processMergeBatch stops on -- so the two cannot disagree.
//
// Transaction commits are not classified here: a commit's oplogs live in txnBuffer and are delivered
// as a unit, which the entrance cannot decide without touching that buffer (see limitation 1 in
// docs/fixes/fix-ddl-dml-ordering-ingest-gate.md).
func (sync *OplogSyncer) isEntryBarrier(raw []byte) bool {
	if !incrOrderingGuarantee() || raw == nil || !conf.Options.FilterDDLEnable {
		return false
	}
	if !mayBeDDL(bson.Raw(raw)) {
		return false
	}
	log, err := parseRawOplog(raw)
	if err != nil {
		// Unparseable raw document: let the deserializer crash on it, where the real diagnostics
		// live. Classifying it as a barrier here would gate on nothing.
		return false
	}
	return ddlFilter.Filter(log)
}

// mayBeDDL is the entrance's cheap screen: it reports whether raw still has to be parsed to decide
// whether it is a DDL. ddlFilter.Filter fires either on an op:"c" command other than applyOps, or
// on an oplog whose namespace ends in system.indexes, so both shapes -- and anything this screen
// cannot read -- have to fall through to the full check. Everything else is a plain data oplog.
func mayBeDDL(raw bson.Raw) bool {
	if ns, ok := raw.Lookup("ns").StringValueOK(); ok && strings.HasSuffix(ns, "system.indexes") {
		return true
	}
	if op, ok := raw.Lookup("op").StringValueOK(); ok {
		return op == "c" // oplog: a DDL is always a command
	}
	// change stream: the event document carries no "op" field. Only the four data operation types
	// convert to something other than a command (see oplog.ConvertEvent2Oplog), so every other
	// type -- including each DDL-shaped one -- is parsed rather than skipped.
	if operationType, ok := raw.Lookup("operationType").StringValueOK(); ok {
		switch operationType {
		case "insert", "update", "delete", "replace":
			return false
		}
	}
	return true
}

// parseRawOplog parses one raw oplog the way the pipeline parses it, for the configured fetch
// method. The deserializer and the entrance classification both build their parser from
// newRawOplogParser, so a raw oplog is never interpreted two different ways and the two cannot
// drift apart.
func parseRawOplog(raw []byte) (*oplog.PartialLog, error) {
	return newRawOplogParser()(raw)
}

// pipelineQuiesced reports whether every oplog that entered the in-memory pipeline has already
// been handed to the batcher. rawInFlight counts from bufferInput (the single ingress) down to the
// deserializer's push, and it is decremented only after that push completes, so a zero reading is
// proof -- a single atomic load, no TOCTOU -- that nothing is still travelling between the
// persister and the batcher. The queues have to be empty as well: a batch that has arrived is only
// quiesced once the batcher has taken it.
//
// Nothing is consumed here. The deserializers and the batcher keep running while this is polled,
// which is what lets the gate wait without stalling the pipeline.
func (sync *OplogSyncer) pipelineQuiesced() bool {
	if atomic.LoadInt64(&sync.rawInFlight) != 0 {
		return false
	}
	for i := range sync.logsQueue {
		if len(sync.logsQueue[i]) != 0 {
			return false
		}
	}
	return true
}

// waitPipelineQuiesced blocks until pipelineQuiesced holds twice in a row.
//
// The second read is the confirm pass: a deserializer's decrement lands just after its push, and
// the disk-replay ingress (Persister.retrieve) can hand a batch over while this goroutine is
// between the two reads, so one clean reading is not yet proof that the tail is in. The wait covers
// the in-memory pipeline only: an oplog already handed to the batcher (remainLogs, batchGroup) or
// held in txnBuffer is ordered by the batcher's own barrier round, not here.
func (sync *OplogSyncer) waitPipelineQuiesced() {
	for round := 1; ; round++ {
		if utils.IncrSentinelOptions.Pause || utils.IncrSentinelOptions.Shutdown ||
			utils.IncrSentinelOptions.ExitPoint > 0 {
			// Shutting down: the exit round confirms the workers and flushes the checkpoint itself,
			// so waiting for the pipeline here would only delay it. Reported, not silent.
			l.Logger.Warnf("%s DDL barrier quiesce interrupted by sentinel (Pause=%v, Shutdown=%v, ExitPoint=%d)",
				sync, utils.IncrSentinelOptions.Pause, utils.IncrSentinelOptions.Shutdown,
				utils.IncrSentinelOptions.ExitPoint)
			return
		}
		if sync.pipelineQuiesced() {
			utils.YieldInMs(barrierQuiesceConfirmGapMs)
			if sync.pipelineQuiesced() {
				return
			}
		}
		utils.YieldInMs(barrierQuiescePollIntervalMs)

		// rawInFlight and the pending queue depths are what tell the two stalls apart: a non-empty
		// queue with rawInFlight stuck means a deserializer is not keeping up, while empty queues
		// with rawInFlight > 0 mean an oplog never left the persister's Buffer.
		if round%barrierQuiesceWarnRounds == 0 {
			l.Logger.Warnf("%s DDL barrier is waiting for the pipeline to quiesce: %d rounds, rawInFlight[%d] pendingQueue%v",
				sync, round, atomic.LoadInt64(&sync.rawInFlight), sync.pendingQueueDepths())
		}
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
	// Ingest gate, closed side: a DDL has been read and injected, and the pipeline has not been
	// proven quiescent since. Reading further from the reader now would only add oplogs *after* that
	// DDL, so hold here until quiescence is back, then reopen.
	//
	// The gate is released from here, on pipeline state, rather than by the batcher once it has
	// applied the barrier. A release that depended on the batcher recognising a barrier would hang
	// this goroutine -- and with it all ingest -- on the first disagreement between the entrance's
	// classification and the batcher's. State cannot disagree with itself: whatever the batcher
	// decided, the gate reopens once every oplog has left the pipeline's queues.
	for sync.batcher.ingestGate.Load() != nil {
		if utils.IncrSentinelOptions.Pause || utils.IncrSentinelOptions.Shutdown ||
			utils.IncrSentinelOptions.ExitPoint > 0 {
			return true // sentinel: stop ingesting and let the outer loop exit
		}
		// Whatever this goroutine had already buffered has to go forward before quiescence is
		// reachable: rawInFlight counts an oplog from the moment it entered Buffer, so one left
		// sitting there would never be released. Safe because this goroutine is Buffer's only
		// writer outside disk replay (FlushBuffer enforces that), and a no-op once Buffer is empty.
		sync.persister.FlushBuffer()
		if sync.pipelineQuiesced() {
			sync.batcher.clearIngestGate()
			continue
		}
		utils.YieldInMs(barrierQuiescePollIntervalMs)
	}

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

	// Ingest gate, closing side: this oplog is a DDL, so it must not reach the batcher until every
	// oplog written before it is already in a logsQueue. persister hands batches out round robin,
	// while getBatch concatenates by queue index -- a queue that fell behind still holds oplogs
	// older than this DDL, and without this wait the batcher would park the DDL and dispatch those
	// older oplogs in a later round, i.e. apply the DDL before the DML written ahead of it.
	//
	// Nothing is read from the reader between here and Inject, so no later oplog can slip in.
	if sync.isEntryBarrier(log) {
		sync.batcher.setIngestGate()
		sync.persister.FlushBuffer()
		sync.waitPipelineQuiesced()
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
