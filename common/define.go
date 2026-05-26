package utils

const (
	// log level
	VarLogLevelDebug   = "debug"
	VarLogLevelInfo    = "info"
	VarLogLevelWarning = "warning"
	VarLogLevelError   = "error"

	// sync mode
	VarSyncModeAll  = "all"
	VarSyncModeIncr = "incr"
	VarSyncModeFull = "full"

	// mongo connect mode
	VarMongoConnectModePrimary            = "primary"
	VarMongoConnectModeSecondaryPreferred = "secondaryPreferred"
	VarMongoConnectModeSecondary          = "secondary"
	VarMongoConnectModeNearset            = "nearest"
	VarMongoConnectModeStandalone         = "standalone"

	// full_sync.create_index
	VarFullSyncCreateIndexNone       = "none"
	VarFullSyncCreateIndexForeground = "foreground"
	VarFullSyncCreateIndexBackground = "background"

	// incr_sync.mongo_fetch_method
	VarIncrSyncMongoFetchMethodOplog        = "oplog"
	VarIncrSyncMongoFetchMethodChangeStream = "change_stream"

	// incr_sync.shard_key
	VarIncrSyncShardKeyAuto       = "auto"
	VarIncrSyncShardKeyId         = "id"
	VarIncrSyncShardKeyCollection = "collection"

	// incr_sync.worker.oplog_compressor
	VarIncrSyncWorkerOplogCompressorNone    = "none"
	VarIncrSyncWorkerOplogCompressorGzip    = "gzip"
	VarIncrSyncWorkerOplogCompressorZlib    = "zlib"
	VarIncrSyncWorkerOplogCompressorDeflate = "deflate"
	VarIncrSyncWorkerOplogCompressorSnappy  = "snappy"

	// incr_sync.tunnel
	VarTunnelDirect = "direct"
	VarTunnelRpc    = "rpc"
	VarTunnelFile   = "file"
	VarTunnelTcp    = "tcp"
	VarTunnelKafka  = "kafka"
	VarTunnelMock   = "mock"

	// incr_sync.tunnel.message
	VarTunnelMessageRaw  = "raw"
	VarTunnelMessageJson = "json"
	VarTunnelMessageBson = "bson"

	// incr_sync.conflict_write_to
	VarIncrSyncConflictWriteToNone = "none"
	VarIncrSyncConflictWriteToDb   = "db"
	VarIncrSyncConflictWriteToSdk  = "sdk"

	// incr_sync.executor.dup_key_strategy
	VarIncrSyncExecutorDupKeyStrategyIgnore         = "ignore"
	VarIncrSyncExecutorDupKeyStrategyError          = "error"
	VarIncrSyncExecutorDupKeyStrategyDeleteAndRetry = "delete_and_retry"
	VarIncrSyncExecutorDupKeyStrategySkip           = "skip"

	// checkpoint.storage.db
	VarCheckpointStorageDbReplicaDefault  = "mongoshake"
	VarCheckpointStorageDbShardingDefault = "admin"
	VarCheckpointStorageCollectionDefault = "ckpt_default"

	// inner variable: checkpoint.storage
	VarCheckpointStorageApi      = "api"
	VarCheckpointStorageDatabase = "database"

	// innder variable: incr_sync.reader_debug
	VarIncrSyncReaderDebugNone    = ""
	VarIncrSyncReaderDebugDiscard = "discard" // throw all
	VarIncrSyncReaderDebugPrint   = "print"   // print

	// special
	VarSpecialSourceDBFlagAliyunServerless = "aliyun_serverless"

	// mongo dbVersions
	VarMongoVersion26  = "2.6.0"
	VarMongoVersion32  = "3.2.0"
	VarMongoVersion36  = "3.6.0"
	VarMongoVersion401 = "4.0.1"

	// replica name for ReplicationCoordinator
	VarReplicaNameMongos = "mongos"

	// syncer timing constants
	VarSyncerFetchErrorRetryMs             = 6000 // interval(ms) to wait after fetch error before retrying
	VarSyncerDDLCheckpointIntervalMs       = 300  // interval(ms) between DDL checkpoint checks
	VarSyncerFilterCheckpointGap           = 180  // duration(seconds) without checkpoint update before mandatory flush
	VarSyncerFilterCheckpointCheckInterval = 180  // interval(seconds) between filter-only checkpoint checks
	VarSyncerCheckCheckpointUpdateTimes    = 10   // max retry count for checkpoint update verification

	// pipeline queue sizing
	VarSyncerPipelineQueueMaxNr    = 4
	VarSyncerPipelineQueueMiddleNr = 2
	VarSyncerPipelineQueueMinNr    = 1
	VarSyncerPipelineQueueLen      = 64

	// oplog reader constants
	VarOplogReaderMaxCappedRetry = 10 // max consecutive CappedPositionLost retries before fatal exit

	// time-series collection
	VarSystemBucketsPrefix   = "system.buckets."
	VarSystemViewsCollection = "system.views"
	VarOplogKeyOriginalSpec  = "originalSpec"
)

type Pair struct {
	First  interface{}
	Second interface{}
}
