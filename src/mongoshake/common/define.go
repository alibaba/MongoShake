package utils

const (
	// log
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
)
