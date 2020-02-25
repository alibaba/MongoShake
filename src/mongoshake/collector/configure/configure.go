package conf

import "mongoshake/common"

type Configuration struct {
	ConfVersion uint `config:"conf.version"` // TODO

	MongoUrls               []string `config:"mongo_urls"`
	MongoCsUrl              string   `config:"mongo_cs_url"`
	MongoConnectMode        string   `config:"mongo_connect_mode"`
	MongoFetchMethod        string   `config:"mongo_fetch_method"`
	MajorityWriteFull       bool     `config:"majority_write.full"`
	MajorityWriteIncr       bool     `config:"majority_write.incr"`
	CollectorId             string   `config:"collector.id"`
	CheckpointInterval      int64    `config:"checkpoint.interval"`
	HTTPListenPort          int      `config:"http_profile"`
	SystemProfile           int      `config:"system_profile"`
	LogLevel                string   `config:"log.level"`
	LogDirectory            string   `config:"log.dir"`
	LogFileName             string   `config:"log.file"`
	LogBuffer               bool     `config:"log.buffer"`
	OplogGIDS               []string `config:"oplog.gids"`
	ShardKey                string   `config:"shard_key"`
	SyncerReaderBufferTime  uint     `config:"syncer.reader.buffer_time"`
	WorkerNum               int      `config:"worker"`
	WorkerOplogCompressor   string   `config:"worker.oplog_compressor"`
	WorkerBatchQueueSize    uint64   `config:"worker.batch_queue_size"`
	AdaptiveBatchingMaxSize int      `config:"adaptive.batching_max_size"`
	FetcherBufferCapacity   int      `config:"fetcher.buffer_capacity"`
	Tunnel                  string   `config:"tunnel"`
	TunnelAddress           []string `config:"tunnel.address"`
	TunnelMessage           string   `config:"tunnel.message"`
	MasterQuorum            bool     `config:"master_quorum"`
	ContextStorage          string   `config:"context.storage"`
	ContextStorageUrl       string   `config:"context.storage.url"`
	ContextAddress          string   `config:"context.address"`
	ContextStartPosition    int64    `config:"context.start_position" type:"date"`
	FilterNamespaceBlack    []string `config:"filter.namespace.black"`
	FilterNamespaceWhite    []string `config:"filter.namespace.white"`
	FilterPassSpecialDb     []string `config:"filter.pass.special.db"`
	SyncMode                string   `config:"sync_mode"`
	TransformNamespace      []string `config:"transform.namespace"`
	DBRef                   bool     `config:"dbref"`

	ReplayerDMLOnly                   bool   `config:"replayer.dml_only"`
	ReplayerExecutor                  int    `config:"replayer.executor"`
	ReplayerExecutorUpsert            bool   `config:"replayer.executor.upsert"`
	ReplayerExecutorInsertOnDupUpdate bool   `config:"replayer.executor.insert_on_dup_update"`
	ReplayerCollisionEnable           bool   `config:"replayer.collision_detection"`
	ReplayerConflictWriteTo           string `config:"replayer.conflict_write_to"`
	ReplayerDurable                   bool   `config:"replayer.durable"`

	FullSyncCollectionParallel           int   `config:"full_sync.collection_parallel"`
	FullSyncDocumentParallel             int   `config:"full_sync.document_parallel"`
	FullSyncDocumentBatchSize            int   `config:"full_sync.document_batch_size"`
	FullSyncCollectionDrop               bool  `config:"full_sync.collection_drop"`
	FullSyncCreateIndex                  bool  `config:"full_sync.create_index"`
	FullSyncOplogStoreDisk               bool  `config:"full_sync.oplog_store_disk"`
	FullSyncOplogStoreDiskMaxSize        int64 `config:"full_sync.oplog_store_disk_max_size"`
	FullSyncExecutorInsertOnDupUpdate    bool  `config:"full_sync.executor.insert_on_dup_update"`
	FullSyncExecutorFilterOrphanDocument bool  `config:"full_sync.executor.filter.orphan_document"`

	/*---------------------------------------------------------*/
	// inner variables
	LogLevelOld    string `config:"log_level"`  // compatible with older versions
	LogFileNameOld string `config:"log_file"`   // compatible with older versions
	LogBufferOld   bool   `config:"log_buffer"` // compatible with older versions

	/*---------------------------------------------------------*/
	// generated variables
	Version string // version
}

func (configuration *Configuration) IsShardCluster() bool {
	return len(configuration.MongoUrls) > 1
}

var Options Configuration

func GetSafeOptions() Configuration {
	polish := Options

	// modify mongo_ulrs
	for i := range polish.MongoUrls {
		polish.MongoUrls[i] = utils.BlockMongoUrlPassword(polish.MongoUrls[i], "***")
	}
	// modify tunnel.address
	for i := range polish.TunnelAddress {
		polish.TunnelAddress[i] = utils.BlockMongoUrlPassword(polish.TunnelAddress[i], "***")
	}
	// modify storage url
	polish.ContextStorageUrl = utils.BlockMongoUrlPassword(polish.ContextStorageUrl, "***")

	return polish
}
