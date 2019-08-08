package conf

type Configuration struct {
	MongoUrls               []string `config:"mongo_urls"`
	MongoConnectMode        string   `config:"mongo_connect_mode"`
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
	MasterQuorum            bool     `config:"master_quorum"`
	ContextStorage          string   `config:"context.storage"`
	ContextStorageUrl       string   `config:"context.storage.url"`
	ContextAddress          string   `config:"context.address"`
	ContextStartPosition    int64    `config:"context.start_position" type:"date"`
	FilterNamespaceBlack    []string `config:"filter.namespace.black"`
	FilterNamespaceWhite    []string `config:"filter.namespace.white"`
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

	ReplayerCollectionDrop     bool `config:"replayer.collection_drop"`
	ReplayerCollectionParallel int  `config:"replayer.collection_parallel"`
	ReplayerDocumentParallel   int  `config:"replayer.document_parallel"`
	ReplayerDocumentBatchSize  int  `config:"replayer.document_batch_size"`

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
