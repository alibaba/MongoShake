package conf

import (
	"mongoshake/common"

	"github.com/getlantern/deepcopy"
)

type Configuration struct {
	// 0. version
	ConfVersion uint `config:"conf.version"` // do not modify the tag name

	// 1. global
	Id                          string   `config:"id"`
	MasterQuorum                bool     `config:"master_quorum"`
	FullSyncHTTPListenPort      int      `config:"full_sync.http_port"`
	IncrSyncHTTPListenPort      int      `config:"incr_sync.http_port"`
	SystemProfilePort           int      `config:"system_profile_port"`
	LogLevel                    string   `config:"log.level"`
	LogDirectory                string   `config:"log.dir"`
	LogFileName                 string   `config:"log.file"`
	LogFlush                    bool     `config:"log.flush"`
	SyncMode                    string   `config:"sync_mode"`
	MongoUrls                   []string `config:"mongo_urls"`
	MongoCsUrl                  string   `config:"mongo_cs_url"`
	MongoSUrl                   string   `config:"mongo_s_url"`
	MongoConnectMode            string   `config:"mongo_connect_mode"`
	Tunnel                      string   `config:"tunnel"`
	TunnelAddress               []string `config:"tunnel.address"`
	TunnelMessage               string   `config:"tunnel.message"`
	FilterNamespaceBlack        []string `config:"filter.namespace.black"`
	FilterNamespaceWhite        []string `config:"filter.namespace.white"`
	FilterPassSpecialDb         []string `config:"filter.pass.special.db"`
	FilterDDLEnable             bool     `config:"filter.ddl_enable"`
	CheckpointStorageUrl        string   `config:"checkpoint.storage.url"`
	CheckpointStorageDb         string   `config:"checkpoint.storage.db"`
	CheckpointStorageCollection string   `config:"checkpoint.storage.collection"`
	CheckpointStartPosition     int64    `config:"checkpoint.start_position" type:"date"`
	TransformNamespace          []string `config:"transform.namespace"`

	// 2. full sync
	FullSyncReaderCollectionParallel     int    `config:"full_sync.reader.collection_parallel"`
	FullSyncReaderWriteDocumentParallel  int    `config:"full_sync.reader.write_document_parallel"`
	FullSyncReaderReadDocumentCount      uint64 `config:"full_sync.reader.read_document_count"`
	FullSyncReaderDocumentBatchSize      int    `config:"full_sync.reader.document_batch_size"`
	FullSyncCollectionDrop               bool   `config:"full_sync.collection_exist_drop"`
	FullSyncCreateIndex                  string `config:"full_sync.create_index"`
	FullSyncReaderOplogStoreDisk         bool   `config:"full_sync.reader.oplog_store_disk"`
	FullSyncReaderOplogStoreDiskMaxSize  int64  `config:"full_sync.reader.oplog_store_disk_max_size"`
	FullSyncExecutorInsertOnDupUpdate    bool   `config:"full_sync.executor.insert_on_dup_update"`
	FullSyncExecutorFilterOrphanDocument bool   `config:"full_sync.executor.filter.orphan_document"`
	FullSyncExecutorMajorityEnable       bool   `config:"full_sync.executor.majority_enable"`

	// 3. incr sync
	IncrSyncMongoFetchMethod              string   `config:"incr_sync.mongo_fetch_method"`
	IncrSyncChangeStreamWatchFullDocument bool     `config:"incr_sync.change_stream.watch_full_document"`
	IncrSyncOplogGIDS                     []string `config:"incr_sync.oplog.gids"`
	IncrSyncShardKey                      string   `config:"incr_sync.shard_key"`
	IncrSyncShardByObjectIdWhiteList      []string `config:"incr_sync.shard_by_object_id_whitelist"`
	IncrSyncWorker                        int      `config:"incr_sync.worker"`
	IncrSyncTargetDelay                   int64    `config:"incr_sync.target_delay"`
	IncrSyncWorkerOplogCompressor         string   `config:"incr_sync.worker.oplog_compressor"`
	IncrSyncWorkerBatchQueueSize          uint64   `config:"incr_sync.worker.batch_queue_size"`
	IncrSyncAdaptiveBatchingMaxSize       int      `config:"incr_sync.adaptive.batching_max_size"`
	IncrSyncFetcherBufferCapacity         int      `config:"incr_sync.fetcher.buffer_capacity"`
	IncrSyncExecutorUpsert                bool     `config:"incr_sync.executor.upsert"`
	IncrSyncExecutorInsertOnDupUpdate     bool     `config:"incr_sync.executor.insert_on_dup_update"`
	IncrSyncConflictWriteTo               string   `config:"incr_sync.conflict_write_to"`
	IncrSyncExecutorMajorityEnable        bool     `config:"incr_sync.executor.majority_enable"`

	/*---------------------------------------------------------*/
	// inner variables, not open to user
	CheckpointStorage        string `config:"checkpoint.storage"`
	CheckpointInterval       int64  `config:"checkpoint.interval"`
	FullSyncExecutorDebug    bool   `config:"full_sync.executor.debug"`
	IncrSyncDBRef            bool   `config:"incr_sync.dbref"`
	IncrSyncExecutor         int    `config:"incr_sync.executor"`
	IncrSyncExecutorDebug    bool   `config:"incr_sync.executor.debug"` // !ReplayerDurable
	IncrSyncReaderDebug      string `config:"incr_sync.reader.debug"`
	IncrSyncCollisionEnable  bool   `config:"incr_sync.collision_detection"`
	IncrSyncReaderBufferTime uint   `config:"incr_sync.reader.buffer_time"`

	/*---------------------------------------------------------*/
	// generated variables
	Version string // version

	/*---------------------------------------------------------*/
	// deprecate variables
	IncrSyncTunnel        string   `config:"incr_sync.tunnel"`         // deprecate since v2.4.1
	IncrSyncTunnelAddress []string `config:"incr_sync.tunnel.address"` // deprecate since v2.4.1
	IncrSyncTunnelMessage string   `config:"incr_sync.tunnel.message"` // deprecate since v2.4.1
	HTTPListenPort        int      `config:"http_profile"`             // deprecate since v2.4.1
	SystemProfile         int      `config:"system_profile"`           // deprecate since v2.4.1
}

func (configuration *Configuration) IsShardCluster() bool {
	return len(configuration.MongoUrls) > 1
}

var Options Configuration

func GetSafeOptions() Configuration {
	polish := new(Configuration)
	deepcopy.Copy(polish, &Options)

	// modify mongo_ulrs
	for i := range Options.MongoUrls {
		polish.MongoUrls[i] = utils.BlockMongoUrlPassword(Options.MongoUrls[i], "***")
	}
	// modify mongo_cs_url
	polish.MongoCsUrl = utils.BlockMongoUrlPassword(Options.MongoCsUrl, "***")
	// modify mongo_s_url
	polish.MongoSUrl = utils.BlockMongoUrlPassword(Options.MongoSUrl, "***")
	// modify tunnel.address
	for i := range Options.TunnelAddress {
		polish.TunnelAddress[i] = utils.BlockMongoUrlPassword(Options.TunnelAddress[i], "***")
	}
	for i := range Options.IncrSyncTunnelAddress {
		polish.IncrSyncTunnelAddress[i] = utils.BlockMongoUrlPassword(Options.IncrSyncTunnelAddress[i], "***")
	}
	// modify storage url
	polish.CheckpointStorageUrl = utils.BlockMongoUrlPassword(Options.CheckpointStorageUrl, "***")

	return *polish
}
