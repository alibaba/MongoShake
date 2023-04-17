package main

import (
	"fmt"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
)

// priority use mongo_s_url
func getSourceDbUrl() (string, error) {
	var source string

	if len(conf.Options.MongoSUrl) != 0 {
		source = conf.Options.MongoSUrl
	} else {
		if len(conf.Options.MongoUrls) > 0 {
			source = conf.Options.MongoUrls[0]
		} else {
			return source, fmt.Errorf("mongo_urls && mongo_s_url should not all be empty")
		}
	}

	return source, nil
}

func SanitizeOptions() error {
	// compatible with old version
	if err := handleDeprecateConf(); err != nil {
		return err
	}

	// default value
	if err := checkDefaultValue(); err != nil {
		return err
	}

	// check connection
	if err := checkConnection(); err != nil {
		return err
	}

	// judge conflict value
	return checkConflict()
}

func handleDeprecateConf() error {
	// IncrSyncTunnel has deprecated since v2.4.1
	if conf.Options.Tunnel == "" && conf.Options.IncrSyncTunnel != "" {
		conf.Options.Tunnel = conf.Options.IncrSyncTunnel
	}
	// IncrSyncTunnelAddress has deprecated since v2.4.1
	if len(conf.Options.TunnelAddress) == 0 && len(conf.Options.IncrSyncTunnelAddress) != 0 {
		conf.Options.TunnelAddress = conf.Options.IncrSyncTunnelAddress
	}
	// IncrSyncTunnelMessage has deprecated since v2.4.1
	if conf.Options.TunnelMessage == "" && conf.Options.IncrSyncTunnelMessage != "" {
		conf.Options.TunnelMessage = conf.Options.IncrSyncTunnelMessage
	}

	// HTTPListenPort has deprecated since v2.4.1
	if conf.Options.HTTPListenPort != 0 && conf.Options.IncrSyncHTTPListenPort == 0 {
		conf.Options.IncrSyncHTTPListenPort = conf.Options.HTTPListenPort
	}
	// SystemProfile has deprecated since v2.4.1
	if conf.Options.SystemProfile != 0 && conf.Options.SystemProfilePort == 0 {
		conf.Options.SystemProfilePort = conf.Options.SystemProfile
	}
	return nil
}

func checkDefaultValue() error {
	// 1. global
	if conf.Options.Id == "" {
		conf.Options.Id = "mongoshake"
	}

	if conf.Options.FullSyncHTTPListenPort <= 0 {
		conf.Options.FullSyncHTTPListenPort = 9101
	}
	if conf.Options.IncrSyncHTTPListenPort <= 0 {
		conf.Options.IncrSyncHTTPListenPort = 9100
	}
	if conf.Options.SystemProfilePort <= 0 {
		conf.Options.SystemProfilePort = 9200
	}

	if conf.Options.LogLevel == "" {
		conf.Options.LogLevel = utils.VarLogLevelInfo
	} else if conf.Options.LogLevel != utils.VarLogLevelDebug && conf.Options.LogLevel != utils.VarLogLevelInfo &&
		conf.Options.LogLevel != utils.VarLogLevelWarning && conf.Options.LogLevel != utils.VarLogLevelError {
		return fmt.Errorf("log.level should in {debug, info, warning, error}")
	}
	if conf.Options.LogFileName == "" {
		conf.Options.LogFileName = "mongoshake.log"
	}

	if conf.Options.SyncMode == "" {
		conf.Options.SyncMode = utils.VarSyncModeIncr
	} else if conf.Options.SyncMode != utils.VarSyncModeAll &&
		conf.Options.SyncMode != utils.VarSyncModeFull &&
		conf.Options.SyncMode != utils.VarSyncModeIncr {
		return fmt.Errorf("sync_mode should in {all, full, incr}")
	}
	if len(conf.Options.MongoSUrl) == 0 {
		if len(conf.Options.MongoUrls) == 0 {
			return fmt.Errorf("mongo_s_url and mongo_urls cannot be empty at the same time")
		}
	}
	if conf.Options.MongoConnectMode == "" {
		conf.Options.MongoConnectMode = utils.VarMongoConnectModeSecondaryPreferred
	} else {
		if conf.Options.MongoConnectMode != utils.VarMongoConnectModePrimary &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeSecondaryPreferred &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeSecondary &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeNearset &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeStandalone {
			return fmt.Errorf("mongo_connect_mode should in {primary, secondaryPreferred, secondary, nearest, standalone}")
		}
	}

	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		if len(conf.Options.MongoSUrl) == 0 && len(conf.Options.MongoUrls) > 1 {
			return fmt.Errorf("mongo_s_url should be given if source is sharding and incr_sync.mongo_fetch_method == %s",
				utils.VarIncrSyncMongoFetchMethodChangeStream)
		}
	}

	if conf.Options.CheckpointStorage == "" {
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
	} else if conf.Options.CheckpointStorage != utils.VarCheckpointStorageDatabase &&
		conf.Options.CheckpointStorage != utils.VarCheckpointStorageApi {
		return fmt.Errorf("checkpoint.storage should in {database, api}")
	}
	if conf.Options.CheckpointStorageUrl == "" {
		// do nothing here
	}
	if conf.Options.CheckpointStorageDb == "" {
		conf.Options.CheckpointStorageDb = utils.VarCheckpointStorageDbReplicaDefault
	}
	if conf.Options.CheckpointStorageCollection == "" {
		conf.Options.CheckpointStorageCollection = utils.VarCheckpointStorageCollectionDefault
	}
	if conf.Options.CheckpointStartPosition <= 0 {
		conf.Options.CheckpointStartPosition = 1
	}
	if conf.Options.CheckpointInterval <= 0 {
		conf.Options.CheckpointInterval = 5000 // ms
	}

	// 2. full sync
	if conf.Options.FullSyncReaderCollectionParallel <= 0 {
		conf.Options.FullSyncReaderCollectionParallel = 6
	}
	if conf.Options.FullSyncReaderWriteDocumentParallel <= 0 {
		conf.Options.FullSyncReaderWriteDocumentParallel = 8
	}
	if conf.Options.FullSyncReaderParallelThread <= 0 {
		conf.Options.FullSyncReaderParallelThread = 1
	} else if conf.Options.FullSyncReaderParallelThread > 128 {
		return fmt.Errorf("full_sync.reader.parallel_thread should <= 128")
	}
	if conf.Options.FullSyncReaderParallelIndex == "" {
		conf.Options.FullSyncReaderParallelIndex = "_id"
	}
	if conf.Options.FullSyncReaderDocumentBatchSize <= 0 {
		conf.Options.FullSyncReaderDocumentBatchSize = 128
	}
	if conf.Options.FullSyncReaderFetchBatchSize <= 0 {
		conf.Options.FullSyncReaderFetchBatchSize = 1024
	}
	if conf.Options.FullSyncCreateIndex == "" {
		conf.Options.FullSyncCreateIndex = utils.VarFullSyncCreateIndexForeground
	} else if conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexNone &&
		conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexForeground &&
		conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexBackground {
		return fmt.Errorf("full_sync.create_index should in {none, foreground, background}")
	}
	if conf.Options.FullSyncReaderOplogStoreDiskMaxSize <= 0 {
		conf.Options.FullSyncReaderOplogStoreDiskMaxSize = 256000
	}

	// 3. incr sync
	if conf.Options.IncrSyncMongoFetchMethod == "" {
		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	} else if conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodOplog &&
		conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodChangeStream {
		return fmt.Errorf("incr_sync.mongo_fetch_method should in {oplog, change_stream}")
	}
	if conf.Options.IncrSyncShardKey == "" {
		conf.Options.IncrSyncShardKey = utils.VarIncrSyncShardKeyCollection
	} else if conf.Options.IncrSyncShardKey != utils.VarIncrSyncShardKeyAuto &&
		conf.Options.IncrSyncShardKey != utils.VarIncrSyncShardKeyId &&
		conf.Options.IncrSyncShardKey != utils.VarIncrSyncShardKeyCollection {
		return fmt.Errorf("incr_sync.shard_key should in {auto, id, collection}")
	}
	if len(conf.Options.IncrSyncShardByObjectIdWhiteList) != 0 {
		if conf.Options.IncrSyncShardKey != utils.VarIncrSyncShardKeyCollection {
			return fmt.Errorf("incr_sync.shard_by_object_id_whitelist should only be set when 'incr_sync.shard_key == collection'")
		}
	}
	if conf.Options.IncrSyncWorker == 0 {
		conf.Options.IncrSyncWorker = 8
	} else if conf.Options.IncrSyncWorker <= 0 || conf.Options.IncrSyncWorker > 256 {
		return fmt.Errorf("incr_sync.worker[%v] should in range [1, 256]", conf.Options.IncrSyncWorker)
	}
	if conf.Options.IncrSyncTunnelWriteThread == 0 {
		conf.Options.IncrSyncTunnelWriteThread = conf.Options.IncrSyncWorker
	} else if conf.Options.IncrSyncTunnelWriteThread%conf.Options.IncrSyncWorker != 0 {
		return fmt.Errorf("incr_sync.tunnel.write_thread[%v] must be an interge multiple of incr_sync.worker[%v]",
			conf.Options.IncrSyncTunnelWriteThread, conf.Options.IncrSyncWorker)
	}
	if conf.Options.IncrSyncWorkerOplogCompressor == "" {
		conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
	} else if conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorGzip &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorZlib &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorDeflate &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorSnappy {
		return fmt.Errorf("incr_sync.worker.oplog_compressor in {none, gzip, zlib, deflate, snappy}")
	}
	if conf.Options.IncrSyncTargetDelay < 0 {
		conf.Options.IncrSyncTargetDelay = 0
	}
	if conf.Options.IncrSyncWorkerBatchQueueSize <= 0 {
		conf.Options.IncrSyncWorkerBatchQueueSize = 64
	}
	if conf.Options.IncrSyncAdaptiveBatchingMaxSize <= 0 {
		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 1024
	}
	if conf.Options.IncrSyncFetcherBufferCapacity <= 0 {
		conf.Options.IncrSyncFetcherBufferCapacity = 256
	}
	if conf.Options.IncrSyncReaderFetchBatchSize <= 0 {
		conf.Options.IncrSyncReaderFetchBatchSize = 1024
	}
	if conf.Options.Tunnel == "" {
		conf.Options.Tunnel = utils.VarTunnelDirect
	} else if conf.Options.Tunnel != utils.VarTunnelDirect &&
		conf.Options.Tunnel != utils.VarTunnelRpc &&
		conf.Options.Tunnel != utils.VarTunnelTcp &&
		conf.Options.Tunnel != utils.VarTunnelFile &&
		conf.Options.Tunnel != utils.VarTunnelKafka &&
		conf.Options.Tunnel != utils.VarTunnelMock {
		return fmt.Errorf("incr_sync.tunnel in {direct, rpc, tcp, file, kafka, mock}")
	}
	if conf.Options.TunnelMessage == "" {
		conf.Options.TunnelMessage = utils.VarTunnelMessageRaw
	} else if conf.Options.TunnelMessage != utils.VarTunnelMessageRaw &&
		conf.Options.TunnelMessage != utils.VarTunnelMessageBson &&
		conf.Options.TunnelMessage != utils.VarTunnelMessageJson {
		return fmt.Errorf("incr_sync.tunnel.message in {raw, bson, json}")
	}
	if conf.Options.IncrSyncExecutor <= 0 {
		conf.Options.IncrSyncExecutor = 1
	}
	if conf.Options.IncrSyncConflictWriteTo == "" {
		conf.Options.IncrSyncConflictWriteTo = utils.VarIncrSyncConflictWriteToNone
	} else if conf.Options.IncrSyncConflictWriteTo != utils.VarIncrSyncConflictWriteToNone &&
		conf.Options.IncrSyncConflictWriteTo != utils.VarIncrSyncConflictWriteToDb &&
		conf.Options.IncrSyncConflictWriteTo != utils.VarIncrSyncConflictWriteToSdk {
		return fmt.Errorf("incr_sync.conflict_write_to in {none, db, sdk}")
	}
	if conf.Options.IncrSyncReaderBufferTime <= 0 {
		conf.Options.IncrSyncReaderBufferTime = 1
	}

	/********************************/
	// set utils

	utils.AppDatabase = conf.Options.CheckpointStorageDb
	utils.APPConflictDatabase = fmt.Sprintf("%s_%s", utils.AppDatabase, "_conflict")
	filter.NsShouldBeIgnore[utils.AppDatabase+"."] = true
	filter.NsShouldBeIgnore[utils.APPConflictDatabase+"."] = true

	return nil
}

func checkConnection() error {
	// check mongo_urls
	for _, mongo := range conf.Options.MongoUrls {
		_, err := utils.NewMongoCommunityConn(mongo, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return fmt.Errorf("connect source mongodb[%v] failed[%v]", utils.BlockMongoUrlPassword(mongo, "***"), err)
		}
	}

	// check mongo_cs_url
	if conf.Options.MongoCsUrl != "" {
		_, err := utils.NewMongoCommunityConn(conf.Options.MongoCsUrl, utils.VarMongoConnectModeSecondaryPreferred,
			true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return fmt.Errorf("connect config-server[%v] failed[%v]", utils.BlockMongoUrlPassword(conf.Options.MongoCsUrl, "***"), err)
		}
	}

	// check tunnel address
	// no need to check target connection when debug flag set.
	if conf.Options.Tunnel == utils.VarTunnelDirect &&
		!conf.Options.FullSyncExecutorDebug &&
		!conf.Options.IncrSyncExecutorDebug {
		for i, mongo := range conf.Options.TunnelAddress {
			targetConn, err := utils.NewMongoCommunityConn(mongo, conf.Options.MongoConnectMode, true,
				utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.TunnelMongoSslRootCaFile)
			if err != nil {
				return fmt.Errorf("connect target tunnel mongodb[%v] failed[%v]", utils.BlockMongoUrlPassword(mongo, "***"), err)
			}

			// set target version
			if i == 0 {
				conf.Options.TargetDBVersion, _ = utils.GetDBVersion(targetConn)
			}
		}
	}

	// set source version
	source, err := getSourceDbUrl()
	if err != nil {
		return err
	}

	sourceConn, _ := utils.NewMongoCommunityConn(source, utils.VarMongoConnectModeSecondaryPreferred, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
	// ignore error
	conf.Options.SourceDBVersion, _ = utils.GetDBVersion(sourceConn)
	if ok, err := utils.GetAndCompareVersion(sourceConn, "2.6.0",
		conf.Options.SourceDBVersion); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("source MongoDB version[%v] should >= 3.0", conf.Options.SourceDBVersion)
	}

	return nil
}

func checkConflict() error {
	/*****************************1. global settings******************************/
	// http_profile & system_profile
	conf.Options.FullSyncHTTPListenPort = utils.MayBeRandom(conf.Options.FullSyncHTTPListenPort)
	conf.Options.IncrSyncHTTPListenPort = utils.MayBeRandom(conf.Options.IncrSyncHTTPListenPort)
	if conf.Options.FullSyncHTTPListenPort == conf.Options.IncrSyncHTTPListenPort {
		return fmt.Errorf("full_sync.http_port should not equal to incr_sync.http_port")
	}

	conf.Options.SystemProfilePort = utils.MayBeRandom(conf.Options.SystemProfilePort)
	// check mongo_cs_url
	if conf.Options.MongoCsUrl == "" && len(conf.Options.MongoUrls) > 1 {
		return fmt.Errorf("mongo_cs_url be config server address when source MongoDB is sharding")
	}
	// set checkpoint.storage.url if empty
	if conf.Options.CheckpointStorageUrl == "" {
		if len(conf.Options.MongoUrls) == 1 {
			// replica-set
			conf.Options.CheckpointStorageUrl = conf.Options.MongoUrls[0]
		} else if len(conf.Options.MongoSUrl) > 0 {
			conf.Options.CheckpointStorageUrl = conf.Options.MongoSUrl
		} else {
			return fmt.Errorf("checkpoint.storage.url should be given when source is sharding")
		}
	}
	// avoid the typo of mongo urls
	if utils.HasDuplicated(conf.Options.MongoUrls) {
		return fmt.Errorf("mongo urls were duplicated")
	}
	// quorm
	if conf.Options.MasterQuorum && conf.Options.CheckpointStorage != utils.VarCheckpointStorageDatabase {
		return fmt.Errorf("context storage should set to 'database' while master election enabled")
	}
	// filter
	if len(conf.Options.FilterNamespaceBlack) != 0 && len(conf.Options.FilterNamespaceWhite) != 0 {
		return fmt.Errorf("at most one of {filter.namespace.black, filter.namespace.white} can be given")
	}
	// filter - filter.pass.special.db
	if len(conf.Options.FilterPassSpecialDb) != 0 {
		// init ns
		filter.InitNs(conf.Options.FilterPassSpecialDb)
	}
	// special variable
	if conf.Options.SpecialSourceDBFlag != "" &&
		conf.Options.SpecialSourceDBFlag != utils.VarSpecialSourceDBFlagAliyunServerless {
		return fmt.Errorf("special.source.db.flag should be empty or 'aliyun_serverless'")
	}
	if conf.Options.SpecialSourceDBFlag == utils.VarSpecialSourceDBFlagAliyunServerless {
		if conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodChangeStream {
			return fmt.Errorf("incr_sync.mongo_fetch_method must be 'change_stream' when special.source.db.flag is set")
		}
	}

	/*****************************2. full sync******************************/

	/*****************************3. incr sync******************************/
	// set incr_sync.worker = shards number if source is sharding
	if len(conf.Options.MongoUrls) > 1 {
		if conf.Options.IncrSyncWorker != len(conf.Options.MongoUrls) &&
			conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
			// only change when incr_sync.mongo_fetch_method = oplog
			conf.Options.IncrSyncWorker = len(conf.Options.MongoUrls)
		}
		if conf.Options.FilterDDLEnable == true &&
			conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
			return fmt.Errorf("DDL is not support for sharding when incr_sync.mongo_fetch_method == 'oplog'")
		}
	}
	if conf.Options.Tunnel == utils.VarTunnelDirect &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone {
		conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
	}
	if len(conf.Options.TunnelAddress) == 0 &&
		conf.Options.Tunnel != utils.VarTunnelMock {
		return fmt.Errorf("incr_sync.tunnel.address shouldn't be empty when incr_sync.tunnel != 'mock'")
	}
	if conf.Options.TunnelKafkaPartitionNumber <= 0 {
		conf.Options.TunnelKafkaPartitionNumber = 1
	} else if conf.Options.TunnelKafkaPartitionNumber > conf.Options.IncrSyncWorker {
		return fmt.Errorf("tunnel.kafka.partition[%v] number should <= incr_sync.worker number[%v]",
			conf.Options.TunnelKafkaPartitionNumber, conf.Options.IncrSyncWorker)
	}
	conf.Options.IncrSyncCollisionEnable = conf.Options.IncrSyncExecutor != 1
	if conf.Options.Tunnel != utils.VarTunnelDirect &&
		conf.Options.SyncMode != utils.VarSyncModeIncr {
		return fmt.Errorf("full sync only support when tunnel type == direct")
	}
	// check source mongodb version >= 4.0 when change stream enable
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		if conf.Options.MongoSUrl == "" && len(conf.Options.MongoUrls) > 1 {
			return fmt.Errorf("mongo_s_url should be given when source is sharding and fetch method is change stream")
		}

		source, err := getSourceDbUrl()
		if err != nil {
			return err
		}

		conn, err := utils.NewMongoCommunityConn(source, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return fmt.Errorf("connect source[%v] failed[%v]", utils.BlockMongoUrlPassword(source, "***"), err)
		}
		if isOk, err := utils.GetAndCompareVersion(conn, "4.0.1", conf.Options.SourceDBVersion); err != nil {
			return fmt.Errorf("compare source[%v] to v4.0.1 failed[%v]", source, err)
		} else if !isOk {
			return fmt.Errorf("source[%v] version should >= 4.0.1 when incr_sync.mongo_fetch_method == %v",
				conf.Options.MongoUrls[0], utils.VarIncrSyncMongoFetchMethodChangeStream)
		}
	} else {
		// disable mongos if fetch method != 'change_stream'
		// conf.Options.MongoSUrl = ""
	}
	// set compressor to none when tunnel message is not 'raw'
	if conf.Options.TunnelMessage != utils.VarTunnelMessageRaw {
		if conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone {
			conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
		}
	}
	// disable oplog disk persist when sync mode isn't 'full'
	if conf.Options.FullSyncReaderOplogStoreDisk {
		if conf.Options.SyncMode != utils.VarSyncModeAll {
			conf.Options.FullSyncReaderOplogStoreDisk = false
		}
	}
	// only enable 'incr_sync.change_stream.watch_full_document' when tunnel != direct.
	if conf.Options.IncrSyncChangeStreamWatchFullDocument {
		if conf.Options.Tunnel == utils.VarTunnelDirect {
			conf.Options.IncrSyncChangeStreamWatchFullDocument = false
		}
	}
	// set start position to 0 when sync_mode != "incr"
	if conf.Options.SyncMode != utils.VarSyncModeIncr {
		conf.Options.CheckpointStartPosition = 1
	}

	/*****************************4. inner variables******************************/
	if conf.Options.IncrSyncReaderDebug != utils.VarIncrSyncReaderDebugNone &&
		conf.Options.IncrSyncReaderDebug != utils.VarIncrSyncReaderDebugDiscard &&
		conf.Options.IncrSyncReaderDebug != utils.VarIncrSyncReaderDebugPrint {
		return fmt.Errorf("incr_sync.reader.debug[%v] invalid", conf.Options.IncrSyncReaderDebug)
	}

	return nil
}
