package main

import (
	"fmt"
	"mongoshake/collector/filter"
	"mongoshake/common"
	"mongoshake/collector/configure"
)

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
	return nil
}

func checkDefaultValue() error {
	// 1. global
	if conf.Options.Id == "" {
		conf.Options.Id = "mongoshake"
	}

	if conf.Options.HTTPListenPort <= 0 {
		conf.Options.HTTPListenPort = 9100
	}
	if conf.Options.SystemProfile <= 0 {
		conf.Options.HTTPListenPort = 9200
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
		conf.Options.SyncMode = utils.VarSyncModeOplog
	} else if conf.Options.SyncMode != utils.VarSyncModeAll && conf.Options.SyncMode != utils.VarSyncModeDocument &&
		conf.Options.SyncMode != utils.VarSyncModeOplog {
		return fmt.Errorf("sync_mode should in {all, document, oplog}")
	}
	if len(conf.Options.MongoUrls) == 0 {
		return fmt.Errorf("mongo_urls shouldn't be empty")
	}
	if conf.Options.MongoConnectMode == "" {
		conf.Options.MongoConnectMode = utils.VarMongoConnectModeSecondaryPreferred
	} else {
		if conf.Options.MongoConnectMode != utils.VarMongoConnectModePrimary &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeSecondaryPreferred &&
			conf.Options.MongoConnectMode != utils.VarMongoConnectModeStandalone {
			return fmt.Errorf("mongo_connect_mode should in {primary, secondaryPreferred, standalone}")
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
	if conf.Options.FullSyncReaderDocumentParallel <= 0 {
		conf.Options.FullSyncReaderDocumentParallel = 8
	}
	if conf.Options.FullSyncReaderDocumentBatchSize <= 0 {
		conf.Options.FullSyncReaderDocumentBatchSize = 128
	} else if conf.Options.FullSyncReaderDocumentBatchSize > 1000 {
		// mgo driver restriction: batch size <= 1000
		conf.Options.FullSyncReaderDocumentBatchSize = 1000
	}
	if conf.Options.FullSyncCreateIndex == "" {
		conf.Options.FullSyncCreateIndex = utils.VarFullSyncCreateIndexForeground
	} else if conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexNone &&
		conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexForeground {
		return fmt.Errorf("full_sync.create_index should in {none, foreground}")
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
	if conf.Options.IncrSyncWorker <= 0 || conf.Options.IncrSyncWorker > 256 {
		return fmt.Errorf("incr_sync.worker should in range [1, 256]")
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
	if conf.Options.IncrSyncWorkerBatchQueueSize <= 0 {
		conf.Options.IncrSyncWorkerBatchQueueSize = 64
	}
	if conf.Options.IncrSyncAdaptiveBatchingMaxSize <= 0 {
		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 1024
	}
	if conf.Options.IncrSyncFetcherBufferCapacity <= 0 {
		conf.Options.IncrSyncFetcherBufferCapacity = 256
	}
	if conf.Options.IncrSyncTunnel == "" {
		conf.Options.IncrSyncTunnel = utils.VarIncrSyncTunnelDirect
	} else if conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelDirect &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelRpc &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelTcp &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelFile &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelKafka &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelMock {
		return fmt.Errorf("incr_sync.tunnel in {direct, rpc, tcp, file, kafka, mock}")
	}
	if conf.Options.IncrSyncTunnelMessage == "" {
		conf.Options.IncrSyncTunnelMessage = utils.VarIncrSyncTunnelMessageRaw
	} else if conf.Options.IncrSyncTunnelMessage != utils.VarIncrSyncTunnelMessageRaw &&
		conf.Options.IncrSyncTunnelMessage != utils.VarIncrSyncTunnelMessageBson &&
		conf.Options.IncrSyncTunnelMessage != utils.VarIncrSyncTunnelMessageJson {
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

	return nil
}

func checkConnection() error {
	// check mongo_urls
	for _, mongo := range conf.Options.MongoUrls {
		_, err := utils.NewMongoConn(mongo, conf.Options.MongoConnectMode, true)
		if err != nil {
			return fmt.Errorf("connect source mongodb[%v] failed[%v]", mongo, err)
		}
	}

	// check mongo_cs_url
	if conf.Options.MongoCsUrl != "" {
		_, err := utils.NewMongoConn(conf.Options.MongoCsUrl, utils.VarMongoConnectModeSecondaryPreferred, true)
		if err != nil {
			return fmt.Errorf("connect config-server[%v] failed[%v]", conf.Options.MongoCsUrl, err)
		}
	}

	// check tunnel address
	if conf.Options.IncrSyncTunnel == utils.VarIncrSyncTunnelDirect {
		for _, mongo := range conf.Options.IncrSyncTunnelAddress {
			_, err := utils.NewMongoConn(mongo, conf.Options.MongoConnectMode, true)
			if err != nil {
				return fmt.Errorf("connect target tunnel mongodb[%v] failed[%v]", mongo, err)
			}
		}
	}

	return nil
}

func checkConflict() error {
	/*****************************1. global settings******************************/
	// http_profile & system_profile
	conf.Options.HTTPListenPort = utils.MayBeRandom(conf.Options.HTTPListenPort)
	conf.Options.SystemProfile = utils.MayBeRandom(conf.Options.SystemProfile)
	// check mongo_cs_url
	if conf.Options.MongoCsUrl == "" && len(conf.Options.MongoUrls) > 1 {
		return fmt.Errorf("mongo_cs_url be config server address when source MongoDB is sharding")
	}
	// set checkpoint.storage.url if empty
	if conf.Options.CheckpointStorageUrl == "" {
		if len(conf.Options.MongoUrls) == 1 {
			// replica-set
			conf.Options.CheckpointStorageUrl = conf.Options.MongoUrls[0]
		} else {
			// sharding
			conf.Options.CheckpointStorageUrl = conf.Options.MongoCsUrl
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

	/*****************************2. full sync******************************/

	/*****************************3. incr sync******************************/
	// set incr_sync.worker = shards number if source is sharding
	if len(conf.Options.MongoUrls) > 1 {
		if conf.Options.IncrSyncWorker != len(conf.Options.MongoUrls) {
			conf.Options.IncrSyncWorker = len(conf.Options.MongoUrls)
		}
		if conf.Options.FilterDDLEnable == true &&
			conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
			return fmt.Errorf("DDL is not support for sharding when incr_sync.mongo_fetch_method == 'oplog'")
		}
	}
	if conf.Options.IncrSyncTunnel == utils.VarIncrSyncTunnelDirect &&
		conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone {
		conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
	}
	if len(conf.Options.IncrSyncTunnelAddress) == 0 &&
		conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelMock {
		return fmt.Errorf("incr_sync.tunnel.address shouldn't be empty when incr_sync.tunnel != 'mock'")
	}
	conf.Options.IncrSyncCollisionEnable = conf.Options.IncrSyncExecutor != 1
	if conf.Options.IncrSyncTunnel != utils.VarIncrSyncTunnelDirect &&
		conf.Options.SyncMode != utils.VarSyncModeOplog {
		return fmt.Errorf("full sync only support when tunnel type == direct")
	}
	// check source mongodb version >= 4.0 when change stream enable
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		conn, err := utils.NewMongoConn(conf.Options.MongoUrls[0], utils.VarMongoConnectModeSecondaryPreferred, true)
		if err != nil {
			return fmt.Errorf("connect source mongodb[%v] failed[%v]", conf.Options.MongoUrls[0], err)
		}
		if isOk, err := utils.GetAndCompareVersion(conn.Session, "4.0.0"); err != nil {
			return fmt.Errorf("compare source mongodb[%v] to v4.0.0 failed[%v]", conf.Options.MongoUrls[0], err)
		} else if !isOk {
			return fmt.Errorf("source mongodb[%v] should >= 4.0.0 when incr_sync.mongo_fetch_method == %v",
				conf.Options.MongoUrls[0], utils.VarIncrSyncMongoFetchMethodChangeStream)
		}
	}
	// set compressor to none when tunnel message is not 'raw'
	if conf.Options.IncrSyncTunnelMessage != utils.VarIncrSyncTunnelMessageRaw {
		if conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone {
			conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
		}
	}

	return nil
}