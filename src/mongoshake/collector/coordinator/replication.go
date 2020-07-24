package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"mongoshake/collector"
	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

// ReplicationCoordinator global coordinator instance. consist of
// one syncerGroup and a number of workers
type ReplicationCoordinator struct {
	MongoD             []*utils.MongoSource // the source mongod
	MongoS             *utils.MongoSource   // the source mongos
	MongoCS            *utils.MongoSource   // the source mongocs
	RealSourceFullSync []*utils.MongoSource // point to MongoD if source is mongod, otherwise MongoS
	RealSourceIncrSync []*utils.MongoSource // point to MongoD if source is mongod, otherwise MongoS

	// Sentinel listener
	fullSentinel *utils.Sentinel
	incrSentinel *utils.Sentinel

	// syncerGroup and workerGroup number is 1:N in ReplicaSet.
	// 1:1 while replicated in shard cluster
	syncerGroup []*collector.OplogSyncer

	// control the qps, TODO, need modify to bucket
	rateController *nimo.SimpleRateController
}

func (coordinator *ReplicationCoordinator) Run() error {
	// check all mongodb deployment and fetch the instance info
	if err := coordinator.sanitizeMongoDB(); err != nil {
		return err
	}
	LOG.Info("Collector startup. shard_by[%s] gids[%s]", conf.Options.IncrSyncShardKey, conf.Options.IncrSyncOplogGIDS)

	// all configurations has changed to immutable
	// opts, _ := json.Marshal(conf.Options)
	opts, _ := json.Marshal(conf.GetSafeOptions())
	LOG.Info("Collector configuration %s", string(opts))

	// sentinel: full and incr
	coordinator.fullSentinel = utils.NewSentinel(utils.TypeFull)
	coordinator.fullSentinel.Register()
	coordinator.incrSentinel = utils.NewSentinel(utils.TypeIncr)
	coordinator.incrSentinel.Register()

	syncMode, fullBeginTs, err := coordinator.selectSyncMode(conf.Options.SyncMode)
	if err != nil {
		return fmt.Errorf("select sync mode failed: %v", err)
	}

	/*
	 * Generally speaking, it's better to use several bridge timestamp so that
	 * each shard match one in sharding mode.
	 * TODO
	 */
	LOG.Info("start running with mode[%v], fullBeginTs[%v]", syncMode, utils.ExtractTimestampForLog(fullBeginTs))

	switch syncMode {
	case utils.VarSyncModeAll:
		if conf.Options.FullSyncReaderOplogStoreDisk {
			LOG.Info("run parallel document oplog")
			if err := coordinator.parallelDocumentOplog(fullBeginTs); err != nil {
				return err
			}
		} else {
			LOG.Info("run serialize document oplog")
			if err := coordinator.serializeDocumentOplog(fullBeginTs); err != nil {
				return err
			}
		}
	case utils.VarSyncModeFull:
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
	case utils.VarSyncModeIncr:
		// check given oplog exists
		beginTs32 := conf.Options.CheckpointStartPosition
		beginTs64 := beginTs32 << 32
		if beginTs32 > 1 {
			LOG.Info("begin timestamp[%v] > 1, need to check oplog exists",
				utils.ExtractTimestampForLog(beginTs64))

			// get current oldest timestamp
			_, _, _, bigOldTs, _, err := utils.GetAllTimestamp(coordinator.MongoD)
			if err != nil {
				return fmt.Errorf("get oldest timestamp failed[%v]", err)
			}

			// if fromMongos {
			if false {
				// currently, we can't check whether the oplog is lost from mongos
				LOG.Info("ignore check oldest timestamp exist when source is mongos")
			} else {
				// the oldest oplog is lost
				if utils.ExtractMongoTimestamp(bigOldTs) >= beginTs32 {
					return fmt.Errorf("incr sync beginTs[%v] is less than current the biggest old timestamp[%v], " +
						"this error usually means illegal start timestamp(checkpoint.start_position) or " +
						"capped collection error happen",
						utils.ExtractMongoTimestamp(beginTs64), utils.ExtractMongoTimestamp(bigOldTs))
				}
			}
		} else {
			// we can't insert Timestamp(0, 0) that will be treat as Now(), so we use Timestamp(1, 0)
			beginTs64 = 1
		}
		if err := coordinator.startOplogReplication(beginTs64, beginTs64); err != nil {
			return err
		}
	default:
		LOG.Critical("unknown sync mode %v", conf.Options.SyncMode)
		return errors.New("unknown sync mode " + conf.Options.SyncMode)
	}

	return nil
}

func (coordinator *ReplicationCoordinator) sanitizeMongoDB() error {
	var conn *utils.MongoConn
	var err error
	var hasUniqIndex = false
	rs := map[string]int{}

	// try to connect CheckpointStorage
	checkpointStorageUrl := conf.Options.CheckpointStorageUrl
	if conn, err = utils.NewMongoConn(checkpointStorageUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); conn == nil || !conn.IsGood() || err != nil {
		LOG.Critical("Connect checkpointStorageUrl[%v] error[%v]. Please add primary node into 'mongo_urls' " +
			"if 'context.storage.url' is empty", checkpointStorageUrl, err)
		return err
	}
	conn.Close()

	for i, src := range coordinator.MongoD {
		if conn, err = utils.NewMongoConn(src.URL, conf.Options.MongoConnectMode, true,
				utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); conn == nil || !conn.IsGood() || err != nil {
			LOG.Critical("Connect mongo server error. %v, url : %s. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-reachable-servers-error", err, src.URL)
			return err
		}

		// a conventional ReplicaSet should have local.oplog.rs collection
		if conf.Options.SyncMode != utils.VarSyncModeFull &&
			// conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog &&
			!conn.HasOplogNs() {
			LOG.Critical("There has no oplog collection in mongo db server")
			conn.Close()
			return errors.New("no oplog ns in mongo. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-oplog-ns-in-mongo-error")
		}

		// check if there has dup server every replica set in RS or Shard
		rsName := conn.AcquireReplicaSetName()
		// rsName will be set to default if empty
		if rsName == "" {
			rsName = fmt.Sprintf("default-%d", i)
			LOG.Warn("Source mongodb have empty replica set name, url[%s], change to default[%s]", src.URL, rsName)
		}

		if _, exist := rs[rsName]; exist {
			LOG.Critical("There has duplicate replica set name : %s", rsName)
			conn.Close()
			return errors.New("duplicated replica set source")
		}
		rs[rsName] = 1
		src.ReplicaName = rsName

		// look around if there has uniq index
		if !hasUniqIndex && conf.Options.IncrSyncShardKey == oplog.ShardAutomatic {
			hasUniqIndex = conn.HasUniqueIndex()
		}
		// doesn't reuse current connection
		conn.Close()
	}

	// we choose sharding by collection if there are unique index
	// existing in collections
	if conf.Options.IncrSyncShardKey == oplog.ShardAutomatic {
		if hasUniqIndex {
			conf.Options.IncrSyncShardKey = oplog.ShardByNamespace
		} else {
			conf.Options.IncrSyncShardKey = oplog.ShardByID
		}
	}

	return nil
}

// TODO, add UT
// if the oplog of checkpoint timestamp exist in all source db, then only do oplog replication instead of document replication
func (coordinator *ReplicationCoordinator) selectSyncMode(syncMode string) (string, int64, error) {
	if syncMode != utils.VarSyncModeAll {
		return syncMode, 0, nil
	}

	// smallestNew is the smallest of the all newest timestamp
	tsMap, _, smallestNew, _, _, err := utils.GetAllTimestamp(coordinator.MongoD)
	if err != nil {
		return syncMode, 0, err
	}

	// fetch mongos checkpoint when using change stream
	var mongosCkpt *ckpt.CheckpointContext
	if coordinator.MongoS != nil && conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		LOG.Info("try to fetch mongos checkpoint")
		ckptManager := ckpt.NewCheckpointManager(coordinator.MongoS.ReplicaName, 0)
		ckpt, _, err := ckptManager.Get()
		if err != nil {
			return "", 0, err
		}
		mongosCkpt = ckpt
	}

	needFull := false
	for replName, ts := range tsMap {
		var ckptRemote *ckpt.CheckpointContext
		if mongosCkpt == nil {
			ckptManager := ckpt.NewCheckpointManager(replName, 0)
			ckpt, _, err := ckptManager.Get()
			if err != nil {
				return "", 0, err
			}
			ckptRemote = ckpt
			LOG.Info("%s checkpoint using mongod/replica_set: %s", replName, ckpt)
		} else {
			ckptRemote = mongosCkpt
			LOG.Info("%s checkpoint using mongos: %s", replName, mongosCkpt)
		}

		// checkpoint less than the oldest timestamp, ckpt.OplogDiskQueue == "" means not enable
		// disk persist
		if ts.Oldest >= ckptRemote.Timestamp && ckptRemote.OplogDiskQueue == "" {
			// check if disk queue enable
			needFull = true
			break
		}
	}

	if needFull {
		return utils.VarSyncModeAll, utils.TimestampToInt64(smallestNew), nil
	} else {
		LOG.Info("sync mode change from 'all' to 'oplog'")
		return utils.VarSyncModeIncr, 0, nil
	}
}

// run incr-sync after full-sync
func (coordinator *ReplicationCoordinator) serializeDocumentOplog(fullBeginTs int64) error {
	if err := coordinator.startDocumentReplication(); err != nil {
		return fmt.Errorf("start document replication failed: %v", err)
	}

	// get current newest timestamp
	_, fullFinishTs, _, oldestTs, _, err := utils.GetAllTimestamp(coordinator.MongoD)
	if err != nil {
		return fmt.Errorf("get full sync finish timestamp failed[%v]", err)
	}

	LOG.Info("------------------------full sync done!------------------------")
	LOG.Info("oldestTs[%v] fullBeginTs[%v] fullFinishTs[%v]", utils.ExtractTimestampForLog(oldestTs),
		utils.ExtractTimestampForLog(fullBeginTs), utils.ExtractTimestampForLog(fullFinishTs))

	// if fromMongoS {
	if false {
		// currently, we can't check whether the oplog is lost from mongos
		LOG.Info("ignore check oldest timestamp exist when source is mongos")
	} else {
		// the oldest oplog is lost
		if utils.TimestampToInt64(oldestTs) >= fullBeginTs {
			err = fmt.Errorf("incr sync ts[%v] is less than current oldest ts[%v], this error means user's " +
				"oplog collection size is too small or full sync continues too long",
				utils.ExtractTimestampForLog(fullBeginTs), utils.ExtractTimestampForLog(oldestTs))
			LOG.Error(err)
			return err
		}
	}

	LOG.Info("finish full sync, start incr sync with timestamp: fullBeginTs[%v], fullFinishTs[%v]",
		utils.ExtractTimestampForLog(fullBeginTs), utils.ExtractTimestampForLog(fullFinishTs))

	return coordinator.startOplogReplication(fullBeginTs, utils.TimestampToInt64(fullFinishTs))
}

// TODO, set initSyncFinishTs into worker
// run full-sync and incr-sync in parallel
func (coordinator *ReplicationCoordinator) parallelDocumentOplog(fullBeginTs int64) error {
	var docError error
	var docWg sync.WaitGroup
	docWg.Add(1)
	nimo.GoRoutine(func() {
		defer docWg.Done()
		if err := coordinator.startDocumentReplication(); err != nil {
			docError = LOG.Critical("document Replication error. %v", err)
			return
		}
		LOG.Info("------------------------full sync done!------------------------")

		/*
		// get current newest timestamp
		endAllTsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			docError = LOG.Critical("document replication get end timestamp failed[%v]", err)
			return
		}

		for replset, endTs := range endAllTsMap {
			beginTs := beginTsMap[replset]
			LOG.Info("document replication replset %v beginTs[%v] endTs[%v]",
				replset, utils.ExtractTs32(beginTs), utils.ExtractTs32(endTs.Newest))
			docEndTsMap[replset] = endTs.Newest
		}*/
	})
	// during document replication, oplog syncer fetch oplog and store on disk, in order to avoid oplog roll up
	// fullSyncFinishPosition means no need to check the end time to disable DDL
	if err := coordinator.startOplogReplication(fullBeginTs, 0); err != nil {
		return LOG.Critical("start oplog replication failed: %v", err)
	}
	// wait for document replication to finish, set docEndTs to oplog syncer, start oplog replication
	docWg.Wait()
	if docError != nil {
		return docError
	}
	LOG.Info("finish document replication, change oplog replication to %v",
		utils.LogFetchStage(utils.FetchStageStoreDiskApply))
	for _, syncer := range coordinator.syncerGroup {
		syncer.StartDiskApply()
	}

	return nil
}
