package coordinator

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

/*
 * compare current checkpoint and database timestamp
 * @return:
 *     int64: the smallest newest timestamp of all mongod
 *     bool: can run incremental sync directly?
 *     error: error
 */
func (coordinator *ReplicationCoordinator) compareCheckpointAndDbTs(syncModeAll bool) (int64, map[string]int64, bool, error) {
	var (
		tsMap       map[string]utils.TimestampNode
		startTsMap  map[string]int64 // replica-set name => timestamp
		smallestNew int64
		err         error
	)

	switch testSelectSyncMode {
	case true:
		// only used for unit test
		tsMap, _, smallestNew, _, _, err = utils.GetAllTimestampInUT()
	case false:
		// smallestNew is the smallest of the all newest timestamp
		tsMap, _, smallestNew, _, _, err = utils.GetAllTimestamp(coordinator.MongoD, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return 0, nil, false, fmt.Errorf("get all timestamp failed: %v", err)
		}
	}

	startTsMap = make(map[string]int64, len(tsMap)+1)

	confTs32 := conf.Options.CheckpointStartPosition
	confTsMongoTs := confTs32 << 32

	l.Logger.Infof("all node timestamp map: %v CheckpointStartPosition:%v", tsMap, utils.Int64ToTimestamp(confTsMongoTs))

	// fetch mongos checkpoint when using change stream
	var mongosCkpt *ckpt.CheckpointContext
	if coordinator.MongoS != nil &&
		conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		l.Logger.Infof("try to fetch mongos checkpoint")
		ckptManager := ckpt.NewCheckpointManager(coordinator.MongoS.ReplicaName, 0)
		ckptVar, exist, err := ckptManager.Get()
		if err != nil {
			return 0, nil, false, fmt.Errorf("get mongos[%v] checkpoint failed: %v",
				coordinator.MongoS.ReplicaName, err)
		} else if ckptVar == nil {
			return 0, nil, false, fmt.Errorf("get mongos[%v] checkpoint empty", coordinator.MongoS.ReplicaName)
		} else if !exist || ckptVar.Timestamp <= 1 { // empty
			mongosCkpt = nil
			// mongosCkpt = ckptVar // still use checkpoint
			startTsMap[coordinator.MongoS.ReplicaName] = int64(confTsMongoTs) // use configuration
		} else {
			mongosCkpt = ckptVar
			startTsMap[coordinator.MongoS.ReplicaName] = int64(ckptVar.Timestamp) // use old checkpoint
		}
	}

	for replName, ts := range tsMap {
		var ckptRemote *ckpt.CheckpointContext
		if mongosCkpt == nil {
			ckptManager := ckpt.NewCheckpointManager(replName, 0)
			ckptVar, exist, err := ckptManager.Get()
			if err != nil {
				return 0, nil, false, fmt.Errorf("get mongod[%v] checkpoint failed: %v", replName, err)
			} else if !exist || ckptVar.Timestamp <= 1 { // empty
				// set nil to make code more clear
				ckptRemote = nil
			} else {
				ckptRemote = ckptVar
			}

			l.Logger.Infof("%s checkpoint using mongod/replica_set: %s, ckptRemote set? [%v]", replName,
				ckptVar, ckptRemote != nil)
		} else {
			ckptRemote = mongosCkpt
			l.Logger.Infof("%s checkpoint using mongos: %s", replName, mongosCkpt)
		}

		if ckptRemote == nil {
			if syncModeAll || confTsMongoTs > (1<<32) && ts.Oldest >= confTsMongoTs {
				l.Logger.Infof("%s syncModeAll[%v] ts.Oldest[%v], confTsMongoTs[%v]", replName, syncModeAll, ts.Oldest,
					confTsMongoTs)
				return smallestNew, nil, false, nil
			}
			startTsMap[replName] = confTsMongoTs
		} else {
			// checkpoint less than the oldest timestamp, ckpt.OplogDiskQueue == "" means not enable
			// disk persist
			if ts.Oldest >= ckptRemote.Timestamp && ckptRemote.OplogDiskQueue == "" {
				l.Logger.Infof("%s ts.Oldest[%v] >= ckptRemote.Timestamp[%v], need full sync", replName,
					ts.Oldest, ckptRemote.Timestamp)

				// can't run incr sync directly
				return smallestNew, nil, false, nil
			}
			startTsMap[replName] = int64(ckptRemote.Timestamp)
		}
	}

	return smallestNew, startTsMap, true, nil
}

func (coordinator *ReplicationCoordinator) isCheckpointExist() (bool, interface{}, error) {
	ckptManager := ckpt.NewCheckpointManager(coordinator.RealSourceFullSync[0].ReplicaName, 0)
	ckptVar, exist, err := ckptManager.Get()
	l.Logger.Infof("isCheckpointExist? %v %v %v", ckptVar, exist, err)
	if err != nil {
		return false, 0, fmt.Errorf("get mongod[%v] checkpoint failed: %v", coordinator.RealSourceFullSync[0].ReplicaName, err)
	} else if !exist {
		// send changeStream
		reader, err := sourceReader.CreateReader(utils.VarIncrSyncMongoFetchMethodChangeStream,
			coordinator.RealSourceFullSync[0].URL,
			coordinator.RealSourceFullSync[0].ReplicaName)
		if err != nil {
			return false, 0, fmt.Errorf("create reader failed: %v", err)
		}

		resumeToken, err := reader.FetchNewestTimestamp()
		if err != nil {
			return false, 0, fmt.Errorf("fetch PBRT fail: %v", err)
		}

		l.Logger.Infof("isCheckpointExist change stream resumeToken: %v", resumeToken)
		return false, resumeToken, nil
	}
	return true, ckptVar.Timestamp, nil
}

// if the oplog of checkpoint timestamp exist in all source db, then only do oplog replication instead of document replication
func (coordinator *ReplicationCoordinator) selectSyncMode(syncMode string) (string, map[string]int64,
	interface{}, error) {
	if syncMode != utils.VarSyncModeAll && syncMode != utils.VarSyncModeIncr {
		return syncMode, nil, int64(0), nil
	}

	// special case, I hate it.
	// TODO, checkpoint support ResumeToken
	if conf.Options.SpecialSourceDBFlag == utils.VarSpecialSourceDBFlagAliyunServerless ||
		(len(conf.Options.MongoSUrl) > 0 && len(conf.Options.MongoCsUrl) == 0 && len(conf.Options.MongoUrls) == 0) {
		// for only mongo_s_url address exists
		if syncMode == utils.VarSyncModeIncr {

			_, startTsMapTmp, _, _ := coordinator.compareCheckpointAndDbTs(syncMode == utils.VarSyncModeAll)
			l.Logger.Infof("for only mongo_s_url address exists startTsMap[%v]", startTsMapTmp)

			return syncMode, startTsMapTmp, int64(0), nil
		}

		ok, token, err := coordinator.isCheckpointExist()
		if err != nil {
			return "", nil, int64(0), fmt.Errorf("check isCheckpointExist failed: %v", err)
		}
		if !ok {
			return utils.VarSyncModeAll, nil, token, nil
		}

		startTsMap := map[string]int64{
			coordinator.RealSourceIncrSync[0].ReplicaName: token.(int64),
		}
		return utils.VarSyncModeIncr, startTsMap, token, nil
	}

	smallestNewTs, startTsMap, canIncrSync, err := coordinator.compareCheckpointAndDbTs(syncMode == utils.VarSyncModeAll)
	if err != nil {
		return "", nil, int64(0), err
	}

	if canIncrSync {
		l.Logger.Infof("sync mode run %v", utils.VarSyncModeIncr)
		return utils.VarSyncModeIncr, startTsMap, int64(0), nil
	} else if syncMode == utils.VarSyncModeIncr || conf.Options.Tunnel != utils.VarTunnelDirect {
		// bugfix v2.4.11: if can not run incr sync directly, return error when sync_mode == "incr"
		// bugfix v2.4.12: return error when tunnel != "direct"
		return "", nil, int64(0), fmt.Errorf("start time illegal, can't run incr sync")
	} else {
		return utils.VarSyncModeAll, nil, smallestNewTs, nil
	}
}

/*
 * fetch all indexes.
 * the cost is low so that no need to run in parallel.
 */
func fetchIndexes(sourceList []*utils.MongoSource, filterFunc func(name string) bool) (map[utils.NS][]bson.D, error) {
	var mutex sync.Mutex
	indexMap := make(map[utils.NS][]bson.D)
	for _, src := range sourceList {
		l.Logger.Infof("source[%v] url[%s] start fetching index...",
			src.ReplicaName, utils.BlockMongoUrlPassword(src.URL, "***"))
		// 1. fetch namespace
		nsList, _, err := utils.GetDbNamespace(src.URL, filterFunc, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return nil, fmt.Errorf("source[%v] get namespace failed: %v", src.ReplicaName, err)
		}

		l.Logger.Infof("index namespace list: %v", nsList)
		// 2. build connection
		conn, err := utils.NewMongoCommunityConn(src.URL, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return nil, fmt.Errorf("source[%v] build connection failed: %v", src.ReplicaName, err)
		}
		defer conn.Close() // it's acceptable to call defer here

		// 3. fetch all indexes
		for _, ns := range nsList {
			cursor, err := conn.Client.Database(ns.Database).Collection(ns.Collection).Indexes().List(nil)
			if err != nil {
				return nil, fmt.Errorf("source[%v] fetch index failed: %v", src.ReplicaName, err)
			}

			indexes := make([]bson.D, 0)
			if err = cursor.All(nil, &indexes); err != nil {
				return nil, fmt.Errorf("index cursor fetch all indexes fail: %v", err)
			}

			mutex.Lock()
			indexMap[ns] = indexes
			mutex.Unlock()
		}

		l.Logger.Infof("source[%v] url[%s] finish fetching index",
			src.ReplicaName, utils.BlockMongoUrlPassword(src.URL, "***"))
	}
	return indexMap, nil
}
