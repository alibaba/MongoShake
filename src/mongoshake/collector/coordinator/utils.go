package coordinator

import (
	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/collector/ckpt"

	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"fmt"
	"mongoshake/collector/reader"
)

/*
 * compare current checkpoint and database timestamp
 * @return:
 *     int64: the smallest newest timestamp of all mongod
 *     bool: can run incremental sync directly?
 *     error: error
 */
func (coordinator *ReplicationCoordinator) compareCheckpointAndDbTs(syncModeAll bool) (bson.MongoTimestamp, map[string]int64, bool, error) {
	var (
		tsMap       map[string]utils.TimestampNode
		startTsMap  map[string]int64 // replica-set name => timestamp
		smallestNew bson.MongoTimestamp
		err         error
	)

	switch testSelectSyncMode {
	case true:
		// only used for unit test
		tsMap, _, smallestNew, _, _, err = utils.GetAllTimestampInUT()
	case false:
		// smallestNew is the smallest of the all newest timestamp
		tsMap, _, smallestNew, _, _, err = utils.GetAllTimestamp(coordinator.MongoD)
		if err != nil {
			return 0, nil, false, fmt.Errorf("get all timestamp failed: %v", err)
		}
	}

	startTsMap = make(map[string]int64, len(tsMap) + 1)

	LOG.Info("all node timestamp map: %v", tsMap)

	confTs32 := conf.Options.CheckpointStartPosition
	confTsMongoTs := bson.MongoTimestamp(confTs32 << 32)

	// fetch mongos checkpoint when using change stream
	var mongosCkpt *ckpt.CheckpointContext
	if coordinator.MongoS != nil && conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		LOG.Info("try to fetch mongos checkpoint")
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

			LOG.Info("%s checkpoint using mongod/replica_set: %s, ckptRemote set? [%v]", replName,
				ckptVar, ckptRemote != nil)
		} else {
			ckptRemote = mongosCkpt
			LOG.Info("%s checkpoint using mongos: %s", replName, mongosCkpt)
		}

		if ckptRemote == nil {
			if syncModeAll || confTsMongoTs > bson.MongoTimestamp(1 << 32) && ts.Oldest >= confTsMongoTs {
				LOG.Info("%s syncModeAll[%v] ts.Oldest[%v], confTsMongoTs[%v]", replName, syncModeAll, ts.Oldest,
					confTsMongoTs)
				return smallestNew, nil, false, nil
			}
			startTsMap[replName] = int64(confTsMongoTs)
		} else {
			// checkpoint less than the oldest timestamp, ckpt.OplogDiskQueue == "" means not enable
			// disk persist
			if ts.Oldest >= ckptRemote.Timestamp && ckptRemote.OplogDiskQueue == "" {
				LOG.Info("%s ts.Oldest[%v] >= ckptRemote.Timestamp[%v], need full sync", replName,
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
	LOG.Info("isCheckpointExist? %v %v %v", ckptVar, exist, err)
	if err != nil {
		return false, 0, fmt.Errorf("get mongod[%v] checkpoint failed: %v", coordinator.RealSourceFullSync[0].ReplicaName, err)
	} else if !exist {
		// send changestream
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

		LOG.Info("isCheckpointExist change stream resumeToken: %v", resumeToken)
		return false, resumeToken, nil
	}
	return true, utils.TimestampToInt64(ckptVar.Timestamp), nil
}

// if the oplog of checkpoint timestamp exist in all source db, then only do oplog replication instead of document replication
func (coordinator *ReplicationCoordinator) selectSyncMode(syncMode string) (string, map[string]int64, interface{}, error) {
	if syncMode != utils.VarSyncModeAll && syncMode != utils.VarSyncModeIncr {
		return syncMode, nil, int64(0), nil
	}

	// special case, I hate it.
	// TODO, checkpoint support ResumeToken
	if conf.Options.SpecialSourceDBFlag == utils.VarSpecialSourceDBFlagAliyunServerless {
		if syncMode == utils.VarSyncModeIncr {
			return syncMode, nil, int64(0), nil
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
		LOG.Info("sync mode run %v", utils.VarSyncModeIncr)
		return utils.VarSyncModeIncr, startTsMap, int64(0), nil
	} else if syncMode == utils.VarSyncModeIncr || conf.Options.Tunnel != utils.VarTunnelDirect {
		// bugfix v2.4.11: if can not run incr sync directly, return error when sync_mode == "incr"
		// bugfix v2.4.12: return error when tunnel != "direct"
		return "", nil, int64(0), fmt.Errorf("start time illegal, can't run incr sync")
	} else {
		return utils.VarSyncModeAll, nil, utils.TimestampToInt64(smallestNewTs), nil
	}
}
