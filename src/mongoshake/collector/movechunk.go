package collector

import (
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"sync"
	"sync/atomic"
	"time"
)

func NewMoveChunkManager() *MoveChunkManager {
	manager := &MoveChunkManager{
		moveChunkMap: make(map[MoveChunkKey]*MoveChunkInfo),
		syncInfoMap:  make(map[int]*SyncInfo),
	}
	return manager
}

type MoveChunkManager struct {
	syncInfoMap map[int]*SyncInfo
	// ensure the order of oplog when move chunk occur
	moveChunkMap  map[MoveChunkKey]*MoveChunkInfo
	moveChunkLock sync.Mutex
}

func (manager *MoveChunkManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncInfoMap[syncer.id] = &SyncInfo{syncer: syncer}
}

func (manager *MoveChunkManager) start() {
	nimo.GoRoutineInLoop(manager.eliminateBarrier)
}

func (manager *MoveChunkManager) workerAckProbe(syncId int, timestamp bson.MongoTimestamp) bool {
	syncer := manager.syncInfoMap[syncId].syncer
	// worker ack must exceed timestamp of insert/delete move chunk oplog
	for _, worker := range syncer.batcher.workerGroup {
		unack := atomic.LoadInt64(&worker.unack)
		ack := atomic.LoadInt64(&worker.ack)
		if ack < unack {
			return false
		}
	}
	return true
}

type SyncInfo struct {
	syncer      *OplogSyncer
	barrierChan chan interface{}
	mutex       sync.Mutex
}

func (syncInfo *SyncInfo) DeleteBarrier() {
	syncInfo.mutex.Lock()
	syncInfo.barrierChan = nil
	syncInfo.mutex.Unlock()
}

func (syncInfo *SyncInfo) BlockOplog(syncId int, partialLog *oplog.PartialLog) {
	var barrierChan chan interface{}
	syncInfo.mutex.Lock()
	barrierChan = syncInfo.barrierChan
	syncInfo.mutex.Unlock()
	// wait barrier channel must out of mutex, because eliminateBarrier need to access timestamp of syncInfo
	if barrierChan != nil {
		LOG.Info("syncer %v wait barrier", syncId)
		<-barrierChan
		LOG.Info("syncer %v wait barrier finish", syncId)
	}
}

type MoveChunkKey struct {
	id        interface{}
	namespace string
}

func (key MoveChunkKey) String() string {
	if id, ok := key.id.(bson.ObjectId); ok {
		return fmt.Sprintf("{%x %v}", string(id), key.namespace)
	} else {
		return fmt.Sprintf("{%v %v}", key.id, key.namespace)
	}
}

type MoveChunkInfo struct {
	insertMap map[int]bson.MongoTimestamp
	// the size of deleteMap will not more than 1
	deleteItem *MCIItem
	barrierMap map[int]chan interface{}
}

type MCIItem struct {
	syncId    int
	timestamp bson.MongoTimestamp
}

func (info *MoveChunkInfo) Barrier(syncId int, syncInfo *SyncInfo, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if _, ok := info.barrierMap[syncId]; ok {
		LOG.Crashf("syncer %v has more than one barrier in barrierMap when move chunk oplog found[%v %v]",
			syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
	barrierChan := make(chan interface{})
	info.barrierMap[syncId] = barrierChan
	syncInfo.mutex.Lock()
	if syncInfo.barrierChan != nil {
		LOG.Crashf("syncer %v has more than one barrier in syncInfoMap when move chunk oplog found[%v %v]",
			syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
	syncInfo.barrierChan = barrierChan
	syncInfo.mutex.Unlock()
}

func (info *MoveChunkInfo) AddMoveChunk(syncId int, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if partialLog.Operation == "d" {
		if info.deleteItem != nil {
			LOG.Crashf("move chunk manager has more than one deleteItem[%v] when delete move chunk oplog found[%v %v]",
				info.deleteItem, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		}
		LOG.Info("syncer %v add delete move chunk oplog[%v %v]",
			syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		info.deleteItem = &MCIItem{syncId: syncId, timestamp: partialLog.Timestamp}
	} else if partialLog.Operation == "i" {
		LOG.Info("syncer %v add insert move chunk oplog[%v %v]",
			syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		info.insertMap[syncId] = partialLog.Timestamp
	} else {
		LOG.Crashf("unsupported %v move chunk oplog[%v %v]",
			partialLog.Operation, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
}

func (manager *MoveChunkManager) eliminateBarrier() {
	var deleteKeyList []MoveChunkKey
	manager.moveChunkLock.Lock()
	LOG.Info("move chunk map len=%v", len(manager.moveChunkMap))
	for syncId, syncInfo := range manager.syncInfoMap {
		for _, worker := range syncInfo.syncer.batcher.workerGroup {
			LOG.Info("syncer %v worker ack[%v] unack[%v]", syncId,
				atomic.LoadInt64(&worker.ack), atomic.LoadInt64(&worker.unack))
		}
	}

	for key, info := range manager.moveChunkMap {
		if info.deleteItem != nil {
			deleteSyncerId := info.deleteItem.syncId
			if !manager.workerAckProbe(deleteSyncerId, info.deleteItem.timestamp) {
				continue
			}

			minInsertTsSyncerId := -1
			var minInsertTs bson.MongoTimestamp
			for syncerId, insertTs := range info.insertMap {
				// if worker ack < insertTs, cannot remove insert move chunk
				if manager.workerAckProbe(syncerId, insertTs) {
					if minInsertTsSyncerId == -1 || minInsertTs > insertTs {
						minInsertTsSyncerId = syncerId
						minInsertTs = insertTs
					}
				}
			}
			if minInsertTsSyncerId != -1 {
				if barrier, ok := info.barrierMap[minInsertTsSyncerId]; ok {
					LOG.Info("syncer %v eliminate insert barrier[%v %v]", minInsertTsSyncerId,
						key.String(), utils.TimestampToOplogString(info.insertMap[minInsertTsSyncerId]))
					manager.syncInfoMap[minInsertTsSyncerId].DeleteBarrier()
					delete(info.barrierMap, minInsertTsSyncerId)
					close(barrier)
				}
				LOG.Info("syncer %v remove insert move chunk oplog[%v %v]", minInsertTsSyncerId,
					key.String(), utils.TimestampToOplogString(info.insertMap[minInsertTsSyncerId]))
				delete(info.insertMap, minInsertTsSyncerId)
				if barrier, ok := info.barrierMap[deleteSyncerId]; ok {
					LOG.Info("syncer %v eliminate delete barrier[%v %v]", deleteSyncerId,
						key.String(), utils.TimestampToOplogString(info.deleteItem.timestamp))
					manager.syncInfoMap[deleteSyncerId].DeleteBarrier()
					delete(info.barrierMap, deleteSyncerId)
					close(barrier)
				}
				LOG.Info("syncer %v remove delete move chunk oplog[%v %v]", deleteSyncerId,
					key.String(), utils.TimestampToOplogString(info.deleteItem.timestamp))
				info.deleteItem = nil
				if len(info.insertMap) == 0 && len(info.barrierMap) == 0 {
					LOG.Info("move chunk map remove move chunk key[%v]", key.String())
					deleteKeyList = append(deleteKeyList, key)

				}
			}
		}
	}
	if len(deleteKeyList) > 0 {
		for _, key := range deleteKeyList {
			delete(manager.moveChunkMap, key)
		}
	}
	manager.moveChunkLock.Unlock()
	time.Sleep(5 * time.Second)
}

// TODO migrate insert/update/delete may occur multiple times
func (manager *MoveChunkManager) barrierBlock(syncId int, partialLog *oplog.PartialLog) bool {
	syncInfo := manager.syncInfoMap[syncId]
	syncInfo.BlockOplog(syncId, partialLog)

	barrier := false
	manager.moveChunkLock.Lock()
	if moveChunkFilter.Filter(partialLog) {
		if oplogId := oplog.GetKey(partialLog.Object, ""); oplogId != nil {
			key := MoveChunkKey{id: oplogId, namespace: partialLog.Namespace}
			info, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when %v move chunk oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(ts), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, syncInfo, key, partialLog)
					barrier = true
				} else if info.deleteItem != nil && info.deleteItem.syncId == syncId {
					// migrate delete at the same syncer
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(info.deleteItem.timestamp), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, syncInfo, key, partialLog)
					barrier = true
				} else {
					// migrate insert/delete at the different syncer
					info.AddMoveChunk(syncId, key, partialLog)
				}
			} else {
				LOG.Info("syncer %v create move chunk info when move chunk oplog found[%v %v]",
					syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
				info := &MoveChunkInfo{
					insertMap:  make(map[int]bson.MongoTimestamp),
					barrierMap: make(map[int]chan interface{}),
				}
				info.AddMoveChunk(syncId, key, partialLog)
				manager.moveChunkMap[key] = info
			}
		}
	} else {
		// when move chuck from A to B, block operation for _id record at B when between migrate insert at B and migrate delete at A
		var oplogId interface{}
		if partialLog.Operation == "u" {
			if id := partialLog.Query[oplog.PrimaryKey]; id != nil {
				oplogId = id
			}
		} else if id := oplog.GetKey(partialLog.Object, ""); id != nil {
			oplogId = id
		}
		if oplogId != nil {
			key := MoveChunkKey{id: oplogId, namespace: partialLog.Namespace}
			info, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(ts), key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, manager.syncInfoMap[syncId], key, partialLog)
					barrier = true
				} else {
					if info.deleteItem != nil && info.deleteItem.syncId == syncId {
						LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v %v] illegal",
							syncId, utils.TimestampToOplogString(info.deleteItem.timestamp),
							key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					}
				}
			}
		}
	}
	manager.moveChunkLock.Unlock()
	return barrier
}
