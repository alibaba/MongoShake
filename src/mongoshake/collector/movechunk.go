package collector

import (
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/oplog"
	"sync"
	"time"
)

func NewMoveChunkManager(syncNum int) *MoveChunkManager {
	manager := &MoveChunkManager{
		moveChunkMap: make(map[MoveChunkKey]*MoveChunkInfo),
		syncInfoMap:  make(map[int]*SyncInfo),
	}
	for syncId := 0; syncId < syncNum; syncId++ {
		manager.syncInfoMap[syncId] = &SyncInfo{timestamp: 0}
	}
	nimo.GoRoutineInLoop(manager.eliminateBarrier)
	return manager
}

type MoveChunkManager struct {
	syncInfoMap map[int]*SyncInfo
	// ensure the order of oplog when move chunk occur
	moveChunkMap  map[MoveChunkKey]*MoveChunkInfo
	moveChunkLock sync.Mutex
}

type SyncInfo struct {
	timestamp bson.MongoTimestamp
	mutex     sync.Mutex
}

type MoveChunkKey struct {
	id        interface{}
	namespace string
}

type MoveChunkInfo struct {
	insertMap map[int]bson.MongoTimestamp
	// the size of deleteMap will not more than 1
	deleteItem *MCIItem
	barrierMap map[int]*MCIBarrier
	mutex      sync.Mutex
}

type MCIItem struct {
	syncId    int
	timestamp bson.MongoTimestamp
}

type MCIBarrier struct {
	op          string
	barrierPipe chan interface{}
	tag         int
}

func (info *MoveChunkInfo) Barrier(syncId int, op string, timestamp bson.MongoTimestamp, tag int) {
	info.mutex.Lock()
	barrier := &MCIBarrier{op: op, barrierPipe: make(chan interface{}), tag: tag}
	info.barrierMap[syncId] = barrier
	info.mutex.Unlock()
	<-barrier.barrierPipe
	if tag == 1 {
		info.mutex.Lock()
		if op == "i" {
			info.insertMap[syncId] = timestamp
		} else if info.deleteItem == nil {
			info.deleteItem = &MCIItem{syncId: syncId, timestamp: timestamp}
		} else {
			LOG.Crashf("syncer %v meet delete oplog ts[%v] after barrier", syncId, timestamp)
		}
		info.mutex.Unlock()
	}
}

func (info *MoveChunkInfo) AddMoveChunk(syncId int, partialLog *oplog.PartialLog) {
	info.mutex.Lock()
	if partialLog.Operation == "d" {
		if info.deleteItem != nil {
			LOG.Crashf("move chunk manager has more than one deleteItem[%v] when delete move chunk oplog found[%v]",
				info.deleteItem, partialLog)
		}
		LOG.Info("syncer %v add delete move chunk oplog found[%v]", syncId, partialLog)
		info.deleteItem = &MCIItem{syncId: syncId, timestamp: partialLog.Timestamp}
	} else if partialLog.Operation == "i" {
		LOG.Info("syncer %v add insert move chunk oplog found[%v]", syncId, partialLog)
		info.insertMap[syncId] = partialLog.Timestamp
	} else {
		LOG.Warn("unsupported move chunk oplog found[%v]", partialLog)
	}
	info.mutex.Unlock()
}

func (manager *MoveChunkManager) eliminateBarrier() {
	var mcMapItemList []*MCMapItem
	manager.moveChunkLock.Lock()
	for key, info := range manager.moveChunkMap {
		mcMapItemList = append(mcMapItemList, &MCMapItem{key: key, info: info})
	}
	manager.moveChunkLock.Unlock()
	for _, item := range mcMapItemList {
		info := item.info
		info.mutex.Lock()
		if info.deleteItem != nil {
			minInsertTsSyncerId := -1
			var minInsertTs bson.MongoTimestamp
			for syncerId, insertTs := range info.insertMap {
				found := true
				for _, syncInfo := range manager.syncInfoMap {
					syncInfo.mutex.Lock()
					if syncInfo.timestamp < insertTs {
						found = false
					}
					syncInfo.mutex.Unlock()
					if !found {
						break
					}
				}
				if found {

					if minInsertTsSyncerId == -1 || minInsertTs > insertTs {
						minInsertTsSyncerId = syncerId
						minInsertTs = insertTs
					}
				}
			}
			if minInsertTsSyncerId != -1 {
				tag := 0
				if barrier, ok := info.barrierMap[info.deleteItem.syncId]; ok {
					LOG.Info("syncer %v remove delete move chunk oplog[%v %v]",
						info.deleteItem.syncId, item.key, info.deleteItem.timestamp)
					delete(info.barrierMap, info.deleteItem.syncId)
					if barrier.op == "d" {
						LOG.Info("MoveChunkManager eliminate delete barrier[%v %v] from syncer %v",
							item.key, info.deleteItem.timestamp, info.deleteItem.syncId)
						close(barrier.barrierPipe)
					}
					if barrier.tag == 1 {
						tag = 1
					}
				}
				info.deleteItem = nil
				if barrier, ok := info.barrierMap[minInsertTsSyncerId]; ok {
					LOG.Info("syncer %v remove insert move chunk oplog[%v %v]",
						minInsertTsSyncerId, item.key, info.insertMap[minInsertTsSyncerId])
					delete(info.barrierMap, minInsertTsSyncerId)
					if barrier.op == "i" {
						LOG.Info("MoveChunkManager eliminate insert barrier[%v %v] from syncer %v",
							item.key, info.insertMap[minInsertTsSyncerId], minInsertTsSyncerId)
						close(barrier.barrierPipe)
					}
					if barrier.tag == 1 {
						tag = 1
					}
				}
				delete(info.insertMap, minInsertTsSyncerId)
				if tag == 0 && len(info.insertMap) == 0 && len(info.barrierMap) == 0 {
					manager.moveChunkLock.Lock()
					LOG.Info("move chunk map remove move chunk key[%v]", item.key)
					delete(manager.moveChunkMap, item.key)
					manager.moveChunkLock.Unlock()
				}
			}
		}
		info.mutex.Unlock()
	}
	time.Sleep(5 * time.Second)
}

type MCMapItem struct {
	key  MoveChunkKey
	info *MoveChunkInfo
}

// TODO migrate insert/update/delete may occur multiple times
func (manager *MoveChunkManager) barrierBlock(syncId int, partialLog *oplog.PartialLog) {
	syncInfo := manager.syncInfoMap[syncId]
	syncInfo.mutex.Lock()
	if partialLog.Timestamp > syncInfo.timestamp {
		syncInfo.timestamp = partialLog.Timestamp
	}
	syncInfo.mutex.Unlock()

	//LOG.Info("syncer %v get oplog[%v]", syncId, partialLog)

	if moveChunkFilter.Filter(partialLog) {
		if oplogId := oplog.GetKey(partialLog.Object, ""); oplogId != nil {
			key := MoveChunkKey{id: oplogId, namespace: partialLog.Namespace}
			manager.moveChunkLock.Lock()
			info, ok := manager.moveChunkMap[key]
			manager.moveChunkLock.Unlock()
			if ok {
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when move chunk oplog found[%v]",
						syncId, ts, partialLog)
					info.Barrier(syncId, "i", ts, 1)
				} else if info.deleteItem != nil && info.deleteItem.syncId == syncId {
					// migrate delete at the same syncer
					LOG.Info("syncer %v meet delete barrier ts[%v] when move chunk oplog found[%v]",
						syncId, ts, partialLog)
					info.Barrier(syncId, "d", ts, 1)
				} else {
					// migrate insert/delete at the different syncer
					info.AddMoveChunk(syncId, partialLog)
				}
			} else {
				LOG.Info("syncer %v create move chunk info when move chunk oplog found[%v]", syncId, partialLog)
				info := &MoveChunkInfo{
					insertMap:  make(map[int]bson.MongoTimestamp),
					barrierMap: make(map[int]*MCIBarrier),
				}
				info.AddMoveChunk(syncId, partialLog)
				manager.moveChunkLock.Lock()
				manager.moveChunkMap[key] = info
				manager.moveChunkLock.Unlock()
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
			manager.moveChunkLock.Lock()
			info, ok := manager.moveChunkMap[key]
			manager.moveChunkLock.Unlock()
			if ok {
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v]",
						syncId, ts, partialLog)
					info.Barrier(syncId, "i", ts, 0)
				}
				if info.deleteItem != nil && info.deleteItem.syncId == syncId {
					LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v] illegal",
						syncId, info.deleteItem.timestamp, partialLog)
				}
			}
		}
	}
}
