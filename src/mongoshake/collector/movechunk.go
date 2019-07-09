package collector

import (
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	utils "mongoshake/common"
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
	barrierMap map[int]*MCIBarrier
	mutex      sync.Mutex
}

type MCIItem struct {
	syncId    int
	timestamp bson.MongoTimestamp
}

type MCIBarrier struct {
	barrierPipe chan interface{}
	tag         int
}

// do info.mutex.Lock before Barrier
func (info *MoveChunkInfo) Barrier(syncId int, key MoveChunkKey, partialLog *oplog.PartialLog, tag int) {
	barrier := &MCIBarrier{barrierPipe: make(chan interface{}), tag: tag}
	info.barrierMap[syncId] = barrier
	info.mutex.Unlock()
	<-barrier.barrierPipe
	if tag == 1 {
		info.mutex.Lock()
		if partialLog.Operation == "i" {
			LOG.Info("syncer %v add insert move chunk oplog[%v %v]",
				syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
			info.insertMap[syncId] = partialLog.Timestamp
		} else if info.deleteItem == nil {
			LOG.Info("syncer %v add delete move chunk oplog[%v %v]",
				syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
			info.deleteItem = &MCIItem{syncId: syncId, timestamp: partialLog.Timestamp}
		} else {
			LOG.Crashf("syncer %v meet delete oplog[%v %v] after barrier",
				syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		}
		info.mutex.Unlock()
	}
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
	var mcMapItemList []*MCMapItem
	manager.moveChunkLock.Lock()
	for key, info := range manager.moveChunkMap {
		mcMapItemList = append(mcMapItemList, &MCMapItem{key: key, info: info})
	}
	manager.moveChunkLock.Unlock()
	LOG.Info("move chunk map len=%v", len(mcMapItemList))
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
				if barrier, ok := info.barrierMap[minInsertTsSyncerId]; ok {
					LOG.Info("syncer %v eliminate insert barrier[%v %v]", minInsertTsSyncerId,
						item.key.String(), utils.TimestampToOplogString(info.insertMap[minInsertTsSyncerId]))
					delete(info.barrierMap, minInsertTsSyncerId)
					close(barrier.barrierPipe)
					if barrier.tag == 1 {
						tag = 1
					}
				}
				LOG.Info("syncer %v remove insert move chunk oplog[%v %v]", minInsertTsSyncerId,
					item.key.String(), utils.TimestampToOplogString(info.insertMap[minInsertTsSyncerId]))
				delete(info.insertMap, minInsertTsSyncerId)
				if barrier, ok := info.barrierMap[info.deleteItem.syncId]; ok {
					LOG.Info("syncer %v eliminate delete barrier[%v %v]", info.deleteItem.syncId,
						item.key.String(), utils.TimestampToOplogString(info.deleteItem.timestamp))
					delete(info.barrierMap, info.deleteItem.syncId)
					close(barrier.barrierPipe)
					if barrier.tag == 1 {
						tag = 1
					}
				}
				LOG.Info("syncer %v remove delete move chunk oplog[%v %v]", info.deleteItem.syncId,
					item.key.String(), utils.TimestampToOplogString(info.deleteItem.timestamp))
				info.deleteItem = nil
				if tag == 0 && len(info.insertMap) == 0 && len(info.barrierMap) == 0 {
					manager.moveChunkLock.Lock()
					LOG.Info("move chunk map remove move chunk key[%v]", item.key.String())
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

	if moveChunkFilter.Filter(partialLog) {
		if oplogId := oplog.GetKey(partialLog.Object, ""); oplogId != nil {
			key := MoveChunkKey{id: oplogId, namespace: partialLog.Namespace}
			manager.moveChunkLock.Lock()
			info, ok := manager.moveChunkMap[key]
			if ok {
				manager.moveChunkLock.Unlock()
				// info.mutex unlock in Barrier
				info.mutex.Lock()
				manager.moveChunkLock.Lock()
				if _, ok := manager.moveChunkMap[key]; !ok {
					LOG.Info("syncer %v access move chunk key[%v] when removed by eliminateBarrier", syncId, key.String())
					manager.moveChunkMap[key] = info
				}
				manager.moveChunkLock.Unlock()
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when %v move chunk oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(ts), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, key, partialLog, 1)
				} else if info.deleteItem != nil && info.deleteItem.syncId == syncId {
					// migrate delete at the same syncer
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(info.deleteItem.timestamp), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, key, partialLog, 1)
				} else {
					// migrate insert/delete at the different syncer
					info.AddMoveChunk(syncId, key, partialLog)
					info.mutex.Unlock()
				}
			} else {
				LOG.Info("syncer %v create move chunk info when move chunk oplog found[%v %v]",
					syncId, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
				info := &MoveChunkInfo{
					insertMap:  make(map[int]bson.MongoTimestamp),
					barrierMap: make(map[int]*MCIBarrier),
				}
				info.AddMoveChunk(syncId, key, partialLog)
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
				info.mutex.Lock()
				if ts, ok := info.insertMap[syncId]; ok {
					// migrate insert at the same syncer
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v %v]",
						syncId, utils.TimestampToOplogString(ts), key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(syncId, key, partialLog, 0)
				} else {
					if info.deleteItem != nil && info.deleteItem.syncId == syncId {
						LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v %v] illegal",
							syncId, utils.TimestampToOplogString(info.deleteItem.timestamp),
							key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					}
					info.mutex.Unlock()
				}
			}
		}
	}
}
