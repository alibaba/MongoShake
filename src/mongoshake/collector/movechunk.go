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
		syncInfoMap:  make(map[string]*SyncInfo),
	}
	return manager
}

type MoveChunkManager struct {
	syncInfoMap map[string]*SyncInfo
	// ensure the order of oplog when move chunk occur
	moveChunkMap  map[MoveChunkKey]*MoveChunkInfo
	moveChunkLock sync.Mutex
}

func (manager *MoveChunkManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncInfoMap[syncer.replset] = &SyncInfo{syncer: syncer}
}

func (manager *MoveChunkManager) start() {
	nimo.GoRoutineInLoop(manager.eliminateBarrier)
}

func (manager *MoveChunkManager) barrierProbe(key MoveChunkKey, timestamp bson.MongoTimestamp) bool {
	result := true
	for _, syncInfo := range manager.syncInfoMap {
		// worker ack must exceed timestamp of insert/delete move chunk oplog
		syncInfo.mutex.Lock()
		for _, worker := range syncInfo.syncer.batcher.workerGroup {
			unack := atomic.LoadInt64(&worker.unack)
			ack := atomic.LoadInt64(&worker.ack)
			if syncInfo.barrierChan != nil && ack == unack &&
				(syncInfo.syncTs > timestamp || syncInfo.barrierKey == key) {
				continue
			}
			if syncInfo.barrierChan == nil && syncInfo.syncTs >= timestamp &&
				(ack == unack || ack < unack && ack > int64(timestamp)) {
				continue
			}
			syncInfo.mutex.Unlock()
			return false
		}
		syncInfo.mutex.Unlock()
	}
	return result
}

type SyncInfo struct {
	syncer      *OplogSyncer
	syncTs		bson.MongoTimestamp
	barrierKey	MoveChunkKey
	barrierChan chan interface{}
	mutex       sync.Mutex
}

func (syncInfo *SyncInfo) DeleteBarrier() {
	syncInfo.mutex.Lock()
	syncInfo.barrierKey = MoveChunkKey{}
	syncInfo.barrierChan = nil
	syncInfo.mutex.Unlock()
}

func (syncInfo *SyncInfo) BlockOplog(replset string, partialLog *oplog.PartialLog) {
	var barrierChan chan interface{}
	syncInfo.mutex.Lock()
	if syncInfo.syncTs < partialLog.Timestamp {
		syncInfo.syncTs = partialLog.Timestamp
	}
	barrierChan = syncInfo.barrierChan
	syncInfo.mutex.Unlock()
	// wait for barrier channel must be out of mutex, because eliminateBarrier need to delete barrier
	if barrierChan != nil {
		LOG.Info("syncer %v wait barrier", replset)
		<-barrierChan
		LOG.Info("syncer %v wait barrier finish", replset)
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
	insertMap  map[string]bson.MongoTimestamp
	// the size of deleteMap will not more than 1
	deleteItem *MCIItem
	barrierMap map[string]chan interface{}
}

type MCIItem struct {
	replset   string
	timestamp bson.MongoTimestamp
}

func (info *MoveChunkInfo) Barrier(replset string, syncInfo *SyncInfo, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if _, ok := info.barrierMap[replset]; ok {
		LOG.Crashf("syncer %v has more than one barrier in barrierMap when move chunk oplog found[%v %v]",
			replset, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
	barrierChan := make(chan interface{})
	info.barrierMap[replset] = barrierChan
	syncInfo.mutex.Lock()
	if syncInfo.barrierChan != nil {
		LOG.Crashf("syncer %v has more than one barrier in syncInfoMap when move chunk oplog found[%v %v]",
			replset, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
	syncInfo.barrierKey = key
	syncInfo.barrierChan = barrierChan
	syncInfo.mutex.Unlock()
}

func (info *MoveChunkInfo) AddMoveChunk(replset string, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if partialLog.Operation == "d" {
		if info.deleteItem != nil {
			LOG.Crashf("move chunk manager has more than one deleteItem[%v] when delete move chunk oplog found[%v %v]",
				info.deleteItem, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		}
		LOG.Info("syncer %v add delete move chunk oplog[%v %v]",
			replset, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		info.deleteItem = &MCIItem{replset: replset, timestamp: partialLog.Timestamp}
	} else if partialLog.Operation == "i" {
		LOG.Info("syncer %v add insert move chunk oplog[%v %v]",
			replset, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
		info.insertMap[replset] = partialLog.Timestamp
	} else {
		LOG.Crashf("unsupported %v move chunk oplog[%v %v]",
			partialLog.Operation, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
	}
}

func (manager *MoveChunkManager) eliminateBarrier() {

	manager.moveChunkLock.Lock()
	LOG.Info("move chunk map len=%v", len(manager.moveChunkMap))
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		for _, worker := range syncInfo.syncer.batcher.workerGroup {
			ack := bson.MongoTimestamp(atomic.LoadInt64(&worker.ack))
			unack := bson.MongoTimestamp(atomic.LoadInt64(&worker.unack))
			LOG.Info("syncer %v worker ack[%v] unack[%v] syncTs[%v] barrierKey[%v]", replset,
				utils.TimestampToOplogString(ack), utils.TimestampToOplogString(unack),
				utils.TimestampToOplogString(syncInfo.syncTs), syncInfo.barrierKey)
		}
		syncInfo.mutex.Unlock()
	}

	var deleteKeyList []MoveChunkKey
	for key, info := range manager.moveChunkMap {
		if info.deleteItem != nil {
			deleteReplset := info.deleteItem.replset
			if !manager.barrierProbe(key, info.deleteItem.timestamp) {
				continue
			}
			minInsertReplset := ""
			var minInsertTs bson.MongoTimestamp
			for replset, insertTs := range info.insertMap {
				if minInsertReplset == "" || minInsertTs > insertTs {
					minInsertReplset = replset
					minInsertTs = insertTs
				}
			}
			if minInsertReplset != "" {
				if barrier, ok := info.barrierMap[minInsertReplset]; ok {
					LOG.Info("syncer %v eliminate insert barrier[%v %v]", minInsertReplset,
						key.String(), utils.TimestampToOplogString(info.insertMap[minInsertReplset]))
					manager.syncInfoMap[minInsertReplset].DeleteBarrier()
					delete(info.barrierMap, minInsertReplset)
					close(barrier)
				}
				LOG.Info("syncer %v remove insert move chunk oplog[%v %v]", minInsertReplset,
					key.String(), utils.TimestampToOplogString(info.insertMap[minInsertReplset]))
				delete(info.insertMap, minInsertReplset)
				if barrier, ok := info.barrierMap[deleteReplset]; ok {
					LOG.Info("syncer %v eliminate delete barrier[%v %v]", deleteReplset,
						key.String(), utils.TimestampToOplogString(info.deleteItem.timestamp))
					manager.syncInfoMap[deleteReplset].DeleteBarrier()
					delete(info.barrierMap, deleteReplset)
					close(barrier)
				}
				LOG.Info("syncer %v remove delete move chunk oplog[%v %v]", deleteReplset,
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
	time.Sleep(1 * time.Second)
}

// TODO migrate insert/update/delete may occur multiple times
func (manager *MoveChunkManager) barrierBlock(replset string, partialLog *oplog.PartialLog) bool {
	syncInfo := manager.syncInfoMap[replset]
	syncInfo.BlockOplog(replset, partialLog)

	barrier := false
	manager.moveChunkLock.Lock()
	if moveChunkFilter.Filter(partialLog) {
		// barrier == true if the syncer already has a insert/delete move chunk oplog before
		if oplogId := oplog.GetKey(partialLog.Object, ""); oplogId != nil {
			key := MoveChunkKey{id: oplogId, namespace: partialLog.Namespace}
			info, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := info.insertMap[replset]; ok {
					LOG.Info("syncer %v meet insert barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToOplogString(ts), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(replset, syncInfo, key, partialLog)
					barrier = true
				} else if info.deleteItem != nil && info.deleteItem.replset == replset {
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToOplogString(info.deleteItem.timestamp), partialLog.Operation,
						key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(replset, syncInfo, key, partialLog)
					barrier = true
				} else {
					// find a insert/delete move chunk firstly
					info.AddMoveChunk(replset, key, partialLog)
				}
			} else {
				LOG.Info("syncer %v create move chunk info when move chunk oplog found[%v %v]",
					replset, key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
				info := &MoveChunkInfo{
					insertMap:  make(map[string]bson.MongoTimestamp),
					barrierMap: make(map[string]chan interface{}),
				}
				info.AddMoveChunk(replset, key, partialLog)
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
				if ts, ok := info.insertMap[replset]; ok {
					// barrier == true if the syncer already has a insert move chunk oplog before
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v %v]",
						replset, utils.TimestampToOplogString(ts), key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					info.Barrier(replset, manager.syncInfoMap[replset], key, partialLog)
					barrier = true
				} else {
					if info.deleteItem != nil && info.deleteItem.replset == replset {
						LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v %v] illegal",
							replset, utils.TimestampToOplogString(info.deleteItem.timestamp),
							key.String(), utils.TimestampToOplogString(partialLog.Timestamp))
					}
				}
			}
		}
	}
	manager.moveChunkLock.Unlock()
	return barrier
}
