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

const (
	MoveChunkSyncTSName = "syncts"
	MoveChunkKeyName    = "key"
	MoveChunkInsertMap  = "insertmap"
	MoveChunkDeleteItem = "deleteitem"
)

func NewMoveChunkManager() *MoveChunkManager {
	manager := &MoveChunkManager{
		moveChunkMap: make(map[MoveChunkKey]*MoveChunkValue),
		syncInfoMap:  make(map[string]*SyncerMoveChunk),
	}
	return manager
}

type MoveChunkManager struct {
	syncInfoMap map[string]*SyncerMoveChunk
	// ensure the order of oplog when move chunk occur
	moveChunkMap  map[MoveChunkKey]*MoveChunkValue
	moveChunkLock sync.Mutex
}

func (manager *MoveChunkManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncInfoMap[syncer.replset] = &SyncerMoveChunk{syncer: syncer}
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

type SyncerMoveChunk struct {
	syncer      *OplogSyncer
	syncTs      bson.MongoTimestamp
	barrierKey  MoveChunkKey
	barrierChan chan interface{}
	mutex       sync.Mutex
}

func (syncInfo *SyncerMoveChunk) DeleteBarrier() {
	syncInfo.mutex.Lock()
	syncInfo.barrierKey = MoveChunkKey{}
	syncInfo.barrierChan = nil
	syncInfo.mutex.Unlock()
}

func (syncInfo *SyncerMoveChunk) BlockOplog(replset string, partialLog *oplog.PartialLog) {
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

type MoveChunkValue struct {
	insertMap map[string]bson.MongoTimestamp
	// the size of deleteMap will not more than 1
	deleteItem *MCIItem
}

type MCIItem struct {
	replset   string
	timestamp bson.MongoTimestamp
}

func (value *MoveChunkValue) Barrier(syncInfo *SyncerMoveChunk, key MoveChunkKey, partialLog *oplog.PartialLog) {
	syncInfo.mutex.Lock()
	if syncInfo.barrierChan != nil {
		LOG.Crashf("syncer %v has more than one barrier in syncInfoMap when move chunk oplog found[%v %v]",
			syncInfo.syncer.replset, key.String(), utils.TimestampToLog(partialLog.Timestamp))
	}
	syncInfo.barrierKey = key
	syncInfo.barrierChan = make(chan interface{})
	syncInfo.mutex.Unlock()
}

func (value *MoveChunkValue) AddMoveChunk(replset string, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if partialLog.Operation == "d" {
		if value.deleteItem != nil {
			LOG.Crashf("move chunk manager has more than one deleteItem[%v] when delete move chunk oplog found[%v %v]",
				value.deleteItem, key.String(), utils.TimestampToLog(partialLog.Timestamp))
		}
		LOG.Info("syncer %v add delete move chunk oplog[%v %v]",
			replset, key.String(), utils.TimestampToLog(partialLog.Timestamp))
		value.deleteItem = &MCIItem{replset: replset, timestamp: partialLog.Timestamp}
	} else if partialLog.Operation == "i" {
		LOG.Info("syncer %v add insert move chunk oplog[%v %v]",
			replset, key.String(), utils.TimestampToLog(partialLog.Timestamp))
		value.insertMap[replset] = partialLog.Timestamp
	} else {
		LOG.Crashf("unsupported %v move chunk oplog[%v %v]",
			partialLog.Operation, key.String(), utils.TimestampToLog(partialLog.Timestamp))
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
				utils.TimestampToLog(ack), utils.TimestampToLog(unack),
				utils.TimestampToLog(syncInfo.syncTs), syncInfo.barrierKey)
		}
		syncInfo.mutex.Unlock()
	}
	var deleteKeyList []MoveChunkKey
	for key, value := range manager.moveChunkMap {
		if value.deleteItem != nil {
			deleteReplset := value.deleteItem.replset
			if !manager.barrierProbe(key, value.deleteItem.timestamp) {
				continue
			}
			minInsertReplset := ""
			var minInsertTs bson.MongoTimestamp
			for replset, insertTs := range value.insertMap {
				if minInsertReplset == "" || minInsertTs > insertTs {
					minInsertReplset = replset
					minInsertTs = insertTs
				}
			}
			if minInsertReplset != "" {
				// remove insert move chunk
				insertBarrier := manager.syncInfoMap[minInsertReplset].barrierChan
				if insertBarrier != nil {
					LOG.Info("syncer %v eliminate insert barrier[%v %v]", minInsertReplset,
						key.String(), utils.TimestampToLog(value.insertMap[minInsertReplset]))
					manager.syncInfoMap[minInsertReplset].DeleteBarrier()
					close(insertBarrier)
				}
				LOG.Info("syncer %v remove insert move chunk oplog[%v %v]", minInsertReplset,
					key.String(), utils.TimestampToLog(value.insertMap[minInsertReplset]))
				delete(value.insertMap, minInsertReplset)
				// remove delete move chunk
				deleteBarrier := manager.syncInfoMap[deleteReplset].barrierChan
				if deleteBarrier != nil {
					LOG.Info("syncer %v eliminate delete barrier[%v %v]", deleteReplset,
						key.String(), utils.TimestampToLog(value.deleteItem.timestamp))
					manager.syncInfoMap[deleteReplset].DeleteBarrier()
					close(deleteBarrier)
				}
				LOG.Info("syncer %v remove delete move chunk oplog[%v %v]", deleteReplset,
					key.String(), utils.TimestampToLog(value.deleteItem.timestamp))
				value.deleteItem = nil
				if len(value.insertMap) == 0 {
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
			value, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := value.insertMap[replset]; ok {
					LOG.Info("syncer %v meet insert barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToLog(ts), partialLog.Operation,
						key.String(), utils.TimestampToLog(partialLog.Timestamp))
					value.Barrier(syncInfo, key, partialLog)
					barrier = true
				} else if value.deleteItem != nil && value.deleteItem.replset == replset {
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToLog(value.deleteItem.timestamp), partialLog.Operation,
						key.String(), utils.TimestampToLog(partialLog.Timestamp))
					value.Barrier(syncInfo, key, partialLog)
					barrier = true
				} else {
					// find a insert/delete move chunk firstly
					value.AddMoveChunk(replset, key, partialLog)
				}
			} else {
				LOG.Info("syncer %v create move chunk value when move chunk oplog found[%v %v]",
					replset, key.String(), utils.TimestampToLog(partialLog.Timestamp))
				value := &MoveChunkValue{
					insertMap: make(map[string]bson.MongoTimestamp),
				}
				value.AddMoveChunk(replset, key, partialLog)
				manager.moveChunkMap[key] = value
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
			value, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := value.insertMap[replset]; ok {
					// barrier == true if the syncer already has a insert move chunk oplog before
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v %v]",
						replset, utils.TimestampToLog(ts), key.String(), utils.TimestampToLog(partialLog.Timestamp))
					value.Barrier(manager.syncInfoMap[replset], key, partialLog)
					barrier = true
				} else {
					if value.deleteItem != nil && value.deleteItem.replset == replset {
						LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v %v] illegal",
							replset, utils.TimestampToLog(value.deleteItem.timestamp),
							key.String(), utils.TimestampToLog(partialLog.Timestamp))
					}
				}
			}
		}
	}
	manager.moveChunkLock.Unlock()
	return barrier
}

func (manager *MoveChunkManager) Load(conn *utils.MongoConn, db string, tablePrefix string) error {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	iter := conn.Session.DB(db).C(tablePrefix + "_mvck_syncer").Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[CheckpointName].(string)
		syncTs, ok2 := ckptDoc[MoveChunkSyncTSName].(bson.MongoTimestamp)
		if !ok1 || !ok2 {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v", ckptDoc)
		} else if syncInfo, ok := manager.syncInfoMap[replset]; !ok {
			return fmt.Errorf("MoveChunkManager load checkpoint unknown replset %v", ckptDoc)
		} else {
			syncInfo.syncTs = syncTs
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	iter = conn.Session.DB(db).C(tablePrefix + "_mvck_map").Find(bson.M{}).Iter()
	for iter.Next(ckptDoc) {
		key, ok1 := ckptDoc[CheckpointName].(MoveChunkKey)
		insertMap, ok2 := ckptDoc[MoveChunkInsertMap].(map[string]bson.MongoTimestamp)
		deleteItem, ok3 := ckptDoc[MoveChunkDeleteItem].(MCIItem)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v", ckptDoc)
		} else {
			manager.moveChunkMap[key] = &MoveChunkValue{insertMap:insertMap, deleteItem:&deleteItem}
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	return nil
}

func (manager *MoveChunkManager) PrepareFlush() error {
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		if syncInfo.barrierChan != nil {
			syncInfo.mutex.Unlock()
			return fmt.Errorf("sycner %v meet move chunk barrier %v", replset, syncInfo.barrierKey)
		}
		syncInfo.mutex.Unlock()
	}
	return nil
}

func (manager *MoveChunkManager) Flush(conn *utils.MongoConn, db string, tablePrefix string) error {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		ckptDoc := map[string]interface{}{
			CheckpointName:      replset,
			MoveChunkSyncTSName: syncInfo.syncTs,
		}
		if _, err := conn.Session.DB(db).C(tablePrefix+"_mvck_syncer").
			Upsert(bson.M{CheckpointName: replset}, ckptDoc); err != nil {
			syncInfo.mutex.Unlock()
			LOG.Critical("MoveChunkManager flush checkpoint syncer %v upsert error %v", ckptDoc, err)
			return err
		}
		syncInfo.mutex.Unlock()
	}
	table := tablePrefix + "_mvck_map"
	if err := conn.Session.DB(db).C(table).DropCollection(); err != nil {
		LOG.Critical("MoveChunkManager flush checkpoint drop collection %v error %v", table, err)
		return err
	}
	for key, value := range manager.moveChunkMap {
		ckptDoc := map[string]interface{}{
			MoveChunkKeyName:    key,
			MoveChunkInsertMap:  value.insertMap,
			MoveChunkDeleteItem: *value.deleteItem,
		}
		if err := conn.Session.DB(db).C(table).Insert(ckptDoc); err != nil {
			LOG.Critical("MoveChunkManager flush checkpoint map %v upsert error %v", ckptDoc, err)
			return err
		}
	}
	return nil
}
