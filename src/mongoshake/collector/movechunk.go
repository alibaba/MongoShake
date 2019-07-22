package collector

import (
	"encoding/json"
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
	MoveChunkSyncTSName  = "syncTs"
	MoveChunkOfferTSName = "offerTs"

	MoveChunkKeyName    = "key"
	MoveChunkInsertMap  = "insertMap"
	MoveChunkDeleteItem = "deleteItem"
	MoveChunkBufferSize = 1000
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
				(syncInfo.offerTs > timestamp || syncInfo.barrierKey == key) {
				continue
			}
			if syncInfo.barrierChan == nil && syncInfo.offerTs >= timestamp &&
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

func (manager *MoveChunkManager) UpdateOfferTs(replset string) {
	if syncInfo, ok := manager.syncInfoMap[replset]; ok {
		syncInfo.mutex.Lock()
		syncInfo.offerTs = syncInfo.syncTs
		syncInfo.mutex.Unlock()
	}
}

func (manager *MoveChunkManager) eliminateBarrier() {
	manager.moveChunkLock.Lock()
	LOG.Info("move chunk map len=%v", len(manager.moveChunkMap))
	if len(manager.moveChunkMap) > 0 {
		for key := range manager.moveChunkMap {
			LOG.Info("move chunk key %v", key)
			break
		}
	}
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		for _, worker := range syncInfo.syncer.batcher.workerGroup {
			ack := bson.MongoTimestamp(atomic.LoadInt64(&worker.ack))
			unack := bson.MongoTimestamp(atomic.LoadInt64(&worker.unack))
			LOG.Info("syncer %v worker ack[%v] unack[%v] syncTs[%v] offerTs[%v] barrierKey[%v]", replset,
				utils.TimestampToLog(ack), utils.TimestampToLog(unack), utils.TimestampToLog(syncInfo.syncTs),
				utils.TimestampToLog(syncInfo.offerTs), syncInfo.barrierKey)
		}
		syncInfo.mutex.Unlock()
	}
	var deleteKeyList []MoveChunkKey
	for key, value := range manager.moveChunkMap {
		if value.deleteItem != nil {
			deleteReplset := value.deleteItem.Replset
			if !manager.barrierProbe(key, value.deleteItem.Timestamp) {
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
				if barrier, ok := value.barrierMap[minInsertReplset]; ok {
					// remove insert move chunk
					LOG.Info("syncer %v eliminate insert barrier[%v %v]", minInsertReplset,
						key.string(), utils.TimestampToLog(value.insertMap[minInsertReplset]))
					manager.syncInfoMap[minInsertReplset].deleteBarrier()
					delete(value.barrierMap, minInsertReplset)
					close(barrier)
				}
				LOG.Info("syncer %v remove insert move chunk oplog[%v %v]", minInsertReplset,
					key.string(), utils.TimestampToLog(value.insertMap[minInsertReplset]))
				delete(value.insertMap, minInsertReplset)
				// remove delete move chunk
				if barrier, ok := value.barrierMap[deleteReplset]; ok {
					LOG.Info("syncer %v eliminate delete barrier[%v %v]", deleteReplset,
						key.string(), utils.TimestampToLog(value.deleteItem.Timestamp))
					manager.syncInfoMap[deleteReplset].deleteBarrier()
					delete(value.barrierMap, deleteReplset)
					close(barrier)
				}
				LOG.Info("syncer %v remove delete move chunk oplog[%v %v]", deleteReplset,
					key.string(), utils.TimestampToLog(value.deleteItem.Timestamp))
				value.deleteItem = nil
				if len(value.insertMap) == 0 && len(value.barrierMap) == 0 {
					LOG.Info("move chunk map remove move chunk key[%v]", key.string())
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
func (manager *MoveChunkManager) BarrierOplog(replset string, partialLog *oplog.PartialLog) bool {
	syncInfo := manager.syncInfoMap[replset]
	syncInfo.blockOplog(replset, partialLog)

	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()

	if syncInfo.filterOplog(partialLog) {
		return false
	}

	barrier := false
	if moveChunkFilter.Filter(partialLog) {
		// barrier == true if the syncer already has a insert/delete move chunk oplog before
		if oplogId := oplog.GetKey(partialLog.Object, ""); oplogId != nil {
			key := MoveChunkKey{Id: oplogId, Namespace: partialLog.Namespace}
			value, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := value.insertMap[replset]; ok {
					LOG.Info("syncer %v meet insert barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToLog(ts), partialLog.Operation,
						key.string(), utils.TimestampToLog(partialLog.Timestamp))
					value.barrierOplog(syncInfo, key, partialLog)
					barrier = true
				} else if value.deleteItem != nil && value.deleteItem.Replset == replset {
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToLog(value.deleteItem.Timestamp), partialLog.Operation,
						key.string(), utils.TimestampToLog(partialLog.Timestamp))
					value.barrierOplog(syncInfo, key, partialLog)
					barrier = true
				} else {
					// find a insert/delete move chunk firstly
					value.addMoveChunk(replset, key, partialLog)
				}
			} else {
				LOG.Info("syncer %v create move chunk value when move chunk oplog found[%v %v]",
					replset, key.string(), utils.TimestampToLog(partialLog.Timestamp))
				value := &MoveChunkValue{
					insertMap:  make(map[string]bson.MongoTimestamp),
					barrierMap: make(map[string]chan interface{}),
				}
				value.addMoveChunk(replset, key, partialLog)
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
			key := MoveChunkKey{Id: oplogId, Namespace: partialLog.Namespace}
			value, ok := manager.moveChunkMap[key]
			if ok {
				if ts, ok := value.insertMap[replset]; ok {
					// barrier == true if the syncer already has a insert move chunk oplog before
					LOG.Info("syncer %v meet insert barrier ts[%v] when operation oplog found[%v %v]",
						replset, utils.TimestampToLog(ts), key.string(), utils.TimestampToLog(partialLog.Timestamp))
					value.barrierOplog(manager.syncInfoMap[replset], key, partialLog)
					barrier = true
				} else {
					if value.deleteItem != nil && value.deleteItem.Replset == replset {
						LOG.Crashf("syncer %v meet delete barrier ts[%v] when operation oplog found[%v %v] illegal",
							replset, utils.TimestampToLog(value.deleteItem.Timestamp),
							key.string(), utils.TimestampToLog(partialLog.Timestamp))
					}
				}
			}
		}
	}
	if !barrier {
		syncInfo.updateSyncTs(partialLog)
	}
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
		offerTs, ok3 := ckptDoc[MoveChunkOfferTSName].(bson.MongoTimestamp)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v", ckptDoc)
		} else if syncInfo, ok := manager.syncInfoMap[replset]; !ok {
			return fmt.Errorf("MoveChunkManager load checkpoint unknown replset %v", ckptDoc)
		} else {
			syncInfo.syncTs = syncTs
			syncInfo.offerTs = offerTs
			LOG.Info("MoveChunkManager load checkpoint set replset[%v] syncTs[%v] offerTs[%v]", replset,
				utils.ExtractTimestampForLog(syncInfo.syncTs), utils.ExtractTimestampForLog(syncInfo.offerTs))
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	iter = conn.Session.DB(db).C(tablePrefix + "_mvck_map").Find(bson.M{}).Iter()
	for iter.Next(ckptDoc) {
		key := MoveChunkKey{}
		value := MoveChunkValue{insertMap: make(map[string]bson.MongoTimestamp)}
		err1 := json.Unmarshal([]byte(ckptDoc[MoveChunkKeyName].(string)), &key)
		err2 := json.Unmarshal([]byte(ckptDoc[MoveChunkInsertMap].(string)), &value.insertMap)
		var err3 error
		if deleteItemStr, ok := ckptDoc[MoveChunkDeleteItem].(string); ok && deleteItemStr != "" {
			var item MCIItem
			err3 = json.Unmarshal([]byte(deleteItemStr), &item)
			value.deleteItem = &item
		} else {
			value.deleteItem, err3 = nil, nil
		}
		if err1 != nil || err2 != nil || err3 != nil {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v. err1[%v] err2[%v] err3[%v]",
				ckptDoc, err1, err2, err3)
		} else {
			manager.moveChunkMap[key] = &value
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	LOG.Info("MoveChunkManager load checkpoint moveChunkMap size[%v]", len(manager.moveChunkMap))
	return nil
}

func (manager *MoveChunkManager) Flush(conn *utils.MongoConn, db string, tablePrefix string) error {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	// check whether can flush
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		if syncInfo.barrierChan != nil {
			syncInfo.mutex.Unlock()
			return fmt.Errorf("MoveChunkManager sycner %v at move chunk barrier %v", replset, syncInfo.barrierKey)
		}
		syncInfo.mutex.Unlock()
	}

	checkpoint_begin := time.Now()
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		ckptDoc := map[string]interface{}{
			CheckpointName:       replset,
			MoveChunkSyncTSName:  syncInfo.syncTs,
			MoveChunkOfferTSName: syncInfo.offerTs,
		}
		if _, err := conn.Session.DB(db).C(tablePrefix+"_mvck_syncer").
			Upsert(bson.M{CheckpointName: replset}, ckptDoc); err != nil {
			syncInfo.mutex.Unlock()
			return fmt.Errorf("MoveChunkManager flush checkpoint syncer %v upsert failed. %v", ckptDoc, err)
		}
		syncInfo.mutex.Unlock()
	}
	table := tablePrefix + "_mvck_map"
	if err := conn.Session.DB(db).C(table).DropCollection(); err != nil && err.Error() != "ns not found" {
		LOG.Critical("MoveChunkManager flush checkpoint drop collection %v failed. %v", table, err)
		return err
	}
	buffer := make([]interface{}, 0, MoveChunkBufferSize)
	for key, value := range manager.moveChunkMap {
		keyStr, err1 := json.Marshal(key)
		insertMapStr, err2 := json.Marshal(value.insertMap)
		var deleteItemStr []byte
		var err3 error
		if value.deleteItem != nil {
			deleteItemStr, err3 = json.Marshal(*value.deleteItem)
		} else {
			deleteItemStr, err3 = []byte(""), nil
		}
		if err1 != nil || err2 != nil || err3 != nil {
			return fmt.Errorf("MoveChunkManager flush checkpoint json key[%v] insertMap[%v] deleteItem[%v] failed",
				key, value.insertMap, value.deleteItem)
		}
		ckptDoc := map[string]interface{}{
			MoveChunkKeyName:    string(keyStr),
			MoveChunkInsertMap:  string(insertMapStr),
			MoveChunkDeleteItem: string(deleteItemStr),
		}
		if len(buffer) >= MoveChunkBufferSize {
			// 1000 * byte size of ckptDoc < 16MB
			if err := conn.Session.DB(db).C(table).Insert(buffer...); err != nil {
				LOG.Critical("MoveChunkManager flush checkpoint map buffer %v insert faild. %v", buffer, err)
				return err
			}
			buffer = make([]interface{}, 0, MoveChunkBufferSize)
		}
		buffer = append(buffer, ckptDoc)
	}
	if len(buffer) > 0 {
		if err := conn.Session.DB(db).C(table).Insert(buffer...); err != nil {
			LOG.Critical("MoveChunkManager flush checkpoint map buffer %v insert faild. %v", buffer, err)
			return err
		}
	}
	LOG.Info("MoveChunkManager flush checkpoint moveChunkMap size[%v] cost %vs",
		len(manager.moveChunkMap), time.Now().Sub(checkpoint_begin).Seconds())
	return nil
}

type SyncerMoveChunk struct {
	syncer      *OplogSyncer
	syncTs      bson.MongoTimestamp
	offerTs     bson.MongoTimestamp
	barrierKey  MoveChunkKey
	barrierChan chan interface{}
	mutex       sync.Mutex
}

func (syncInfo *SyncerMoveChunk) deleteBarrier() {
	syncInfo.mutex.Lock()
	syncInfo.barrierKey = MoveChunkKey{}
	syncInfo.barrierChan = nil
	syncInfo.mutex.Unlock()
}

func (syncInfo *SyncerMoveChunk) filterOplog(partialLog *oplog.PartialLog) bool {
	syncInfo.mutex.Lock()
	defer syncInfo.mutex.Unlock()
	if syncInfo.syncTs >= partialLog.Timestamp {
		return true
	}
	return false
}

func (syncInfo *SyncerMoveChunk) blockOplog(replset string, partialLog *oplog.PartialLog) {
	var barrierChan chan interface{}
	syncInfo.mutex.Lock()
	barrierChan = syncInfo.barrierChan
	syncInfo.mutex.Unlock()
	// wait for barrier channel must be out of mutex, because eliminateBarrier need to delete barrier
	if barrierChan != nil {
		LOG.Info("syncer %v wait barrier", replset)
		<-barrierChan
		LOG.Info("syncer %v wait barrier finish", replset)
	}
}

func (syncInfo *SyncerMoveChunk) updateSyncTs(partialLog *oplog.PartialLog) {
	syncInfo.mutex.Lock()
	defer syncInfo.mutex.Unlock()
	syncInfo.syncTs = partialLog.Timestamp
}

type MoveChunkKey struct {
	Id        interface{} `json:"docId"`
	Namespace string      `json:"namespace"`
}

func (key MoveChunkKey) string() string {
	if id, ok := key.Id.(bson.ObjectId); ok {
		return fmt.Sprintf("{%x %v}", string(id), key.Namespace)
	} else {
		return fmt.Sprintf("{%v %v}", key.Id, key.Namespace)
	}
}

type MoveChunkValue struct {
	insertMap map[string]bson.MongoTimestamp
	// the size of deleteMap will not more than 1
	deleteItem *MCIItem
	barrierMap map[string]chan interface{}
}

type MCIItem struct {
	Replset   string              `json:"replset"`
	Timestamp bson.MongoTimestamp `json:"deleteTs"`
}

func (value *MoveChunkValue) barrierOplog(syncInfo *SyncerMoveChunk, key MoveChunkKey, partialLog *oplog.PartialLog) {
	replset := syncInfo.syncer.replset
	if _, ok := value.barrierMap[replset]; ok {
		LOG.Crashf("syncer %v has more than one barrier in barrierMap when move chunk oplog found[%v %v]",
			replset, key.string(), utils.TimestampToLog(partialLog.Timestamp))
	}
	barrierChan := make(chan interface{})
	value.barrierMap[replset] = barrierChan
	syncInfo.mutex.Lock()
	if syncInfo.barrierChan != nil {
		LOG.Crashf("syncer %v has more than one barrier in syncInfoMap when move chunk oplog found[%v %v]",
			syncInfo.syncer.replset, key.string(), utils.TimestampToLog(partialLog.Timestamp))
	}
	syncInfo.barrierKey = key
	syncInfo.barrierChan = barrierChan
	syncInfo.mutex.Unlock()
}

func (value *MoveChunkValue) addMoveChunk(replset string, key MoveChunkKey, partialLog *oplog.PartialLog) {
	if partialLog.Operation == "d" {
		if value.deleteItem != nil {
			LOG.Crashf("move chunk key[%v] has more than one deleteItem[%v %v] when delete move chunk oplog found[%v %v]",
				key.string(), value.deleteItem.Replset, utils.TimestampToLog(value.deleteItem.Timestamp),
				replset, utils.TimestampToLog(partialLog.Timestamp))
		}
		LOG.Info("syncer %v add delete move chunk oplog[%v %v]",
			replset, key.string(), utils.TimestampToLog(partialLog.Timestamp))
		value.deleteItem = &MCIItem{Replset: replset, Timestamp: partialLog.Timestamp}
	} else if partialLog.Operation == "i" {
		LOG.Info("syncer %v add insert move chunk oplog[%v %v]",
			replset, key.string(), utils.TimestampToLog(partialLog.Timestamp))
		value.insertMap[replset] = partialLog.Timestamp
	} else {
		LOG.Crashf("unsupported %v move chunk oplog[%v %v]",
			partialLog.Operation, key.string(), utils.TimestampToLog(partialLog.Timestamp))
	}
}
