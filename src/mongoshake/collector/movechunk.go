package collector

import (
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	conf "mongoshake/collector/configure"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MoveChunkBarrierKey          = "barrierKey"
	MoveChunkKeyName             = "key"
	MoveChunkInsertMap           = "insertMap"
	MoveChunkDeleteItem          = "deleteItem"
	MoveChunkBufferSize          = 1000
	MoveChunkUnResponseThreshold = 30 // s
)

func NewMoveChunkManager(ckptManager *CheckpointManager) *MoveChunkManager {
	if !conf.Options.MoveChunkEnable {
		return nil
	}
	manager := &MoveChunkManager{
		ckptManager:  ckptManager,
		moveChunkMap: make(map[MoveChunkKey]*MoveChunkValue),
		syncInfoMap:  make(map[string]*SyncerMoveChunk),
	}
	ckptManager.registerPersis(manager)
	return manager
}

type MoveChunkManager struct {
	ckptManager *CheckpointManager
	syncInfoMap map[string]*SyncerMoveChunk
	// ensure the order of oplog when move chunk occur
	moveChunkMap  map[MoveChunkKey]*MoveChunkValue
	moveChunkLock sync.Mutex
}

func (manager *MoveChunkManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncInfoMap[syncer.replset] = &SyncerMoveChunk{syncer: syncer}
}

func (manager *MoveChunkManager) start() {
	nimo.GoRoutineInLoop(func() {
		removeOK := manager.eliminateBarrier()
		if removeOK {
			time.Sleep(50 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(conf.Options.MoveChunkInterval) * time.Millisecond)
		}
	})
	// periodically log internal information to monitor
	nimo.GoRoutine(func() {
		manager.moveChunkLock.Lock()
		if len(manager.moveChunkMap) > 0 {
			LOG.Info("move chunk map len=%v", len(manager.moveChunkMap))
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
				batcher := syncInfo.syncer.batcher
				LOG.Info("syncer %v worker ack[%v] unack[%v] syncTs[%v] lastResponseTime[%v] barrierKey[%v]", replset,
					utils.TimestampToLog(ack), utils.TimestampToLog(unack), utils.TimestampToLog(batcher.syncTs),
					utils.TimestampToLog(batcher.lastResponseTime.Unix()), syncInfo.barrierKey)
			}
			syncInfo.mutex.Unlock()
		}
		manager.moveChunkLock.Unlock()
		time.Sleep(60*time.Second)
	})
}

func (manager *MoveChunkManager) barrierProbe(key MoveChunkKey, value *MoveChunkValue) bool {
	insertMap := value.insertMap
	deleteItem := value.deleteItem
	current := time.Now()
	for _, syncInfo := range manager.syncInfoMap {
		// worker ack must exceed timestamp of insert/delete move chunk oplog
		syncInfo.mutex.Lock()
		replset := syncInfo.syncer.replset
		_, hasInsert := insertMap[replset]
		for _, worker := range syncInfo.syncer.batcher.workerGroup {
			unack := atomic.LoadInt64(&worker.unack)
			ack := atomic.LoadInt64(&worker.ack)
			if syncInfo.barrierChan != nil && ack == unack &&
				(syncInfo.syncer.batcher.syncTs >= deleteItem.Timestamp || syncInfo.barrierKey == key) {
				continue
			}
			if syncInfo.barrierChan == nil && (hasInsert && replset != deleteItem.Replset ||
				syncInfo.syncer.batcher.syncTs >= deleteItem.Timestamp &&
					(ack == unack || ack < unack && ack >= int64(deleteItem.Timestamp))) {
				continue
			}
			if current.After(syncInfo.syncer.batcher.lastResponseTime.Add(MoveChunkUnResponseThreshold * time.Second)) {
				continue
			}
			syncInfo.mutex.Unlock()
			return false
		}
		syncInfo.mutex.Unlock()
	}
	return true
}

func (manager *MoveChunkManager) minTsProbe(minTs bson.MongoTimestamp) bool {
	current := time.Now()
	for _, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		if syncInfo.barrierChan == nil && syncInfo.syncer.batcher.syncTs < minTs &&
			!current.After(syncInfo.syncer.batcher.lastResponseTime.Add(MoveChunkUnResponseThreshold*time.Second)) {
			syncInfo.mutex.Unlock()
			return false
		}
		syncInfo.mutex.Unlock()
	}
	return true
}

func (manager *MoveChunkManager) eliminateBarrier() bool {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	removeOK := false
	var deleteKeyList []MoveChunkKey
	for key, value := range manager.moveChunkMap {
		if value.deleteItem != nil {
			deleteReplset := value.deleteItem.Replset
			if !manager.barrierProbe(key, value) {
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
			if minInsertReplset != "" && manager.minTsProbe(minInsertTs) {
				removeOK = true
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
	return removeOK
}

func (manager *MoveChunkManager) BarrierOplog(replset string, partialLog *oplog.PartialLog) (bool, bool, interface{}) {
	syncInfo := manager.syncInfoMap[replset]
	barrierChan := syncInfo.getBarrier()
	// wait for barrier channel must be out of mutex, because eliminateBarrier need to delete barrier
	if barrierChan != nil {
		LOG.Info("syncer %v wait barrier", replset)
		// unlock all mutex before blocking by move chunk barrier
		manager.ckptManager.mutex.RUnlock()
		<-barrierChan
		manager.ckptManager.mutex.RLock()
		LOG.Info("syncer %v wait barrier finish", replset)
	}
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()

	barrier := false
	resend := false
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
					resend = true
				} else if value.deleteItem != nil && value.deleteItem.Replset == replset {
					LOG.Info("syncer %v meet delete barrier ts[%v] when %v move chunk oplog found[%v %v]",
						replset, utils.TimestampToLog(value.deleteItem.Timestamp), partialLog.Operation,
						key.string(), utils.TimestampToLog(partialLog.Timestamp))
					//value.barrierOplog(syncInfo, key, partialLog)
					barrier = true
					// if move chunk fail and retry, the dest oplog list will be I -> D -> I,
					// so D & I will be eliminate at the same syncer
					value.addMoveChunk(replset, key, partialLog)
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
					resend = true
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
	// updateObj used for test case, it means update operation can execute
	var updateObj interface{}
	if partialLog.Operation == "u" {
		updateObj = partialLog.Object
	}
	return barrier, resend, updateObj
}

func (manager *MoveChunkManager) Load(tablePrefix string) error {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	conn := manager.ckptManager.conn
	db := manager.ckptManager.db

	iter := conn.Session.DB(db).C(tablePrefix + "_mvck_syncer").Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[utils.CheckpointName].(string)
		barrierKey, ok2 := ckptDoc[MoveChunkBarrierKey].(map[string]interface{})
		if !ok1 || !ok2 {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v", ckptDoc)
		} else if syncInfo, ok := manager.syncInfoMap[replset]; !ok {
			return fmt.Errorf("MoveChunkManager load checkpoint unknown replset %v", ckptDoc)
		} else {
			// load barrier key and channel
			if len(barrierKey) > 0 {
				if err := utils.Map2Struct(barrierKey, "bson", &syncInfo.barrierKey); err != nil {
					return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v. err[%v]",
						ckptDoc, err)
				}
				syncInfo.barrierChan = make(chan interface{})
			}
			LOG.Info("MoveChunkManager load checkpoint set replset[%v] syncInfo barrierKey[%v] barrierChan[%v]",
				replset, syncInfo.barrierKey, syncInfo.barrierChan)
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	iter = conn.Session.DB(db).C(tablePrefix + "_mvck_map").Find(bson.M{}).Iter()
	for iter.Next(ckptDoc) {
		key := MoveChunkKey{}
		value := MoveChunkValue{insertMap: make(map[string]bson.MongoTimestamp),
			barrierMap: make(map[string]chan interface{})}

		err1 := utils.Map2Struct(ckptDoc[MoveChunkKeyName].(map[string]interface{}), "bson", &key)
		for k, v := range ckptDoc[MoveChunkInsertMap].(map[string]interface{}) {
			value.insertMap[k] = v.(bson.MongoTimestamp)
		}
		var err2 error
		deleteItem := ckptDoc[MoveChunkDeleteItem].(map[string]interface{})
		if len(deleteItem) > 0 {
			value.deleteItem = &MCIItem{}
			err2 = utils.Map2Struct(deleteItem, "bson", value.deleteItem)
		}
		if err1 != nil || err2 != nil {
			return fmt.Errorf("MoveChunkManager load checkpoint illegal record %v. err1[%v] err2[%v]",
				ckptDoc, err1, err2)
		} else {
			manager.moveChunkMap[key] = &value
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("MoveChunkManager close iterator failed. %v", err)
	}
	for replset, syncInfo := range manager.syncInfoMap {
		if syncInfo.barrierChan != nil {
			value := manager.moveChunkMap[syncInfo.barrierKey]
			value.barrierMap[replset] = syncInfo.barrierChan
		}
	}
	LOG.Info("MoveChunkManager load checkpoint moveChunkMap size[%v]", len(manager.moveChunkMap))
	return nil
}

func (manager *MoveChunkManager) Flush(tablePrefix string) error {
	manager.moveChunkLock.Lock()
	defer manager.moveChunkLock.Unlock()
	conn := manager.ckptManager.conn
	db := manager.ckptManager.db

	checkpointBegin := time.Now()
	for replset, syncInfo := range manager.syncInfoMap {
		syncInfo.mutex.Lock()
		var barrierKey map[string]interface{}
		var err error
		if syncInfo.barrierChan != nil {
			barrierKey, err = utils.Struct2Map(&syncInfo.barrierKey, "bson")
			if err != nil {
				return fmt.Errorf("MoveChunkManager flush checkpoint json barrierKey[%v] failed",
					syncInfo.barrierKey)
			}
		}
		ckptDoc := map[string]interface{}{
			utils.CheckpointName: replset,
			MoveChunkBarrierKey:  barrierKey,
		}
		if _, err = conn.Session.DB(db).C(tablePrefix+"_mvck_syncer").
			Upsert(bson.M{utils.CheckpointName: replset}, ckptDoc); err != nil {
			syncInfo.mutex.Unlock()
			return fmt.Errorf("MoveChunkManager flush checkpoint syncer %v upsert failed. %v", ckptDoc, err)
		}
		syncInfo.mutex.Unlock()
	}
	table := tablePrefix + "_mvck_map"
	if err := conn.Session.DB(db).C(table).DropCollection(); err != nil && err.Error() != "ns not found" {
		return LOG.Critical("MoveChunkManager flush checkpoint drop collection %v failed. %v", table, err)
	}
	buffer := make([]interface{}, 0, MoveChunkBufferSize)
	for key, value := range manager.moveChunkMap {
		keyItem, err1 := utils.Struct2Map(&key, "bson")
		var deleteItem map[string]interface{}
		var err2 error
		if value.deleteItem != nil {
			deleteItem, err2 = utils.Struct2Map(value.deleteItem, "bson")
		}
		if err1 != nil || err2 != nil {
			return fmt.Errorf("MoveChunkManager flush checkpoint json key[%v] insertMap[%v] deleteItem[%v] failed",
				key, value.insertMap, value.deleteItem)
		}
		ckptDoc := map[string]interface{}{
			MoveChunkKeyName:    keyItem,
			MoveChunkInsertMap:  value.insertMap,
			MoveChunkDeleteItem: deleteItem,
		}
		if len(buffer) >= MoveChunkBufferSize {
			// 1000 * byte size of ckptDoc < 16MB
			if err := conn.Session.DB(db).C(table).Insert(buffer...); err != nil {
				return LOG.Critical("MoveChunkManager flush checkpoint moveChunkMap buffer %v insert failed. %v", buffer, err)
			}
			buffer = make([]interface{}, 0, MoveChunkBufferSize)
		}
		buffer = append(buffer, ckptDoc)
	}
	if len(buffer) > 0 {
		if err := conn.Session.DB(db).C(table).Insert(buffer...); err != nil {
			return LOG.Critical("MoveChunkManager flush checkpoint moveChunkMap buffer %v insert failed. %v", buffer, err)
		}
	}
	LOG.Info("MoveChunkManager flush checkpoint moveChunkMap size[%v] cost %vs",
		len(manager.moveChunkMap), time.Now().Sub(checkpointBegin).Seconds())
	return nil
}

func (manager *MoveChunkManager) GetTableList(tablePrefix string) []string {
	return []string{tablePrefix + "_mvck_syncer", tablePrefix + "_mvck_map"}
}

type SyncerMoveChunk struct {
	syncer      *OplogSyncer
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

func (syncInfo *SyncerMoveChunk) getBarrier() chan interface{} {
	syncInfo.mutex.Lock()
	defer syncInfo.mutex.Unlock()
	return syncInfo.barrierChan
}

type MoveChunkKey struct {
	Id        interface{} `bson:"docId"`
	Namespace string      `bson:"namespace"`
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
	Replset   string              `bson:"replset"`
	Timestamp bson.MongoTimestamp `bson:"deleteTs"`
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
