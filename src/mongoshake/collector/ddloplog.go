package collector

import (
	"encoding/json"
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"math"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"strings"
	"sync"
	"time"
)

const (
	CheckpointKeyNs     = "ns"
	CheckpointKeyObject = "obj"
	CheckpointBlocklog  = "blockLog"
	CheckpointDBMap     = "dbMap"

	DDLCheckInterval       = 1  // s
	DDLUnResponseThreshold = 30 // s
)

type DDLKey struct {
	Namespace string
	ObjectStr string
}

type DDLValue struct {
	blockLog  *oplog.PartialLog
	blockChan chan bool
	dbMap     map[string]bson.MongoTimestamp
}

type DDLManager struct {
	ckptManager *CheckpointManager
	ddlMap      map[DDLKey]*DDLValue
	syncMap     map[string]*OplogSyncer

	FromCsConn   *utils.MongoConn // share config server url
	ToIsSharding bool

	lastDDLValue *DDLValue // avoid multiple eliminate the same ddl
	ddlLock      sync.Mutex
}

func NewDDLManager(ckptManager *CheckpointManager) *DDLManager {
	if !DDLSupportForSharding() {
		return nil
	}
	var fromCsConn *utils.MongoConn
	var err error
	if fromCsConn, err = utils.NewMongoConn(conf.Options.MongoCsUrl, utils.ConnectModePrimary, true); err != nil {
		LOG.Crashf("Connect MongoCsUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
	}
	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(conf.Options.TunnelAddress[0], utils.ConnectModePrimary, true); err != nil {
		LOG.Crashf("Connect toUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
	}
	defer toConn.Close()

	manager := &DDLManager{
		ckptManager:  ckptManager,
		ddlMap:       make(map[DDLKey]*DDLValue),
		syncMap:      make(map[string]*OplogSyncer),
		FromCsConn:   fromCsConn,
		ToIsSharding: utils.IsSharding(toConn.Session),
	}
	ckptManager.registerPersis(manager)
	return manager
}

func (manager *DDLManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncMap[syncer.replset] = syncer
}

func (manager *DDLManager) start() {
	nimo.GoRoutineInLoop(func() {
		if ddlMinValue := manager.eliminateBlock(); ddlMinValue != nil {
			ddlMinValue.blockChan <- true
		}
		time.Sleep(DDLCheckInterval * time.Second)
	})
}

func (manager *DDLManager) addDDL(replset string, log *oplog.PartialLog) *DDLValue {
	manager.ddlLock.Lock()
	defer manager.ddlLock.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if _, ok := manager.ddlMap[ddlKey]; !ok {
			manager.ddlMap[ddlKey] = &DDLValue{
				blockChan: make(chan bool),
				dbMap:     make(map[string]bson.MongoTimestamp),
				blockLog:  log}
		}
		ddlValue := manager.ddlMap[ddlKey]
		ddlValue.dbMap[replset] = log.Timestamp
		return ddlValue
	} else {
		LOG.Crashf("DDLManager syncer %v json marshal ddl log %v error. %v", replset, log.Object, err)
		return nil
	}
}

func (manager *DDLManager) BlockDDL(replset string, log *oplog.PartialLog) bool {
	ddlValue := manager.addDDL(replset, log)
	LOG.Info("Oplog syncer %v block at ddl log %v", replset, log)
	// ddl is the only operation in this batch, so no need to update syncTs after dispatch.
	// why need update? maybe do checkpoint when syncer block, but synTs has not been updated yet
	manager.syncMap[replset].batcher.syncTs = manager.syncMap[replset].batcher.unsyncTs
	manager.ckptManager.mutex.RUnlock()
	_, ok := <-ddlValue.blockChan
	manager.ckptManager.mutex.RLock()
	return ok
}

func (manager *DDLManager) UnBlockDDL(replset string, log *oplog.PartialLog) {
	manager.ddlLock.Lock()
	defer manager.ddlLock.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if ddlValue, ok := manager.ddlMap[ddlKey]; ok {
			close(ddlValue.blockChan)
			delete(manager.ddlMap, ddlKey)
		} else {
			LOG.Crashf("DDLManager syncer %v ddlKey[%v] not in ddlMap error", replset, ddlKey)
		}
	} else {
		LOG.Crashf("DDLManager syncer %v UnBlockDDL json marshal %v error. %v", replset, log.Object, err)
	}
}

func (manager *DDLManager) Load(tablePrefix string) error {
	manager.ddlLock.Lock()
	defer manager.ddlLock.Unlock()
	conn := manager.ckptManager.conn
	db := manager.ckptManager.db

	iter := conn.Session.DB(db).C(tablePrefix + "_ddl").Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		namespace, ok1 := ckptDoc[CheckpointKeyNs].(string)
		object, ok2 := ckptDoc[CheckpointKeyObject].(string)
		blockLogDoc, ok3 := ckptDoc[CheckpointBlocklog].(map[string]interface{})
		dbMapDoc, ok4 := ckptDoc[CheckpointDBMap].(map[string]interface{})
		blockLog := &oplog.PartialLog{}
		err := utils.Map2Struct(blockLogDoc, "bson", blockLog)
		if !ok1 || !ok2 || !ok3 || !ok4 || err != nil {
			return fmt.Errorf("DDLManager load checkpoint illegal record %v. " +
				"ok1[%v] ok2[%v] ok3[%v] ok4[%v] err[%v]",
				ckptDoc, ok1, ok2, ok3, ok4, err)
		}
		dbMap := make(map[string]bson.MongoTimestamp)
		for replset, ts := range dbMapDoc {
			if ts, ok := ts.(bson.MongoTimestamp); ok {
				dbMap[replset] = ts
			} else {
				return fmt.Errorf("DDLManager load checkpoint illegal dbMap %v", dbMap)
			}
		}
		ddlKey := DDLKey{Namespace: namespace, ObjectStr: object}
		manager.ddlMap[ddlKey] = &DDLValue{
			blockLog:  blockLog,
			blockChan: make(chan bool),
			dbMap:     dbMap}
		LOG.Info("DDLManager load ddlMap key %v", ddlKey)
	}
	LOG.Info("DDLManager load checkpoint ddlMap size[%v]", len(manager.ddlMap))
	return nil
}

func (manager *DDLManager) Flush(tablePrefix string) error {
	manager.ddlLock.Lock()
	defer manager.ddlLock.Unlock()
	conn := manager.ckptManager.conn
	db := manager.ckptManager.db

	table := tablePrefix + "_ddl"
	if err := conn.Session.DB(db).C(table).DropCollection(); err != nil && err.Error() != "ns not found" {
		return LOG.Critical("DDLManager flush checkpoint drop collection %v failed. %v", table, err)
	}
	var buffer []interface{}
	for ddlKey, ddlValue := range manager.ddlMap {
		blockLog, err := utils.Struct2Map(ddlValue.blockLog, "bson")
		if err != nil {
			return fmt.Errorf("DDLManager flush checkpoint json blockLog[%v] failed", ddlValue.blockLog)
		}
		ckptDoc := map[string]interface{}{
			CheckpointKeyNs:     ddlKey.Namespace,
			CheckpointKeyObject: ddlKey.ObjectStr,
			CheckpointBlocklog:  blockLog,
			CheckpointDBMap:     ddlValue.dbMap,
		}
		buffer = append(buffer, ckptDoc)
	}
	if len(buffer) > 0 {
		if err := conn.Session.DB(db).C(table).Insert(buffer...); err != nil {
			return LOG.Critical("DDLManager flush checkpoint ddlMap buffer %v insert failed. %v", buffer, err)
		}
	}
	LOG.Info("DDLManager flush checkpoint ddlMap size[%v]", len(manager.ddlMap))
	return nil
}

func (manager *DDLManager) GetTableList(tablePrefix string) []string {
	return []string{tablePrefix + "_ddl"}
}

func (manager *DDLManager) eliminateBlock() *DDLValue {
	manager.ddlLock.Lock()
	defer manager.ddlLock.Unlock()
	if len(manager.ddlMap) > 0 {
		LOG.Info("ddl block map len=%v", len(manager.ddlMap))
		for key := range manager.ddlMap {
			LOG.Info("ddl block key %v", key)
		}
	}
	// get the earliest ddl operator
	var ddlMinTs bson.MongoTimestamp = math.MaxInt64
	var ddlMinKey DDLKey
	for ddlKey, ddlValue := range manager.ddlMap {
		for _, blockTs := range ddlValue.dbMap {
			if ddlMinTs > blockTs {
				ddlMinTs = blockTs
				ddlMinKey = ddlKey
			}
		}
	}
	if ddlMinTs == math.MaxInt64 {
		return nil
	}
	ddlMinValue := manager.ddlMap[ddlMinKey]
	if ddlMinValue == manager.lastDDLValue {
		LOG.Info("DDLManager already eliminate ddl %v", ddlMinKey)
		return nil
	}
	// whether non sharding ddl
	var shardColSpec *utils.ShardCollectionSpec
	if manager.FromCsConn != nil {
		time.Sleep(DDLCheckInterval * time.Second)
		shardColSpec = utils.GetShardCollectionSpec(manager.FromCsConn.Session, ddlMinValue.blockLog)
		if shardColSpec == nil {
			LOG.Info("DDLManager eliminate block and run non sharding ddl %v", ddlMinKey)
			manager.lastDDLValue = ddlMinValue
			return ddlMinValue
		}
	}
	// try to run the earliest ddl
	if strings.HasSuffix(ddlMinKey.Namespace, "system.indexes") {
		LOG.Info("DDLManager eliminate block and run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		return ddlMinValue
	}
	var object bson.D
	if err := json.Unmarshal([]byte(ddlMinKey.ObjectStr), &object); err != nil {
		LOG.Crashf("DDLManager eliminate unmarshal bson %v from ns[%v] failed. %v",
			ddlMinKey.ObjectStr, ddlMinKey.Namespace, err)
	}
	operation, _ := oplog.ExtraCommandName(object)
	switch operation {
	case "create":
		fallthrough
	case "createIndexes":
		fallthrough
	case "collMod":
		LOG.Info("DDLManager eliminate block and run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		return ddlMinValue
	case "deleteIndex":
		fallthrough
	case "deleteIndexes":
		fallthrough
	case "dropIndex":
		fallthrough
	case "dropIndexes":
		fallthrough
	case "dropDatabase":
		fallthrough
	case "drop":
		// drop ddl must block until get drop oplog from all dbs or unblocked db run more than ddlMinTs
		current := time.Now()
		for replset, syncer := range manager.syncMap {
			if _, ok := ddlMinValue.dbMap[replset]; ok {
				continue
			}
			if syncer.batcher.syncTs >= ddlMinTs {
				continue
			}
			// if the syncer is un responsible for 1 minute, then the syncer has finished to sync
			if current.After(syncer.batcher.lastResponseTime.Add(DDLUnResponseThreshold * time.Second)) {
				continue
			}
			LOG.Info("DDLManager eliminate cannot sync ddl %v with col spec %v. "+
				"replset %v ddlMinTs[%v] syncTs[%v] UnResTimes[%v]",
				ddlMinKey, shardColSpec, replset, utils.TimestampToLog(ddlMinTs),
				utils.TimestampToLog(syncer.batcher.syncTs), utils.TimestampToLog(syncer.batcher.lastResponseTime))
			return nil
		}
		LOG.Info("DDLManager eliminate block and force run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		return ddlMinValue
	case "renameCollection":
		fallthrough
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		fallthrough
	case "applyOps":
		LOG.Crashf("DDLManager illegal DDL %v", ddlMinKey)
	default:
		LOG.Info("DDLManager eliminate block and run unsupported ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		return ddlMinValue
	}
	return nil
}

func TransformShardingDDL(replset string, log *oplog.PartialLog, shardColSpec *utils.ShardCollectionSpec, toIsSharding bool) []*oplog.PartialLog {
	logD := log.Dump(nil)
	if strings.HasSuffix(log.Namespace, "system.indexes") {
		// insert into system.indexes only create index at one shard, so need to transform
		collection := strings.SplitN(shardColSpec.Ns, ".", 2)[1]
		object := bson.D{{"createIndexes", collection}}
		object = append(object, log.Object...)
		tlog := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: "c", Gid: log.Gid,
			Namespace: shardColSpec.Ns, Object: object}
		LOG.Info("TransformShardingDDL syncer %v transform DDL log %v to tlog %v",
			replset, logD, tlog)
		return []*oplog.PartialLog{tlog}
	}

	operation, _ := oplog.ExtraCommandName(log.Object)
	switch operation {
	case "create":
		if toIsSharding {
			db := strings.SplitN(log.Namespace, ".", 2)[0]
			t1log := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: log.Operation, Gid: log.Gid,
				Namespace: log.Namespace, Object: bson.D{{"enableSharding", db}}}
			t2log := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: log.Operation, Gid: log.Gid,
				Namespace: log.Namespace, Object: bson.D{{"shardCollection", shardColSpec.Ns},
					{"key", shardColSpec.Key}, {"unique", shardColSpec.Unique}}}
			LOG.Info("TransformShardingDDL syncer %v transform DDL log %v to t1log %v t2log %v",
				replset, logD, t1log, t2log)
			return []*oplog.PartialLog{t1log, t2log}
		}
		fallthrough
	case "createIndexes":
		fallthrough
	case "dropDatabase":
		fallthrough
	case "collMod":
		fallthrough
	case "drop":
		fallthrough
	case "deleteIndex":
		fallthrough
	case "deleteIndexes":
		fallthrough
	case "dropIndex":
		fallthrough
	case "dropIndexes":
		return []*oplog.PartialLog{log}
	case "renameCollection":
		fallthrough
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		fallthrough
	case "applyOps":
		LOG.Crashf("TransformShardingDDL syncer %v illegal DDL log %v", replset, logD)
	default:
		LOG.Crashf("TransformShardingDDL syncer %v meet unsupported DDL log %s", replset, logD)
	}
	return nil
}

func TransformDbDDL(replset string, log *oplog.PartialLog) []*oplog.PartialLog {
	if strings.HasSuffix(log.Namespace, "system.indexes") {
		logD := log.Dump(nil)
		// insert into system.indexes can not used in MongoDB 4.2, so need to transform
		// {"op" : "i", "ns" : "my.system.indexes", "o" : { "v" : 2, "key" : { "date" : 1 }, "name" : "date_1", "ns" : "my.tbl", "expireAfterSeconds" : 3600 }
		namespace, ok := oplog.GetKey(log.Object, "ns").(string)
		if !ok {
			LOG.Crashf("TransformDbDDL syncer %v illegal DDL log %v", replset, logD)
		}
		collection := strings.SplitN(namespace, ".", 2)[1]
		object := bson.D{{"createIndexes", collection}}
		object = append(object, log.Object...)
		tlog := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: "c", Gid: log.Gid,
			Namespace: namespace, Object: object}
		LOG.Info("TransformDbDDL syncer %v transform DDL log %v to tlog %v",
			replset, logD, tlog)
		return []*oplog.PartialLog{tlog}
	} else {
		return []*oplog.PartialLog{log}
	}
}