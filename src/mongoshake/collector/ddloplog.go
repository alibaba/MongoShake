package collector

import (
	"encoding/json"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"math"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DDLCheckInterval = 1 // s
	DDLForceRunInterval = 10 // s
)

type DDLKey struct {
	Namespace string
	ObjectStr string
}

type DDLValue struct {
	blockLog *oplog.PartialLog
	blockChan chan bool
	blockMap  map[string]bson.MongoTimestamp
}

type DDLManager struct {
	ddlMap  map[DDLKey]*DDLValue
	syncMap map[string]*OplogSyncer

	FromCsConn   *utils.MongoConn // share config server url
	ToIsSharding bool

	lastDDLValue *DDLValue // avoid multiple eliminate the same ddl
	mutex        sync.Mutex
}

func NewDDLManager() *DDLManager {
	var fromCsConn *utils.MongoConn
	var err error
	if DDLSupportForSharding() {
		if fromCsConn, err = utils.NewMongoConn(conf.Options.MongoCsUrl, utils.ConnectModePrimary, true); err != nil {
			LOG.Crashf("Connect MongoCsUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
		}
	}

	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(conf.Options.TunnelAddress[0], utils.ConnectModePrimary, true); err != nil {
		LOG.Crashf("Connect toUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
	}
	defer toConn.Close()

	return &DDLManager{
		ddlMap:       make(map[DDLKey]*DDLValue),
		syncMap:      make(map[string]*OplogSyncer),
		FromCsConn:   fromCsConn,
		ToIsSharding: utils.IsSharding(toConn.Session),
	}
}

func (manager *DDLManager) start() {
	nimo.GoRoutineInLoop(func() {
		manager.eliminateBlock()
		time.Sleep(DDLCheckInterval * time.Second)
	})
}

func (manager *DDLManager) addDDL(replset string, log *oplog.PartialLog) *DDLValue {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if _, ok := manager.ddlMap[ddlKey]; !ok {
			manager.ddlMap[ddlKey] = &DDLValue{
				blockChan: make(chan bool),
				blockMap: make(map[string]bson.MongoTimestamp),
				blockLog: log}
		}
		ddlValue := manager.ddlMap[ddlKey]
		ddlValue.blockMap[replset] = log.Timestamp
		return ddlValue
	} else {
		LOG.Crashf("DDLManager syncer %v json marshal ddl log %v error. %v", replset, log.Object, err)
		return nil
	}
}

func (manager *DDLManager) BlockDDL(replset string, log *oplog.PartialLog) bool {
	ddlValue := manager.addDDL(replset, log)
	LOG.Info("Oplog syncer %v block at ddl log %v", replset, log)
	_, ok := <-ddlValue.blockChan
	return ok
}

func (manager *DDLManager) UnBlockDDL(replset string, log *oplog.PartialLog) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if value, ok := manager.ddlMap[ddlKey]; ok {
			close(value.blockChan)
			delete(manager.ddlMap, ddlKey)
		} else {
			LOG.Crashf("DDLManager syncer %v ddlKey[%v] not in ddlMap error", replset, ddlKey)
		}
	} else {
		LOG.Crashf("DDLManager syncer %v UnBlockDDL json marshal %v error. %v", replset, log.Object, err)
	}
}

func (manager *DDLManager) eliminateBlock() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	// get all block shards and the earliest ddl operator
	AllBlockMap := make(map[string]bson.MongoTimestamp)
	var ddlMinTs bson.MongoTimestamp = math.MaxInt64
	var ddlMinKey DDLKey
	for ddlKey, value := range manager.ddlMap {
		for replset, ts := range value.blockMap {
			AllBlockMap[replset] = ts
			if ddlMinTs > ts {
				ddlMinTs = ts
				ddlMinKey = ddlKey
			}
		}
	}
	if ddlMinTs == math.MaxInt64 {
		return
	}
	ddlMinValue := manager.ddlMap[ddlMinKey]
	if ddlMinValue == manager.lastDDLValue {
		LOG.Info("DDLManager already eliminate ddl %v", ddlMinKey)
		return
	}
	// whether non sharding ddl
	if manager.FromCsConn != nil {
		time.Sleep(DDLCheckInterval * time.Second)
		shardColSpec := utils.GetShardCollectionSpec(manager.FromCsConn.Session, ddlMinValue.blockLog)
		if shardColSpec == nil {
			LOG.Info("DDLManager eliminate block and run non sharding ddl %v", ddlMinKey)
			manager.lastDDLValue = ddlMinValue
			ddlMinValue.blockChan <- true
			return
		}
	}
	// try to run the earliest ddl
	if strings.HasSuffix(ddlMinKey.Namespace, "system.indexes") {
		LOG.Info("DDLManager eliminate block and run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		ddlMinValue.blockChan <- true
		return
	}
	var object bson.D
	if err := json.Unmarshal([]byte(ddlMinKey.ObjectStr), &object); err != nil {
		LOG.Crashf("DDLManager unmarshal bson %v from ns[%v] failed. %v",
			ddlMinKey.ObjectStr, ddlMinKey.Namespace, err)
	}
	operation, _ := oplog.ExtraCommandName(object)
	switch operation {
	case "create":
		fallthrough
	case "createIndexes":
		fallthrough
	case "deleteIndex":
		fallthrough
	case "deleteIndexes":
		fallthrough
	case "collMod":
		fallthrough
	case "dropIndex":
		fallthrough
	case "dropIndexes":
		LOG.Info("DDLManager eliminate block and run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		ddlMinValue.blockChan <- true
	case "dropDatabase":
		fallthrough
	case "drop":
		// drop dll must block until get all oplog or 10 seconds later
		for replset, syncer := range manager.syncMap {
			if _, ok := AllBlockMap[replset]; ok {
				continue
			}
			for _, worker := range syncer.batcher.workerGroup {
				unack := bson.MongoTimestamp(atomic.LoadInt64(&worker.unack))
				if unack < ddlMinTs + (DDLForceRunInterval << 32) {
					return
				}
			}
		}
		LOG.Info("DDLManager eliminate block and force run ddl %v", ddlMinKey)
		manager.lastDDLValue = ddlMinValue
		ddlMinValue.blockChan <- true
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
		ddlMinValue.blockChan <- true
	}
}

func (manager *DDLManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncMap[syncer.replset] = syncer
}

func TransformDDL(replset string, log *oplog.PartialLog, shardColSpec *utils.ShardCollectionSpec, toIsSharding bool) []*oplog.PartialLog {
	logD := log.Dump(nil)
	if strings.HasSuffix(log.Namespace, "system.indexes") {
		// insert into system.indexes only create index at one shard, so need to transform
		collection := strings.SplitN(shardColSpec.Ns, ".", 2)[1]
		object := bson.D{{"createIndexes", collection}}
		object = append(object, log.Object...)
		tlog := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: "c", Gid: log.Gid,
			Namespace: shardColSpec.Ns, Object: object}
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
			LOG.Info("TransformDDL syncer %v transform DDL log %v to t1log[%v] t2log[%v]",
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
		LOG.Crashf("TransformDDL syncer %v illegal DDL log[%v]", replset, logD)
	default:
		LOG.Crashf("TransformDDL syncer %v meet unsupported DDL log[%s]", replset, logD)
	}
	return nil
}
