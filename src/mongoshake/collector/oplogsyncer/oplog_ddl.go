package oplogsyncer

import (
	"encoding/json"
	"fmt"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"strings"
	"sync"
)

type DDLKey struct {
	Namespace string
	ObjectStr string
}

type DDLValue struct {
	BlockChan chan bool
	TsMap     map[string]bson.MongoTimestamp
}

type DDLManager struct {
	mutex sync.Mutex
	// number of db shard
	nShard       int
	ddlMap       map[DDLKey]*DDLValue
	FromCsConn   *utils.MongoConn
	ToIsSharding bool
}

func NewDDLManager(nShard int, fromCsUrl string, toUrl string) *DDLManager {
	var fromCsConn *utils.MongoConn
	var err error
	if !conf.Options.ReplayerDMLOnly && fromCsUrl != "" {
		if fromCsConn, err = utils.NewMongoConn(fromCsUrl, utils.ConnectModePrimary, true); err != nil {
			LOG.Crashf("Connect MongoCsUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
		}
	}

	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(toUrl, utils.ConnectModePrimary, true); err != nil {
		LOG.Crashf("Connect toUrl[%v] error[%v].", conf.Options.MongoCsUrl, err)
	}
	defer toConn.Close()

	return &DDLManager{
		nShard:       nShard,
		ddlMap:       make(map[DDLKey]*DDLValue),
		FromCsConn:   fromCsConn,
		ToIsSharding: utils.IsSharding(toConn.Session),
	}
}

func (manager *DDLManager) Enabled() bool {
	return manager.FromCsConn != nil
}

func (manager *DDLManager) BlockDDL(replset string, log *oplog.PartialLog) *DDLValue {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if _, ok := manager.ddlMap[ddlKey]; !ok {
			manager.ddlMap[ddlKey] = &DDLValue{BlockChan: make(chan bool),
				TsMap: make(map[string]bson.MongoTimestamp)}
		}
		manager.ddlMap[ddlKey].TsMap[replset] = log.Timestamp
		if len(manager.ddlMap[ddlKey].TsMap) >= manager.nShard {
			return nil
		}
		return manager.ddlMap[ddlKey]
	} else {
		LOG.Crashf("DDLManager syncer %v BlockDDL json marshal %v error. %v", replset, log.Object, err)
	}
	return nil
}

func (manager *DDLManager) UnBlockDDL(replset string, log *oplog.PartialLog) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if objectStr, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(objectStr)}
		if value, ok := manager.ddlMap[ddlKey]; ok {
			close(value.BlockChan)
			delete(manager.ddlMap, ddlKey)
		} else {
			LOG.Crashf("DDLManager syncer %v ddlKey[%v] not in ddlMap error", replset, ddlKey)
		}
	} else {
		LOG.Crashf("DDLManager syncer %v UnBlockDDL json marshal %v error. %v", replset, log.Object, err)
	}
}

func GetDDLNamespace(replset string, log *oplog.PartialLog) string {
	operation, _ := oplog.ExtraCommandName(log.Object)
	logD := log.Dump(nil)
	switch operation {
	case "create":
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
		db := strings.SplitN(log.Namespace, ".", 2)[0]
		collection, ok := oplog.GetKey(log.Object, operation).(string)
		if !ok {
			LOG.Crashf("GetDDLNamespace syncer %v meet illegal DDL log[%s]", replset, logD)
		}
		return fmt.Sprintf("%s.%s", db, collection)
	case "renameCollection":
		fallthrough
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		fallthrough
	case "applyOps":
		LOG.Crashf("GetDDLNamespace syncer %v ignore DDL log[%v]", replset, logD)
	default:
		if strings.HasSuffix(log.Namespace, "system.indexes") {
			namespace, ok := oplog.GetKey(log.Object, "ns").(string)
			if !ok {
				LOG.Crashf("GetDDLNamespace syncer %v meet illegal DDL log[%s]", replset, logD)
			}
			return namespace
		}
	}
	return log.Namespace
}

func ShardingDDLFilter(log *oplog.PartialLog, shardColSpec *utils.ShardCollectionSpec) bool {
	if strings.HasSuffix(log.Namespace, "system.indexes") {
		indexKey, ok := oplog.GetKey(log.Object, "key").(bson.D)
		if !ok {
			LOG.Crashf("ShardingDDLFilter get key from log object %v failed", log.Object)
		}
		var specKey bson.D
		if err := bson.Unmarshal(shardColSpec.Key.Data, &specKey); err != nil {
			LOG.Crashf("ShardingDDLFilter unmarshal bson %v failed. %v", shardColSpec.Key.Data, err)
		}
		if len(indexKey) == len(specKey) {
			for i := range indexKey {
				if indexKey[i] != specKey[i] {
					return false
				}
			}
			return true
		}
	}
	return false
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
		LOG.Crashf("TransformDDL syncer %v ignore DDL log[%v]", replset, logD)
	default:
		LOG.Crashf("TransformDDL syncer %v meet unsupported DDL log[%s]", replset, logD)
	}
	return nil
}
