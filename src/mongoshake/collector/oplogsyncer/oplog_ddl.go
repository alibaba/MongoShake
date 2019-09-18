package oplogsyncer

import (
	"encoding/json"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
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
	// number of db shard
	nShard int
	DDLMap map[DDLKey]*DDLValue
	mutex  sync.Mutex
}

func NewDDLManager(nShard int) *DDLManager {
	return &DDLManager{
		nShard: nShard,
		DDLMap: make(map[DDLKey]*DDLValue),
	}
}

func (manager *DDLManager) BlockDDL(replset string, log *oplog.PartialLog) chan bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if object, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(object)}
		if value, ok := manager.DDLMap[ddlKey]; !ok {
			value.TsMap[replset] = log.Timestamp
		} else {
			manager.DDLMap[ddlKey] = &DDLValue{BlockChan: make(chan bool),
				TsMap: make(map[string]bson.MongoTimestamp)}
		}
		if len(manager.DDLMap[ddlKey].TsMap) >= manager.nShard {
			return nil
		}
		return manager.DDLMap[ddlKey].BlockChan
	} else {
		LOG.Crashf("DDLManager BlockDDL json marshal %v error. %v", log.Object, err)
	}
	return nil
}

func (manager *DDLManager) UnBlockDDL(log *oplog.PartialLog) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if object, err := json.Marshal(log.Object); err == nil {
		ddlKey := DDLKey{Namespace: log.Namespace, ObjectStr: string(object)}
		close(manager.DDLMap[ddlKey].BlockChan)
		delete(manager.DDLMap, ddlKey)
	} else {
		LOG.Crashf("DDLManager UnBlockDDL json marshal %v error. %v", log.Object, err)
	}
}

func TransformDDL(replset string, log *oplog.PartialLog, shardColSpec *utils.ShardCollectionSpec) []*oplog.PartialLog {
	operation, _ := oplog.ExtraCommandName(log.Object)
	logD := log.Dump(nil)
	switch operation {
	case "create":
		db := strings.SplitN(log.Namespace, ".", 2)[0]
		t1log := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: log.Operation, Gid: log.Gid,
			Namespace: log.Namespace, Object: bson.D{{"enableSharding", db}}}
		collection, ok := oplog.GetKey(log.Object, "create").(string)
		if !ok {
			LOG.Warn("TransformDDL replset[%v] meet illegal DDL log[%s]", replset, logD)
			return nil
		}
		ns := utils.NS{Database: db, Collection: collection}
		t2log := &oplog.PartialLog{Timestamp: log.Timestamp, Operation: log.Operation, Gid: log.Gid,
			Namespace: log.Namespace, Object: bson.D{{"shardCollection", ns.Str()},
				{"key", shardColSpec.Key}, {"unique", shardColSpec.Unique}}}
		LOG.Info("TransformDDL replset[%v] transform DDL log[%v] to t1log[%v] t2log[%v]",
			replset, logD, t1log, t2log)
		return []*oplog.PartialLog{t1log, t2log}
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
		LOG.Info("TransformDDL replset[%v] ignore DDL log[%v]", replset, logD)
		return nil
	default:
		LOG.Warn("TransformDDL replset[%v] meet unsupported DDL log[%s]", replset, logD)
		return nil
	}
	return nil
}
