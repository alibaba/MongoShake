package sharding

import (
	"fmt"
	"strings"

	"mongoshake/oplog"
	"mongoshake/common"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"reflect"
)

const (
	ConfigDB = "config"

	SettingsCol   = "settings"
	ShardCol      = "shards"
	ChunkCol      = "chunks"
	CollectionCol = "collections"

	HashedShard = "hashed"
	RangedShard = "ranged"

	ConfigShardLogInterval = 3 // s
)

// get balancer status from config server
func GetBalancerStatusByUrl(csUrl string) (bool, error) {
	var conn *utils.MongoConn
	var err error
	if conn, err = utils.NewMongoConn(csUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault); conn == nil || err != nil {
		return true, err
	}
	defer conn.Close()

	var retMap map[string]interface{}
	err = conn.Session.DB(ConfigDB).C(SettingsCol).Find(bson.M{"_id": "balancer"}).Limit(1).One(&retMap)
	if err != nil && err != mgo.ErrNotFound {
		return true, err
	}
	if stopped, ok := retMap["stopped"].(bool); ok {
		return !stopped, nil
	} else {
		return true, nil
	}
}

type ChunkRange struct {
	// the minimum/maximum of the chunk range of multiple columns shard key has multiple values
	Mins []interface{}
	Maxs []interface{}
}

type ShardCollection struct {
	Chunks []*ChunkRange
	// shard key may have multiple columns, for example {a:1, b:1, c:1}
	Keys      []string
	ShardType string
}

// {replset: {namespace: []ChunkRange} }
type ShardingChunkMap map[string]map[string]*ShardCollection

type DBChunkMap map[string]*ShardCollection

func GetChunkMapByUrl(csUrl string) (ShardingChunkMap, error) {
	var conn *utils.MongoConn
	var err error
	if conn, err = utils.NewMongoConn(csUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault); conn == nil || err != nil {
		return nil, err
	}
	defer conn.Close()

	chunkMap := make(ShardingChunkMap)
	type ShardDoc struct {
		Tag  string `bson:"_id"`
		Host string `bson:"host"`
	}
	// map: _id -> replset name
	shardMap := make(map[string]string)
	var shardDoc ShardDoc
	shardIter := conn.Session.DB(ConfigDB).C(ShardCol).Find(bson.M{}).Iter()
	for shardIter.Next(&shardDoc) {
		replset := strings.Split(shardDoc.Host, "/")[0]
		shardMap[shardDoc.Tag] = replset
		chunkMap[replset] = make(DBChunkMap)
	}

	type ChunkDoc struct {
		Ns    string    `bson:"ns"`
		Min   *bson.Raw `bson:"min"`
		Max   *bson.Raw `bson:"max"`
		Shard string    `bson:"shard"`
	}
	// only sharded collections exist on "config.chunks"
	var chunkDoc ChunkDoc
	chunkIter := conn.Session.DB(ConfigDB).C(ChunkCol).Find(bson.M{}).Sort("min").Iter()
	for chunkIter.Next(&chunkDoc) {
		// get all keys and shard type(range or hashed)
		keys, shardType, err := GetColShardType(conn.Session, chunkDoc.Ns)
		if err != nil {
			return nil, err
		}

		// the namespace is sharded, chunk map of each shard need to initialize
		for _, dbChunkMap := range chunkMap{
			if _, ok := dbChunkMap[chunkDoc.Ns]; !ok {
				dbChunkMap[chunkDoc.Ns] = &ShardCollection{Keys: keys, ShardType: shardType}
			}
		}

		// validate "min" and "max" in chunk
		replset := shardMap[chunkDoc.Shard]
		var minD, maxD bson.D
		err1 := bson.Unmarshal(chunkDoc.Min.Data, &minD)
		err2 := bson.Unmarshal(chunkDoc.Max.Data, &maxD)
		if err1 != nil || err2 != nil || len(minD) != len(maxD) {
			return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc min[%v] max[%v]. err1[%v] err2[%v]",
				minD, maxD, err1, err2)
		}

		shardCol := chunkMap[replset][chunkDoc.Ns]
		var mins, maxs []interface{}
		for i, item := range minD {
			if item.Name != shardCol.Keys[i] {
				return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc min[%v] keys[%v]",
					minD, shardCol.Keys)
			}
			mins = append(mins, item.Value)
		}
		for i, item := range maxD {
			if item.Name != shardCol.Keys[i] {
				return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc max[%v] keys[%v]",
					maxD, shardCol.Keys)
			}
			maxs = append(maxs, item.Value)
		}
		chunkRange := &ChunkRange{Mins: mins, Maxs: maxs}
		shardCol.Chunks = append(shardCol.Chunks, chunkRange)
	}
	return chunkMap, nil
}

// input given namespace, return all keys and shard type(range or hashed)
func GetColShardType(session *mgo.Session, namespace string) ([]string, string, error) {
	var colDoc bson.D
	if err := session.DB(ConfigDB).C(CollectionCol).Find(bson.M{"_id": namespace}).One(&colDoc); err != nil {
		return nil, "", err
	}

	var keys []string
	var shardType string
	var ok bool
	if colDoc, ok = oplog.GetKey(colDoc, "key").(bson.D); !ok {
		return nil, "", fmt.Errorf("GetColShardType with namespace[%v] has no key item in doc %v", namespace, colDoc)
	}

	for _, item := range colDoc {
		fmt.Println(item)
		// either be a single hashed field, or a list of ascending fields
		switch v := item.Value.(type) {
		case string:
			shardType = HashedShard
		case int:
			shardType = RangedShard
		case float64:
			shardType = RangedShard
		default:
			return nil, "", fmt.Errorf("GetColShardType with namespace[%v] doc[%v] meet unknown ShakeKey type[%v]",
				namespace, colDoc, reflect.TypeOf(v))
		}
		keys = append(keys, item.Name)
	}
	return keys, shardType, nil
}

type ShardCollectionSpec struct {
	Ns     string
	Key    bson.D
	Unique bool
}

func GetShardCollectionSpec(session *mgo.Session, log *oplog.PartialLog) *ShardCollectionSpec {
	type ConfigDoc struct {
		Timestamp bson.MongoTimestamp `bson:"ts"`
		Operation string              `bson:"op"`
		Object    bson.D              `bson:"o"`
	}
	namespace := GetDDLNamespace(log)

	var configDoc ConfigDoc
	var leftDoc, rightDoc ConfigDoc
	colSpecIter := session.DB("local").C("oplog.rs").
		Find(bson.M{"ns": "config.collections", "o._id": namespace}).Sort("ts:1").Iter()
	defer colSpecIter.Close()
	for colSpecIter.Next(&configDoc) {
		if configDoc.Timestamp < log.Timestamp {
			if leftDoc.Timestamp < configDoc.Timestamp {
				leftDoc = configDoc
			}
		} else {
			rightDoc = configDoc
			break
		}
	}
	if leftDoc.Operation != "" {
		if dropped, ok := oplog.GetKey(leftDoc.Object, "dropped").(bool); ok && !dropped {
			LOG.Info("GetShardCollectionSpec from left doc %v of config.collections for log %v",
				leftDoc, log)
			return &ShardCollectionSpec{Ns: namespace,
				Key:    oplog.GetKey(leftDoc.Object, "key").(bson.D),
				Unique: oplog.GetKey(leftDoc.Object, "unique").(bool),
			}
		}
	}
	if rightDoc.Operation != "" {
		if dropped, ok := oplog.GetKey(rightDoc.Object, "dropped").(bool); ok && !dropped {
			if rightDoc.Timestamp < log.Timestamp+(ConfigShardLogInterval<<32) {
				LOG.Info("GetShardCollectionSpec from right doc %v of config.collections for log %v",
					rightDoc, log)
				return &ShardCollectionSpec{Ns: namespace,
					Key:    oplog.GetKey(rightDoc.Object, "key").(bson.D),
					Unique: oplog.GetKey(rightDoc.Object, "unique").(bool),
				}
			}
			LOG.Warn("GetShardCollectionSpec get no spec from invalid right doc %v of config.collections for log %v",
				rightDoc, log)
		}
	}
	LOG.Info("GetShardCollectionSpec has no config collection spec for ns[%v]", namespace)
	return nil
}

func GetDDLNamespace(log *oplog.PartialLog) string {
	operation, _ := oplog.ExtraCommandName(log.Object)
	logD := log.Dump(nil, true)
	switch operation {
	case "create":
		fallthrough
	case "createIndexes":
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
		fallthrough
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		db := strings.SplitN(log.Namespace, ".", 2)[0]
		collection, ok := oplog.GetKey(log.Object, operation).(string)
		if !ok {
			LOG.Crashf("GetDDLNamespace meet illegal DDL log[%s]", logD)
		}
		return fmt.Sprintf("%s.%s", db, collection)
	case "dropDatabase":
		return log.Namespace
	case "renameCollection":
		ns, ok := oplog.GetKey(log.Object, operation).(string)
		if !ok {
			LOG.Crashf("extraCommandName meets illegal %v oplog %v, ignore!", operation, log.Object)
		}
		return ns
	case "applyOps":
		LOG.Crashf("GetDDLNamespace illegal DDL log[%v]", logD)
	default:
		if strings.HasSuffix(log.Namespace, "system.indexes") {
			namespace, ok := oplog.GetKey(log.Object, "ns").(string)
			if !ok {
				LOG.Crashf("GetDDLNamespace meet illegal DDL log[%s]", logD)
			}
			return namespace
		}
	}
	return log.Namespace
}

func IsSharding(session *mgo.Session) bool {
	var result interface{}
	err := session.DB("config").C("version").Find(bson.M{}).One(&result)
	if err != nil {
		return false
	} else {
		return true
	}
}
