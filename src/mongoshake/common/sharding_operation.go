package utils

import (
	"fmt"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/oplog"
	"strings"

	LOG "github.com/vinllen/log4go"
)

const (
	ConfigDB = "config"

	SettingsCol   = "settings"
	ShardCol      = "shards"
	ChunkCol      = "chunks"
	CollectionCol = "collections"

	HashedShard = "hashed"
	RangedShard = "ranged"
)

// get balancer status from config server
func GetBalancerStatusByUrl(url string) (bool, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModePrimary, true); conn == nil || err != nil {
		return true, err
	}
	defer conn.Close()

	var retMap map[string]interface{}
	err = conn.Session.DB(ConfigDB).C(SettingsCol).Find(bson.M{"_id": "balancer"}).Limit(1).One(&retMap)
	if err != nil {
		return true, err
	}
	return !retMap["stopped"].(bool), nil
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

func GetChunkMapByUrl(url string) (ShardingChunkMap, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModePrimary, true); conn == nil || err != nil {
		return nil, err
	}
	defer conn.Close()

	chunkMap := make(ShardingChunkMap)
	type ShardDoc struct {
		Tag  string `bson:"_id"`
		Host string `bson:"host"`
	}
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
	var chunkDoc ChunkDoc
	chunkIter := conn.Session.DB(ConfigDB).C(ChunkCol).Find(bson.M{}).Sort("min").Iter()
	for chunkIter.Next(&chunkDoc) {
		replset := shardMap[chunkDoc.Shard]
		if _, ok := chunkMap[replset][chunkDoc.Ns]; !ok {
			keys, shardType, err := GetColShardType(conn.Session, chunkDoc.Ns)
			if err != nil {
				return nil, err
			}
			chunkMap[replset][chunkDoc.Ns] = &ShardCollection{Keys: keys, ShardType: shardType}
		}
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

func GetColShardType(session *mgo.Session, namespace string) ([]string, string, error) {
	var colDoc bson.D
	if err := session.DB(ConfigDB).C(CollectionCol).Find(bson.M{"_id": namespace}).One(&colDoc); err != nil {
		return nil, "", err
	}
	var keys []string
	var shardType string
	var ok bool
	if colDoc, ok = oplog.GetKey(colDoc, "key").(bson.D); !ok {
		return nil, "", fmt.Errorf("GetColShardType has no key item in doc %v", colDoc)
	}
	for _, item := range colDoc {
		// either be a single hashed field, or a list of ascending fields
		switch item.Value.(type) {
		case string:
			shardType = HashedShard
		case float64:
			shardType = RangedShard
		default:
			return nil, "", fmt.Errorf("GetColShardType meet unknown ShakeKey type %v", colDoc)
		}
		keys = append(keys, item.Name)
	}
	return keys, shardType, nil
}

type ShardCollectionSpec struct {
	Ns      string    `bson:"_id"`
	Key     *bson.Raw `bson:"key"`
	Unique  bool      `bson:"unique"`
	Dropped bool      `bson:"dropped"`
}

func GetShardCollectionSpec(session *mgo.Session, namespace string) *ShardCollectionSpec {
	var colSpecDoc ShardCollectionSpec
	var err error
	// enable sharding for db
	colSpecIter := session.DB("config").C("collections").Find(bson.M{"_id": namespace}).Iter()
	for colSpecIter.Next(&colSpecDoc) {
		if !colSpecDoc.Dropped {
			return &colSpecDoc
		}
	}
	if err = colSpecIter.Close(); err != nil {
		LOG.Critical("Close iterator of config.collections failed. %v", err)
	}
	return nil
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
