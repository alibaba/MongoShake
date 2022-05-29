package sharding

import (
	"context"
	"fmt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"

	"reflect"

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
func GetBalancerStatusByUrl(csUrl string) (bool, error) {
	var conn *utils.MongoCommunityConn
	var err error
	if conn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault,
		conf.Options.MongoSslRootCaFile); conn == nil || err != nil {
		return true, err
	}
	defer conn.Close()

	var result bson.M
	err = conn.Client.Database(ConfigDB).Collection(SettingsCol).FindOne(nil,
		bson.M{"_id": "balancer"}, nil).Decode(&result)
	if err != nil && err != mongo.ErrNoDocuments {
		return true, err
	}
	if stopped, ok := result["stopped"].(bool); ok {
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
	var conn *utils.MongoCommunityConn
	var err error
	if conn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile); conn == nil || err != nil {
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

	shardCursor, err := conn.Client.Database(ConfigDB).Collection(ShardCol).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	for shardCursor.Next(context.Background()) {
		err = shardCursor.Decode(&shardDoc)
		if err != nil {
			LOG.Warn("GetChunkMapByUrl Decode Failed, err[%v]", err)
			continue
		}

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
	chunkCursor, err := conn.Client.Database(ConfigDB).Collection(ChunkCol).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	for chunkCursor.Next(context.Background()) {
		err = chunkCursor.Decode(&chunkDoc)
		if err != nil {
			LOG.Warn("GetChunkMapByUrl Decode Failed, err[%v]", err)
			continue
		}

		// get all keys and shard type(range or hashed)
		keys, shardType, err := GetColShardType(conn, chunkDoc.Ns)
		if err != nil {
			return nil, err
		}

		// the namespace is sharded, chunk map of each shard need to initialize
		for _, dbChunkMap := range chunkMap {
			if _, ok := dbChunkMap[chunkDoc.Ns]; !ok {
				dbChunkMap[chunkDoc.Ns] = &ShardCollection{Keys: keys, ShardType: shardType}
			}
		}

		// validate "min" and "max" in chunk
		replset := shardMap[chunkDoc.Shard]
		var minD, maxD bson.D
		err1 := bson.Unmarshal(*chunkDoc.Min, &minD)
		err2 := bson.Unmarshal(*chunkDoc.Max, &maxD)
		if err1 != nil || err2 != nil || len(minD) != len(maxD) {
			return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc min[%v] max[%v]. err1[%v] err2[%v]",
				minD, maxD, err1, err2)
		}

		shardCol := chunkMap[replset][chunkDoc.Ns]
		var mins, maxs []interface{}
		for i, item := range minD {
			if item.Key != shardCol.Keys[i] {
				return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc min[%v] keys[%v]",
					minD, shardCol.Keys)
			}
			mins = append(mins, item.Value)
		}
		for i, item := range maxD {
			if item.Key != shardCol.Keys[i] {
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
func GetColShardType(conn *utils.MongoCommunityConn, namespace string) ([]string, string, error) {
	var colDoc bson.D
	if err := conn.Client.Database(ConfigDB).Collection(CollectionCol).FindOne(context.Background(),
		bson.M{"_id": namespace}).Decode(&colDoc); err != nil {
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
		keys = append(keys, item.Key)
	}
	return keys, shardType, nil
}

type ShardCollectionSpec struct {
	Ns     string
	Key    bson.D
	Unique bool
}
