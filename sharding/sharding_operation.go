package sharding

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
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
	// Shard key columns and the corresponding per-column shard type.
	// ShardTypes[i] is the type of Keys[i] and is either HashedShard or
	// RangedShard. Compound hashed keys (MongoDB 4.4+) such as
	// {a: 1, b: "hashed"} are represented with a mix of values; consumers
	// must check ShardTypes[i] per key instead of treating the collection
	// as uniformly hashed or ranged.
	Keys       []string
	ShardTypes []string
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
			l.Logger.Warnf("GetChunkMapByUrl Decode Failed, err[%v]", err)
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
			l.Logger.Warnf("GetChunkMapByUrl Decode Failed, err[%v]", err)
			continue
		}

		// get all keys and per-key shard type(range or hashed)
		keys, shardTypes, err := GetColShardType(conn, chunkDoc.Ns)
		if err != nil {
			return nil, err
		}

		// the namespace is sharded, chunk map of each shard need to initialize
		for _, dbChunkMap := range chunkMap {
			if _, ok := dbChunkMap[chunkDoc.Ns]; !ok {
				dbChunkMap[chunkDoc.Ns] = &ShardCollection{Keys: keys, ShardTypes: shardTypes}
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

// GetColShardType returns the shard key columns and the per-column shard
// type for the given namespace. The second return value has the same length
// as the first; each entry is either HashedShard or RangedShard.
//
// Compound shard keys can mix the two — e.g. {a: 1, b: "hashed"} (MongoDB
// 4.4+ compound hashed). Callers must consult the per-column type when
// deciding whether to ComputeHash; treating the whole collection as a
// single type silently mis-classifies documents at the shard boundary.
func GetColShardType(conn *utils.MongoCommunityConn, namespace string) ([]string, []string, error) {
	var colDoc bson.D
	if err := conn.Client.Database(ConfigDB).Collection(CollectionCol).FindOne(context.Background(),
		bson.M{"_id": namespace}).Decode(&colDoc); err != nil {
		return nil, nil, err
	}

	keyDoc, ok := oplog.GetKey(colDoc, "key").(bson.D)
	if !ok {
		return nil, nil, fmt.Errorf("GetColShardType with namespace[%v] has no key item in doc %v", namespace, colDoc)
	}
	keys, shardTypes, err := parseShardKey(keyDoc)
	if err != nil {
		return nil, nil, fmt.Errorf("GetColShardType with namespace[%v]: %w", namespace, err)
	}
	return keys, shardTypes, nil
}

// parseShardKey turns a config.collections "key" subdocument into the per-
// column (name, type) pair. It is the pure data path of GetColShardType and
// is split out so we can unit-test compound hashed shard keys without a
// mongos. Each item's value is either a string ("hashed") or a numeric
// direction (1 / -1, including int32/int64/float64 as decoded by the BSON
// driver). The two slices returned are 1:1 with keyDoc and are intentionally
// not coalesced — callers like OrphanFilter.Filter rely on the per-column
// type rather than a collection-wide flag.
func parseShardKey(keyDoc bson.D) ([]string, []string, error) {
	keys := make([]string, 0, len(keyDoc))
	shardTypes := make([]string, 0, len(keyDoc))
	for _, item := range keyDoc {
		switch v := item.Value.(type) {
		case string:
			shardTypes = append(shardTypes, HashedShard)
		case int, int32, int64, float64:
			shardTypes = append(shardTypes, RangedShard)
		default:
			return nil, nil, fmt.Errorf("shard key field[%v] has unsupported value type[%v]",
				item.Key, reflect.TypeOf(v))
		}
		keys = append(keys, item.Key)
	}
	return keys, shardTypes, nil
}

type ShardCollectionSpec struct {
	Ns     string
	Key    bson.D
	Unique bool
}
