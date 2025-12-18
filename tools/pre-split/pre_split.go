// run cmd example:
// ./pre_split.linux -log_dir=./ -src_url="xx" -dst_url="xx" -db_name=ycsb -coll_name=test1 -sample_rate=0.01 -dry_run=true
// ./pre_split.linux -log_dir=./ -db_name="*" -coll_name="*"
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	guid "github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Command-line flags
var (
	srcMongoUrl = flag.String("src_url", "", "src mongodb uri")
	dstMongoUrl = flag.String("dst_url", "", "dst mongodb uri")
	dbName      = flag.String("db_name", "ycsb", "db name")
	collName    = flag.String("coll_name", "test1", "collection name")
	// todo: support pre-shard to another namespace
	// dstDbName = flag.String("to_db_name", "", "dst db name if not the same as src one")
	// dstCollName = flag.String("to_coll_name", "", "dst collection name if not the same as src one")
	sampleRate = flag.Float64("sample_rate", 0.1, "sample rate")
	dryRun     = flag.Bool("dry_run", false, "dry run mode, only print commands without execution")

	Version500 = Version{5, 0, 0}
	Version600 = Version{6, 0, 0}
	Version603 = Version{6, 0, 3}
	SrcV       Version
	DstV       Version

	DbConfig        = "config"
	DbLocal         = "local"
	DbAdmin         = "admin"
	CollDatabases   = "databases"
	CollCollections = "collections"
	CollChunks      = "chunks"
	CollShards      = "shards"
	SampleThreshold = int64(5000)

	ShardKeyTypeRange  = "range"
	ShardKeyTypeHashed = "hashed"
)

// Version for MongoDB, like: 6.0.3
type Version [3]int

func (v1 Version) Cmp(v2 Version) int {
	for i := range v1 {
		if v1[i] < v2[i] {
			return -1
		}
		if v1[i] > v2[i] {
			return 1
		}
	}
	return 0
}

func (v1 Version) LT(v2 Version) bool {
	return v1.Cmp(v2) == -1
}

func (v1 Version) LTE(v2 Version) bool {
	return v1.Cmp(v2) != 1
}

func (v1 Version) GT(v2 Version) bool {
	return v1.Cmp(v2) == 1
}

func (v1 Version) GTE(v2 Version) bool {
	return v1.Cmp(v2) != -1
}

type ShardedCollSpec struct {
	ObjectId string            `bson:"_id"`
	Dropped  bool              `bson:"dropped"`
	ShardKey bson.D            `bson:"key"`
	Unique   bool              `bson:"unique"`
	UUID     *primitive.Binary `bson:"uuid"`
}

type ShardedCollSpec1 struct {
	ObjectId string    `bson:"_id"`
	Dropped  bool      `bson:"dropped"`
	ShardKey bson.D    `bson:"key"`
	Unique   bool      `bson:"unique"`
	UUID     guid.UUID `bson:"uuid"`
}

type ChunkDoc struct {
	ObjectID     primitive.ObjectID  `bson:"_id"`
	Lastmod      primitive.Timestamp `bson:"lastmod"`
	LastmodEpoch primitive.ObjectID  `bson:"lastmodEpoch"`
	Ns           string              `bson:"ns"`
	Min          bson.D              `bson:"min"`
	Max          bson.D              `bson:"max"`
	Shard        string              `bson:"shard"`
	History      []bson.M            `bson:"history"`
}

func GetMongoClient(uri string) (*mongo.Client, error) {
	// set encoder/decoder for registry
	mongoRegistry.RegisterTypeEncoder(tUUID, bsoncodec.ValueEncoderFunc(uuidEncodeValue))
	mongoRegistry.RegisterTypeDecoder(tUUID, bsoncodec.ValueDecoderFunc(uuidDecodeValue))
	// Set client options
	clientOptions := options.Client().ApplyURI(uri).SetRegistry(mongoRegistry)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	log.Infof("Connected to MongoDB:[%v] succeed!", uri)
	return client, nil
}

func initAllClients() (*mongo.Client, *mongo.Client, error) {
	srcClient, err := GetMongoClient(*srcMongoUrl)
	if err != nil {
		log.Errorf("get src mongo client failed: %v", err)
		return nil, nil, err
	}
	err = srcClient.Ping(context.Background(), nil)
	if err != nil {
		log.Errorf("ping src mongo client failed: %v", err)
		return nil, nil, err
	}

	dstClient, err := GetMongoClient(*dstMongoUrl)
	if err != nil {
		// 如果是dryRun模式，不强制需要连接目标数据库
		if *dryRun {
			log.Infof("get dst mongo client failed: %v, but we are in dry-run mode, skip...", err)
			return srcClient, nil, nil
		}
		log.Errorf("get dst mongo client failed: %v", err)
		return srcClient, nil, err
	}

	err = dstClient.Ping(context.Background(), nil)
	if err != nil {
		log.Errorf("ping mongo client failed: %v, %v", err, err)
		return srcClient, dstClient, err
	}

	return srcClient, dstClient, nil
}

func getDbVersionArray(client *mongo.Client) (Version, error) {
	var version Version
	var res bson.M
	err := client.Database(DbAdmin).RunCommand(context.Background(), bson.M{"buildInfo": 1}).Decode(&res)
	if err != nil {
		return version, fmt.Errorf("error getting buildInfo: %v", err)
	}

	versionStr, ok := res["version"].(string)
	if !ok {
		return Version{}, fmt.Errorf("failed to retrieve version string")
	}

	versionParts := strings.Split(versionStr, ".")
	if len(versionParts) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %s", versionStr)
	}

	for i, part := range versionParts {
		num, err := strconv.Atoi(part)
		if err != nil {
			return Version{}, fmt.Errorf("nvalid version number: %s", part)
		}
		version[i] = num
	}

	return version, nil
}

// check if both client is created through mongos and have sufficient permissions(not implemented)
func preCheck(src *mongo.Client, dst *mongo.Client) (Version, Version, error) {
	var SrcVersion, dstVersion Version
	// check src
	result := bson.M{}
	err := src.Database(DbAdmin).RunCommand(context.Background(), bson.D{{"isMaster", 1}}).
		Decode(&result)
	if err != nil {
		log.Errorf("src run 'isMaster' failed:%v", err)
		return SrcVersion, dstVersion, err
	}
	// check if connected to mongos
	if result["msg"] == "isdbgrid" {
		log.Infof("src connected to a mongos")
	} else {
		log.Errorf("src didn't connect to a mongos")
		return SrcVersion, dstVersion, err
	}
	// get dbVersion array
	SrcVersion, err = getDbVersionArray(src)
	if err != nil {
		log.Errorf("src getDbVersionArray failed:%v", err)
		return SrcVersion, dstVersion, err
	}

	// check dst
	err = dst.Database(DbAdmin).RunCommand(context.Background(), bson.D{{"isMaster", 1}}).
		Decode(&result)
	if err != nil {
		// 如果是dryRun模式，不强制目标数据库检查
		if *dryRun {
			return SrcVersion, dstVersion, nil
		}
		log.Errorf("dst run 'isMaster' failed:%v", err)
		return SrcVersion, dstVersion, err
	}
	if result["msg"] == "isdbgrid" {
		log.Infof("src connected to a mongos")
	} else {
		log.Errorf("src didn't connect to a mongos")
		return SrcVersion, dstVersion, err
	}
	dstVersion, err = getDbVersionArray(dst)
	if err != nil {
		log.Errorf("dst getDbVersionArray failed:%v", err)
		return SrcVersion, dstVersion, err
	}

	return SrcVersion, dstVersion, nil
}

// getShardKeyAndUuid return shard key, type(range/hashed) and uuid if exists
func getShardKeyAndUuid(client *mongo.Client, dbName, collName string) (bson.D, string, bool, *primitive.Binary, error) {
	collection := client.Database(DbConfig).Collection(CollCollections)
	filter := bson.D{{Key: "_id", Value: dbName + "." + collName}}

	var result ShardedCollSpec
	err := collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		if err.Error() == mongo.ErrNoDocuments.Error() {
			log.Errorf("namespace %s.%s is not sharded or does not exist", dbName, collName)
			return nil, "", false, nil, err
		}
		return nil, "", false, nil, fmt.Errorf("error retrieving sharding information: %v", err)
	}
	log.Infof("found namespace %s.%s in config.collections, spec:%v", dbName, collName, result)

	// debug for uuid type
	var result1 ShardedCollSpec1
	err1 := collection.FindOne(context.Background(), filter).Decode(&result1)
	if err != nil {
		if err1.Error() == mongo.ErrNoDocuments.Error() {
			log.Errorf("namespace %s.%s is not sharded or does not exist", dbName, collName)
			return nil, "", false, nil, err1
		}
		return nil, "", false, nil, fmt.Errorf("error retrieving sharding information: %v", err1)
	}
	log.Infof("sharded coll spec result:%v", result1)
	// zhongli: the guid.UUID implementation is working good!
	// Raw log:
	// ---------------sharded coll spec result:{ycsb.test1 false [{_id 1}] false f5860009-b1f8-445c-b98d-9f9d46e61834}
	// Doc in config.collections:
	// { "_id" : "ycsb.test1",
	//   "lastmodEpoch" : ObjectId("66695522e3680eddfd4dbe84"),
	//   "lastmod" : ISODate("2024-06-12T07:58:27.260Z"),
	//   "timestamp" : Timestamp(1718179106, 19),
	//   "uuid" : UUID("f5860009-b1f8-445c-b98d-9f9d46e61834"),
	//   "key" : { "_id" : 1 },
	//   "unique" : false,
	//   "noBalance" : false }

	shardKeyType := ShardKeyTypeRange
	hashedKey := ""

	// check if hashed, could be hashed index or compound hashed index:
	// 1) { "fieldA" : "hashed"}
	// 2) { "fieldA" : "hashed", "fieldB": 1}
	// 3) { "fieldA" : 1, "fieldB" : "hashed", "fieldC": 1}
	// 4) { "fieldA" : 1, "fieldB" : 1, "fieldC" : "hashed" }
	for _, key := range result.ShardKey {
		if key.Value == "hashed" {
			shardKeyType = ShardKeyTypeHashed
			hashedKey = key.Key
		}
	}

	if shardKeyType == ShardKeyTypeHashed && hashedKey == "" {
		return result.ShardKey, "", false, nil, fmt.Errorf("unexpected shard key: %v", result.ShardKey)
	}

	if len(result.ShardKey) == 0 {
		return result.ShardKey, "", false, nil, fmt.Errorf("unexpected config.collections doc:%+v", result)
	}

	return result.ShardKey, shardKeyType, result1.Unique, result.UUID, nil
}

// getShardsList return all shardNames from config.shards
func getShardsList(c *mongo.Client) ([]string, error) {
	cursor, err := c.Database(DbConfig).Collection(CollShards).Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var shardNames []string
	for cursor.Next(context.Background()) {
		var shard bson.M
		if err := cursor.Decode(&shard); err != nil {
			return nil, err
		}
		shardName, _ := shard["_id"].(string)
		shardNames = append(shardNames, shardName)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return shardNames, nil
}

// getPrimaryShard return the primary shard name of specified db
func getPrimaryShard(c *mongo.Client, dbName string) (string, error) {
	filter := bson.D{{"_id", fmt.Sprintf("%s", dbName)}}
	var result bson.M
	err := c.Database(DbConfig).Collection(CollDatabases).FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		return "", fmt.Errorf("find db:%s in config.databses failed: %v", dbName, err)
	}
	primaryShard, ok := result["primary"].(string)
	if !ok {
		return "", fmt.Errorf("unexpected doc:%+v", result)
	}
	return primaryShard, nil
}

func moveChunkIfNeeded(dstC *mongo.Client, dbName, collName string, hasHashed bool) error {
	// get new uuid for namespace
	shardKey, shardType, _, uuid, err := getShardKeyAndUuid(dstC, dbName, collName)
	if hasHashed && shardType != ShardKeyTypeHashed {
		return fmt.Errorf("unexpected shard key:%v", shardKey)
	}

	// count chunk num
	chunkNum, err := countChunks(dstC, dbName, collName, uuid, DstV)
	if err != nil {
		log.Errorf("count chunk num failed: %v", err)
		return err
	}
	log.Infof("dst chunk num:%v", chunkNum)

	// get all shards list
	shardNames, err := getShardsList(dstC)
	if err != nil {
		return fmt.Errorf("get shards list failed: %v", err)
	}

	// get primary shards
	primaryShard, err := getPrimaryShard(dstC, dbName)
	if err != nil {
		return fmt.Errorf("get primary shard failed: %v", err)
	}

	// iter all chunks and evenly move then to all shards

	filter := bson.D{{Key: "uuid", Value: *uuid}}
	cursor, err := dstC.Database(DbConfig).Collection(CollChunks).Find(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("error retrieving chunks: %v", err)
	}
	defer cursor.Close(context.Background())

	i := 0
	for cursor.Next(context.Background()) {
		i++
		var chunk ChunkDoc
		if err := cursor.Decode(&chunk); err != nil {
			return fmt.Errorf("error decoding chunk: %v", err)
		}
		if chunk.Shard != primaryShard {
			log.Errorf("unexpected chunk info:%+v, not in primary shard:%v", chunk, primaryShard)
			return fmt.Errorf("unexpected chunk info:%+v", chunk)
		}
		targetShard := shardNames[i%len(shardNames)]
		if targetShard == primaryShard {
			continue
		} else {
			var cmd bson.D
			if haveMin, _ := haveMinMaxKey(chunk.Min, chunk.Max); haveMin {
				continue
			} else {
				if !hasHashed {
					// use moveChunk cmd with 'find' options
					cmd = bson.D{
						{Key: "moveChunk", Value: fmt.Sprintf("%s.%s", dbName, collName)},
						{Key: "find", Value: chunk.Min},
						{Key: "to", Value: targetShard},
					}
				} else {
					// use moveChunk cmd with 'bounds' options
					cmd = bson.D{
						{Key: "moveChunk", Value: fmt.Sprintf("%s.%s", dbName, collName)},
						{Key: "bounds", Value: bson.A{chunk.Min, chunk.Max}},
						{Key: "to", Value: targetShard},
					}
				}
			}

			if *dryRun {
				log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
			} else {
				res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
				if res.Err() != nil {
					return fmt.Errorf("movechunk failed: %v, cmd: %v", res.Err(), cmd)
				}
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error iterating config.chunks: %v", err)
	}
	return nil
}

// haveMinMaxKey return true if bound have 'minKey' or 'maxKey'
func haveMinMaxKey(min, max bson.D) (bool, bool) {
	haveMin, haveMax := false, false
	for _, v := range min {
		if _, ok := v.Value.(primitive.MinKey); ok {
			haveMin = true
			break
		}
	}
	for _, v := range max {
		if _, ok := v.Value.(primitive.MaxKey); ok {
			haveMax = true
			break
		}
	}
	return haveMin, haveMax
}

// preSharingForRange does the pre-sharing work for range sharding
func preSharingForRange(srcC, dstC *mongo.Client, dbName, collName string, shardKey bson.D,
	unique bool, uuid *primitive.Binary, chunkNum int64) error {
	// run 'enableSharding' and 'shardCollection'
	if SrcV.GTE(Version600) {
		cmd := bson.D{{Key: "enableSharding", Value: dbName}}
		if *dryRun {
			log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
		} else {
			res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
			if res.Err() != nil {
				return fmt.Errorf("error enabling sharding for %s: %v", dbName, res.Err())
			}
		}
	}
	cmd := bson.D{
		{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", dbName, collName)},
		{Key: "key", Value: shardKey},
		{Key: "unique", Value: unique},
	}
	if *dryRun {
		log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
	} else {
		res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
		if res.Err() != nil {
			return fmt.Errorf("error sharding collection %s: %v", collName, res.Err())
		} else {
			log.Infof("run shardCollection for %s.%s succeed", dbName, collName)
		}
	}

	// check if shard key have hashed field
	hasHashedField := false
	for _, v := range shardKey {
		if v.Value == "hashed" {
			hasHashedField = true
			break
		}
	}

	// should use 'uuid' if version > 5.0.0, otherwise use 'ns'
	var filter bson.D
	if SrcV.GTE(Version500) {
		filter = bson.D{{Key: "uuid", Value: *uuid}}
	} else {
		filter = bson.D{{Key: "ns", Value: dbName + "." + collName}}
	}
	log.Infof("Filter: %v", filter)
	opts := options.Find().SetSort(bson.D{{Key: "min", Value: 1}})
	// get the chunk distribution for specified namespace from the src 'config.chunks'
	cursor, err := srcC.Database(DbConfig).Collection(CollChunks).Find(context.Background(), filter, opts)
	if err != nil {
		return fmt.Errorf("error retrieving chunk distribution for %s.%s: %v", dbName, collName, err)
	}
	defer cursor.Close(context.Background())

	// do not use sample if chunk Num < threshold
	if chunkNum <= SampleThreshold {
		*sampleRate = 1.0
	}
	expectedNum := int(math.Floor(*sampleRate * float64(chunkNum)))
	step := int(chunkNum) / expectedNum
	log.Infof("Sample rate:%v, expected chunk num:%v, step:%v", *sampleRate, chunkNum, step)
	i := 0
	for cursor.Next(context.Background()) {
		i++
		if i%step != 0 {
			continue
		}
		var chunk ChunkDoc
		if err := cursor.Decode(&chunk); err != nil {
			log.Fatal(err)
		}
		log.Infof("chunk:%+v", chunk)

		if chunk.Min == nil || chunk.Max == nil {
			return fmt.Errorf("unexpected chunk doc:%+v", chunk)
		}

		// skip minKey
		haveMin, _ := haveMinMaxKey(chunk.Min, chunk.Max)
		if haveMin {
			log.Infof("skipping chunk with minKey:%v", chunk)
			continue
		}
		cmd = bson.D{
			{Key: "split", Value: fmt.Sprintf("%s.%s", dbName, collName)},
			{Key: "middle", Value: chunk.Min},
		}
		if *dryRun {
			log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
		} else {
			res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
			if res.Err() != nil {
				return fmt.Errorf("error splitting chunk: %v, cmd: %v", res.Err(), cmd)
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error iterating config.chunks: %v", err)
	}

	// version > 6.0.3, need to manually move chunk, otherwise all split chunks are in the same shard
	if DstV.GTE(Version603) {
		if dstC == nil {
			log.Infof("skip moveChunk since dst client is invalid, we may in dry run mode.")
			return nil
		}
		return moveChunkIfNeeded(dstC, dbName, collName, hasHashedField)
	} else {
		log.Infof("dst version <6.0.3, server will automatically complete balancing based on chunks num")
	}

	return nil
}

// preShardingForHashed does the pre-sharding work for hashed sharding
func preShardingForHashed(srcC, dstC *mongo.Client, dbName, collName string, shardKey bson.D,
	unique bool, uuid *primitive.Binary, chunkNum int64) error {
	// simple use 'shardCollection' with 'numInitialChunks' option,
	// but only when the hashed index key is the prefix of shard key
	if shardKey[0].Value == "hashed" {
		// run 'enableSharding', it's ok to run multi times
		if SrcV.GTE(Version600) {
			cmd := bson.D{{Key: "enableSharing", Value: dbName}}
			if *dryRun {
				log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
			} else {
				res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
				if res.Err() != nil {
					return fmt.Errorf("error enabling sharding for %s: %v", dbName, res.Err())
				}
			}
		}

		// run 'shardCollection'
		// todo: support other options like 'collation'/'timeseries'
		cmd := bson.D{
			{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", dbName, collName)},
			{Key: "key", Value: shardKey},
			{Key: "unique", Value: unique},
			{Key: "numInitialChunks", Value: chunkNum}, // 或者根据你的需求指定一个合适的值
		}
		if *dryRun {
			log.Infof("[DRY_RUN] db.adminCommand( %v )", cmd)
		} else {
			res := dstC.Database(DbAdmin).RunCommand(context.Background(), cmd)
			if res.Err() != nil {
				return fmt.Errorf("error pre-splitting hashed collection %s.%s: %v", dbName, collName, res.Err())
			}
		}
	} else {
		// use preSharingForRange instead
		return preSharingForRange(srcC, dstC, dbName, collName, shardKey, unique, uuid, chunkNum)
	}

	return nil
}

// countChunks count the number of chunks for specified sharded collection, support all db versions
func countChunks(c *mongo.Client, dbName, collName string, uuid *primitive.Binary, dbVersion Version) (int64, error) {
	var filter bson.D
	// should use 'uuid' if version > 5.0.0, otherwise use 'ns'
	if dbVersion.GTE(Version500) {
		// example:
		// {
		// "_id" : ObjectId("6661174bb4e237ce6ff403c5"),
		// "uuid" : UUID("65358c89-9c57-4311-83fa-1f9b09f40c55"),
		// "min" : { "_id" : { "$minKey" : 1 } },
		// "max" : { "_id" : NumberLong("-4611686018427387902") },
		// "shard" : "d-2ze109bad99675f4",
		// "lastmod" : Timestamp(1, 0),
		// "history" : [ { "validAfter" : Timestamp(1717638987, 16), "shard" : "d-2ze109bad99675f4" } ]
		// }
		if uuid == nil {
			return 0, fmt.Errorf("ns:[%s.%s] uuid not found", dbName, collName)
		}
		filter = bson.D{{Key: "uuid", Value: *uuid}}
	} else {
		// example:
		// {
		// "_id" : ObjectId("6659b1518d8d30acea4454fe"),
		// "lastmod" : Timestamp(10, 0),
		// "lastmodEpoch" : ObjectId("6659b151b90470686bd3d809"),
		// "ns" : "ycsb.test1",
		// "min" : { "_id" : { "$minKey" : 1 } },
		// "max" : { "_id" : "user1000133110176059407" },
		// "shard" : "d-2ze561024b478524",
		// "history" : [ { "validAfter" : Timestamp(1717388165, 150), "shard" : "d-2ze561024b478524" } ]
		// }
		filter = bson.D{{Key: "ns", Value: dbName + "." + collName}}
	}
	log.Infof("filter: %+v", filter)

	count, err := c.Database(DbConfig).Collection(CollChunks).
		CountDocuments(context.Background(), filter)
	if err != nil {
		return 0, fmt.Errorf("error counting chunks for %s.%s: %v", dbName, collName, err)
	}

	return count, nil
}

func handleNamespace(srcC *mongo.Client, dstC *mongo.Client, dbName, collName string) error {
	//todo:check if namespace is valid

	// check if range sharding or hashed sharding
	var res bson.M
	err := srcC.Database(DbConfig).Collection(CollDatabases).
		FindOne(context.Background(), bson.D{{"_id", dbName}}).Decode(&res)
	if err != nil {
		log.Errorf("try to find db:%v in config.databases failed: %v", dbName, err)
		return err
	}

	// only version < 6.0.0 can use partitioned to check if a database's sharding enabled status
	if SrcV.GTE(Version600) {
		log.Infof("skip check a database's sharding enabled status since the db version is %v", SrcV)
	} else {
		if partitioned, ok := res["partitioned"]; ok {
			if partitioned == true {
				log.Infof("db [%s] is partitioned", dbName)
			} else {
				return fmt.Errorf("db [%s] is not partitioned", dbName)
			}
		}
	}

	// check if the collection is sharded and get shard key & uuid
	shardKey, shardType, unique, uuid, err := getShardKeyAndUuid(srcC, dbName, collName)
	if err != nil {
		log.Errorf("get shard key and uuid failed: %v", err)
		return err
	} else {
		log.Infof("get shard key:%v, type:%v, uuid:%v", shardKey, shardType, uuid)
	}

	// count chunk nums
	var chunkNum int64
	chunkNum, err = countChunks(srcC, dbName, collName, uuid, SrcV)
	if err != nil {
		log.Errorf("count chunk num failed: %v", err)
		return err
	}
	log.Infof("ns: [%s.%s] chunk num in total: %v", dbName, collName, chunkNum)

	switch shardType {
	case "range":
		// do pre-sharding for range sharding
		log.Infof("ns:[%s.%s]pre-sharding(range)", dbName, collName)
		err = preSharingForRange(srcC, dstC, dbName, collName, shardKey, unique, uuid, chunkNum)
		if err != nil {
			log.Errorf("pre-sharding(range) for ns:[%s.%s] failed: %v", dbName, collName, err)
			return err
		}
		log.Infof("pre-sharding(range) for ns:[%s.%s] succeed!", dbName, collName)
	case "hashed":
		// do pre-sharding for hashed sharding
		err = preShardingForHashed(srcC, dstC, dbName, collName, shardKey, unique, uuid, chunkNum)
		if err != nil {
			log.Errorf("pre-sharding(hashed) for ns:[%s.%s] failed: %v", dbName, collName, err)
			return err
		}
		log.Infof("pre-sharding(hashed) for ns: [%s.%s] succeed!", dbName, collName)
	default:
		return fmt.Errorf("unexpected shardType: %v", shardType)
	}

	return nil
}

func main() {
	// Parse command-line flags
	flag.Parse()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// init src & dst mongo client
	srcC, dstC, err := initAllClients()
	if err != nil {
		log.Fatalf("init mongo client failed: %v", err)
	}
	log.Infof("init mongo client succeed!")

	// pre-check
	SrcV, DstV, err = preCheck(srcC, dstC)
	if err != nil {
		log.Fatalf("pre-check failed: %v", err)
	}
	log.Infof("pre-check done, src version: %v, dst version: %v", SrcV, DstV)

	// handle all namespaces or specified namespace
	if *dbName == "*" && *collName == "*" {
		log.Infoln("handle all namespaces")
		dbList, err := srcC.ListDatabaseNames(context.Background(), bson.D{})
		if err != nil {
			log.Exitf("list database names failed: %v", err)
		}
		for _, d := range dbList {
			if d == DbConfig || d == DbLocal || d == DbAdmin {
				continue
			}
			log.Infof("handle database: %v", d)
			// handle all collections in the database
			colls, err := srcC.Database(d).ListCollectionNames(context.Background(), bson.D{})
			if err != nil {
				log.Exitf("list collection names failed: %v", err)
			}
			for _, coll := range colls {
				log.Infof("handle collection: %v", coll)
				// handle the collection
				if err = handleNamespace(srcC, dstC, d, coll); err != nil {
					if strings.Contains(err.Error(), "is not partitioned") {
						log.Warningf("skip db:%s since is not partitioned", d)
					} else {
						log.Exitf("handle namespace failed: %v", err)
					}
				}
			}
		}
	} else if *dbName != "*" && *collName == "*" {
		log.Infof("handle specified databases: %v", *dbName)
		// handle all collections in the database
		colls, err := srcC.Database(*dbName).ListCollectionNames(context.Background(), bson.D{})
		if err != nil {
			log.Exitf("list collection names failed: %v", err)
		}
		for _, coll := range colls {
			log.Infof("handle collection: %v", coll)
			// handle the collection
			err = handleNamespace(srcC, dstC, *dbName, coll)
			if err != nil {
				log.Exitf("handle namespace failed: %v", err)
			}
		}
	} else {
		log.Infof("handle specified namespace: %v.%v", *dbName, *collName)
		err = handleNamespace(srcC, dstC, *dbName, *collName)
		if err != nil {
			log.Exitf("handle namespace failed: %v", err)
		}
	}

	_ = srcC.Disconnect(context.Background())
	_ = dstC.Disconnect(context.Background())
	log.Infoln("DONE")
	log.Flush()
}

// ------------------------------
// This is a value (de|en)coder for the github.com/google/uuid UUID type. For best experience, register
// mongoRegistry to mongo client instance via options, e.g.
//  clientOptions := options.Client().SetRegistry(mongoRegistry)
//
// Only BSON binary subtype 0x04 is supported.

// No official implementation. see more details in https://jira.mongodb.org/browse/GODRIVER-2484
var (
	tUUID       = reflect.TypeOf(guid.UUID{})
	uuidSubtype = byte(0x04)

	mongoRegistry = bson.NewRegistry()
)

func uuidEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tUUID {
		return bsoncodec.ValueEncoderError{Name: "uuidEncodeValue", Types: []reflect.Type{tUUID}, Received: val}
	}
	b := val.Interface().(guid.UUID)
	return vw.WriteBinaryWithSubtype(b[:], uuidSubtype)
}

func uuidDecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tUUID {
		return bsoncodec.ValueDecoderError{Name: "uuidDecodeValue", Types: []reflect.Type{tUUID}, Received: val}
	}

	var data []byte
	var subtype byte
	var err error
	switch vrType := vr.Type(); vrType {
	case bson.TypeBinary:
		data, subtype, err = vr.ReadBinary()
		if subtype != uuidSubtype {
			return fmt.Errorf("unsupported binary subtype %v for UUID", subtype)
		}
	case bson.TypeNull:
		err = vr.ReadNull()
	case bson.TypeUndefined:
		err = vr.ReadUndefined()
	default:
		return fmt.Errorf("cannot decode %v into a UUID", vrType)
	}

	if err != nil {
		return err
	}
	uuid2, err := guid.FromBytes(data)
	if err != nil {
		return err
	}
	val.Set(reflect.ValueOf(uuid2))
	return nil
}
