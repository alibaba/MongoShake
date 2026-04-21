package oplog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	nimo "github.com/gugemichael/nimo4go"
	"github.com/mongodb/mongo-tools-common/json"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

const (
	testUrl                  = unit_test_common.TestUrl
	testMongoShardingAddress = unit_test_common.TestUrlSharding

	uuidMark = "ui"
)

var (
	client *mongo.Client
)

// below structs are for decode result of listIndexes
// IndexInfo
type IndexInfo struct {
	Key                bson.D `bson:"key"`
	Name               string `bson:"name"`
	V                  int32  `bson:"v"`
	ExpireAfterSeconds int32  `bson:"expireAfterSeconds"`
	Hidden             bool   `bson:"hidden"`
	PrepareUnique      bool   `bson:"prepareUnique"`
	Unique             bool   `bson:"unique"`
}

// Cursor
type Cursor struct {
	ID         int64       `bson:"id"`
	NS         string      `bson:"ns"`
	FirstBatch []IndexInfo `bson:"firstBatch"`
}

// ListIndexesResult
type ListIndexesResult struct {
	Cursor Cursor  `bson:"cursor"`
	OK     float64 `bson:"ok"`
}

// DocCollections is for result of config.collections
type DocCollections struct {
	ObjectId  string              `bson:"_id"`
	Timestamp primitive.Timestamp `bson:"timestamp"`
	Uuid      *primitive.Binary   `bson:"uuid"`
	ShardKey  bson.D              `bson:"key"`
	Unique    bool                `bson:"unique"`
	NoBalance bool                `bson:"noBalance"`
}

func marshalData(input bson.M) bson.Raw {
	var dataRaw bson.Raw
	if data, err := bson.Marshal(input); err != nil {
		return nil
	} else {
		dataRaw = data[:]
	}

	return dataRaw
}

// newMongoClient only used in unit test
func newMongoClient(url string) (*mongo.Client, error) {
	encodedURL, err := utils.EncodeMongoURI(url)
	if err != nil {
		return nil, fmt.Errorf("failed to encode MongoDB URL: %v", err)
	}
	clientOps := options.Client().ApplyURI(encodedURL)

	client, err := mongo.NewClient(clientOps)
	if err != nil {
		return nil, fmt.Errorf("new client failed: %v", err)
	}
	if err := client.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect to %s failed: %v", url, err)
	}

	return client, nil
}

// ApplyOpsFilter is synced from utils.ApplyOpsFilter
func ApplyOpsFilter(key string) bool {
	k := strings.TrimSpace(key)
	if k == "$db" {
		// 40621, $db is not allowed in OP_QUERY requests
		return true
	} else if k == "ui" {
		return true
	}

	return false
}

// RunCommand is synced from executor.RunCommand
func RunCommand(database, operation string, log *PartialLog, client *mongo.Client) error {
	defer l.Logger.Debugf("RunCommand run DDL: %v", log.Dump(nil, true))
	dbHandler := client.Database(database)
	l.Logger.Infof("RunCommand run DDL with type[%s]", operation)
	var err error
	switch operation {
	case "createIndexes":
		/*
		 * after v3.6, the given oplog should have uuid when run applyOps with createIndexes.
		 * so we modify oplog base this ref:
		 * https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes
		 * Note: Here we have to handle original createIndex oplog (only 1 index) or
		 * createIndexes oplog (maybe multi indexes) converted from change events.
		 */
		var innerBsonD, command bson.D
		var indexes bson.A
		var ok bool
		for i, ele := range log.Object {
			if i == 0 {
				nimo.AssertTrue(ele.Key == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else if ele.Key == "indexes" {
				indexes, ok = ele.Value.(bson.A)
				if !ok {
					err = fmt.Errorf("unexpected createIndexes oplog:%v", log)
				}
			} else {
				// "o" : { "createIndexes" : "test", "v" : 2, "key" : { "a" : "hashed" }, "name" : "a_hashed" }
				innerBsonD = append(innerBsonD, ele)
			}
		}
		command = append(command, log.Object[0]) // createIndexes
		if len(indexes) != 0 {
			command = append(command, primitive.E{Key: "indexes", Value: indexes})
		} else {
			command = append(command, primitive.E{
				Key: "indexes",
				Value: []bson.D{ // only has 1 bson.D
					innerBsonD,
				},
			})
		}
		err = dbHandler.RunCommand(nil, command).Err()
	case "commitIndexBuild":
		/*
			If multiple indexes are created, 'commitIndexBuild' only generate one oplog,
			however 'createIndexes' will generate multi oplogs.
				{
					"op" : "c",
					"ns" : "test.$cmd",
					"ui" : UUID("617ffe90-6dac-4e71-b570-1825422c1896"),
					"o" : {
						"commitIndexBuild" : "car",
						"indexBuildUUID" : UUID("4e9b7457-b612-42bb-bbad-bd6e9a2d63a7"),
						"indexes" : [
							{ "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" },
							{ "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" }
						]},
					"ts" : Timestamp(1653620229, 6),
					"t" : NumberLong(1),
					"v" : NumberLong(2),
					"wall" : ISODate("2022-05-27T02:57:09.187Z")
				}
				Equivalent 'createIndexes' command:
				> db.car.createIndexes([{"count":1},{"type":1}])
				{
					"ts": Timestamp(1653620582, 3),
					"t": NumberLong(2),
					"h": NumberLong(0),
					"v": 2,
					"op": "c",
					"ns": "test.$cmd",
					"ui": UUID("51d35827-e8b5-4891-8818-41326718505d"),
					"wall": ISODate("2022-05-27T03:03:02.282Z"),
					"o": {
						"createIndexes": "car",
						"v": 2,
						"key": {"type": 1},
						"name": "type_1"
					}
				}
				{
					"ts": Timestamp(1653620582, 2),
					"t": NumberLong(2),
					"h": NumberLong(0),
					"v": 2,
					"op": "c",
					"ns": "test.$cmd",
					"ui": UUID("51d35827-e8b5-4891-8818-41326718505d"),
					"wall": ISODate("2022-05-27T03:03:02.281Z"),
					"o": {
						"createIndexes": "car",
						"v": 2,
						"key": {"count": 1},
						"name": "count_1"
					}
				}
		*/
		var command bson.D
		for i, ele := range log.Object {
			if i == 0 {
				command = append(command, primitive.E{
					Key:   "createIndexes",
					Value: ele.Value.(string),
				})
				nimo.AssertTrue(ele.Key == "commitIndexBuild", "should panic when ele.Name != 'commitIndexBuild'")
			} else {
				if ele.Key == "indexes" {
					command = append(command, primitive.E{
						Key:   "indexes",
						Value: ele.Value,
					})
				}
			}
		}

		nimo.AssertTrue(len(command) >= 2, "createIndexes command must at least have two elements")
		l.Logger.Debugf("RunCommand commitIndexBuild oplog after conversion[%v]", command)
		err = dbHandler.RunCommand(nil, command).Err()
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applyOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		var store bson.D
		for _, ele := range log.Object {
			if ApplyOpsFilter(ele.Key) {
				continue
			}
			if ele.Key == "applyOps" {
				switch v := ele.Value.(type) {
				case []interface{}:
					for i, ele := range v {
						doc := ele.(bson.D)
						v[i] = RemoveFiled(doc, uuidMark)
					}
				case bson.D:
					ret := make(bson.D, 0, len(v))
					for _, ele := range v {
						if ele.Key == uuidMark {
							continue
						}
						ret = append(ret, ele)
					}
					ele.Value = ret
				case []bson.M:
					for _, ele := range v {
						if _, ok := ele[uuidMark]; ok {
							delete(ele, uuidMark)
						}
					}
				}

			}
			store = append(store, ele)
		}
		err = dbHandler.RunCommand(nil, store).Err()
	case "dropDatabase":
		err = dbHandler.Drop(nil)
	case "renameCollection":
		// handle '"dropTarget": UUID("52c1c147-2408-4d96-9d0f-889a759ab079")' in object,
		// change to {dropTarget: true} as described in:
		//https://www.mongodb.com/docs/v5.0/reference/method/db.collection.renameCollection/
		tmpCmd := log.Object
		if log.Object != nil && GetKey(log.Object, "dropTarget") != nil {
			SetFiled(tmpCmd, "dropTarget", true)
		}
		err = client.Database("admin").RunCommand(nil, tmpCmd).Err()
	case "create":
		if GetKey(log.Object, "autoIndexId") != nil &&
			GetKey(log.Object, "idIndex") != nil {
			// exits "autoIndexId" and "idIndex", remove "autoIndexId"
			log.Object = RemoveFiled(log.Object, "autoIndexId")
		}
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
		// using applyOps directly, refer to mongo-tools:HandleNonTxnOp
		// ensure every shard collection's uuid is same
		doc := bson.M{}
		doc["ts"] = log.Timestamp
		if log.Version > 0 {
			doc["v"] = log.Version
		}
		doc["op"] = log.Operation
		if log.Gid != "" {
			doc["g"] = log.Gid
		}
		doc["ns"] = log.Namespace
		if log.Object != nil && len(log.Object) > 0 {
			doc["o"] = log.Object
		}
		if log.Query != nil && len(log.Query) > 0 {
			doc["o2"] = log.Query
		}
		// uuid BinDataType == 3 or 4, see "BinDataType" in src/mongo/bson/bsontypes.h
		if log.UI != nil && (log.UI.Subtype == 3 || log.UI.Subtype == 4) {
			doc["ui"] = log.UI
		}
		l.Logger.Infof("run applyOps for %s: %v", operation, doc)
		err = client.Database("admin").RunCommand(context.Background(),
			bson.D{{"applyOps", []interface{}{doc}}}, nil).Err()
		if err != nil {
			l.Logger.Errorf("run applyOps[%v] for %s failed: %v", doc, operation, err)
			return err
		}
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		fallthrough

		// NOTE: below commands are carefully-constructed oplogs transformed from change events which could call
		//'runCommand()' directly, this does not mean that these exist in the native MongoDB oplog.
	case "shardCollection":
		fallthrough
	case "reshardCollection":
		fallthrough
	case "refineCollectionShardKey":
		if !IsRunOnAdminCommand(operation) {
			err = dbHandler.RunCommand(nil, log.Object).Err()
		} else {
			err = client.Database("admin").RunCommand(nil, log.Object).Err()
		}
	default:
		l.Logger.Infof("type[%s] not found, use applyOps", operation)

		// filter log.Object
		var rec bson.D
		for _, ele := range log.Object {
			if ApplyOpsFilter(ele.Key) {
				continue
			}

			rec = append(rec, ele)
		}
		log.Object = rec // reset log.Object

		var store bson.D
		store = append(store, primitive.E{
			Key: "applyOps",
			Value: []bson.D{
				log.Dump(nil, true),
			},
		})
		err = dbHandler.RunCommand(nil, store).Err()
	}

	return err
}

func runOplog(data *PartialLog) error {
	ns := strings.Split(data.Namespace, ".")
	switch data.Operation {
	case "i":
		_, err := client.Database(ns[0]).Collection(ns[1]).InsertOne(context.Background(), data.Object)
		return err
	case "d":
		_, err := client.Database(ns[0]).Collection(ns[1]).DeleteOne(context.Background(), data.Object)
		return err
	case "u":
		_, err := client.Database(ns[0]).Collection(ns[1]).UpdateOne(context.Background(), data.Query, data.Object)
		return err
	case "c":
		operation, found := ExtraCommandName(data.Object)
		if !found {
			return fmt.Errorf("extract command failed")
		}

		return RunCommand(ns[0], operation, data, client)
	default:
		return fmt.Errorf("unknown op[%v]", data.Operation)
	}
}

func runByte(input []byte) error {
	data, err := ConvertEvent2Oplog(input, false)
	if err != nil {
		return err
	}
	fmt.Println(data)

	return runOplog(data)
}

func getAllDoc(db, coll string) map[string]bson.M {
	cursor, _ := client.Database(db).Collection(coll).Find(context.Background(), bson.M{})
	ret := make(map[string]bson.M)
	result := make(bson.M)
	for cursor.Next(context.Background()) {
		_ = cursor.Decode(&result)
		ret[result["_id"].(string)] = result
		result = make(bson.M)
	}
	return ret
}

func TestConvertEvent2Oplog(t *testing.T) {
	// test TestConvertEvent2Oplog
	var nr int

	// test insert only
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")
	}

	// test insert & replace
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		// insert a:2
		eventInsert2 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "2",
				},
				bson.E{
					Key:   "a",
					Value: "2",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventInsert2)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		// replace a:2 => a:20
		eventUpdate1 := Event{
			OperationType: "replace",
			DocumentKey: bson.D{
				{"_id", "2"},
			},
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "2",
				},
				bson.E{
					Key:   "a",
					Value: "20",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventUpdate1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 2, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")
		assert.Equal(t, "20", all["2"]["a"], "should be equal")
	}

	// test insert & update & delete
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		// insert a:2
		eventInsert2 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "2",
				},
				bson.E{
					Key:   "a",
					Value: "2",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventInsert2)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		// update a:2 => b:300, c:400
		eventUpdate1 := Event{
			OperationType: "update",
			DocumentKey: bson.D{
				{"_id", "2"},
			},
			UpdateDescription: bson.M{
				"updatedFields": bson.M{
					"b": "300",
					"c": "400",
				},
				"removedFields": []string{"a"},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventUpdate1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 2, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")
		assert.Equal(t, nil, all["2"]["a"], "should be equal")
		assert.Equal(t, "300", all["2"]["b"], "should be equal")
		assert.Equal(t, "400", all["2"]["c"], "should be equal")

		// delete a:1
		eventDelete1 := Event{
			OperationType: "delete",
			DocumentKey: bson.D{
				{"_id", "1"},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventDelete1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all = getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, bson.M(nil), all["1"], "should be equal")
		assert.Equal(t, nil, all["2"]["a"], "should be equal")
		assert.Equal(t, "300", all["2"]["b"], "should be equal")
		assert.Equal(t, "400", all["2"]["c"], "should be equal")
	}

	// test insert & drop collection
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")

		// drop collection
		eventDropCollection1 := Event{
			OperationType: "drop",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventDropCollection1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		list, err := client.Database("testDb").ListCollectionNames(context.Background(), bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test insert & rename collection
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		_ = client.Database("testDb2").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")

		// rename testDb.testColl => testDb2.testColl2
		eventRename1 := Event{
			OperationType: "rename",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			To: bson.M{
				"db":   "testDb2",
				"coll": "testColl2",
			},
		}
		out, err = bson.Marshal(eventRename1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		list, err := client.Database("testDb").ListCollectionNames(context.Background(), bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")

		all = getAllDoc("testDb2", "testColl2")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")
	}

	// test insert & drop database
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")

		// drop testDb
		eventRename1 := Event{
			OperationType: "dropDatabase",
			Ns: bson.M{
				"db": "testDb",
			},
		}
		out, err = bson.Marshal(eventRename1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		list, err := client.Database("testDb").ListCollectionNames(context.Background(), bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test create & createIndexes & modify & dropIndexes
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++
		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		// create collection
		eventCreateCollection := Event{
			OperationType: "create",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{
					Key:   "changeStreamPreAndPostImages",
					Value: bson.D{bson.E{Key: "enabled", Value: true}},
				},
				bson.E{
					Key: "idIndex",
					Value: bson.D{
						bson.E{Key: "v", Value: 2},
						bson.E{Key: "key", Value: bson.D{bson.E{Key: "_id", Value: 1}}},
						bson.E{Key: "name", Value: "_id_"},
					},
				},
			},
		}
		out, err := bson.Marshal(eventCreateCollection)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		// createIndexes
		eventCreateIndexes := Event{
			OperationType: "createIndexes",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{
					Key: "indexes",
					Value: bson.A{
						bson.D{
							bson.E{Key: "v", Value: 2},
							bson.E{Key: "key", Value: bson.D{bson.E{Key: "a", Value: 1}}},
							bson.E{Key: "name", Value: "a_1"},
						},
						bson.D{
							bson.E{Key: "v", Value: 2},
							bson.E{Key: "key", Value: bson.D{bson.E{Key: "b", Value: 1}}},
							bson.E{Key: "name", Value: "b_1"},
						},
					},
				},
			},
		}
		out, err = bson.Marshal(eventCreateIndexes)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err = bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")
		var result ListIndexesResult
		err = client.Database("testDb").RunCommand(context.Background(),
			bson.D{bson.E{Key: "listIndexes", Value: "testColl"}}).Decode(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result.Cursor.FirstBatch), "should be equal")
		assert.Equal(t, "_id_", result.Cursor.FirstBatch[0].Name, "should be equal")
		assert.Equal(t, "a_1", result.Cursor.FirstBatch[1].Name, "should be equal")
		assert.Equal(t, "b_1", result.Cursor.FirstBatch[2].Name, "should be equal")
		// modify (use {prepareUnique:true} & {hidden:ture} for example)
		eventModifyUnique := Event{
			OperationType: "modify",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{
					Key: "index",
					Value: bson.D{
						bson.E{Key: "name", Value: "a_1"},
						bson.E{Key: "prepareUnique", Value: true},
					},
				},
			},
		}
		out, err = bson.Marshal(eventModifyUnique)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("testDb").RunCommand(context.Background(),
			bson.D{bson.E{Key: "listIndexes", Value: "testColl"}}).Decode(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result.Cursor.FirstBatch), "should be equal")
		for _, idx := range result.Cursor.FirstBatch {
			if idx.Name == "a_1" {
				assert.Equal(t, true, idx.PrepareUnique, "should be equal")
			}
		}
		eventModifyHidden := Event{
			OperationType: "modify",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{
					Key: "index",
					Value: bson.D{
						bson.E{Key: "name", Value: "a_1"},
						bson.E{Key: "hidden", Value: true},
					},
				},
			},
		}
		out, err = bson.Marshal(eventModifyHidden)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("testDb").RunCommand(context.Background(),
			bson.D{bson.E{Key: "listIndexes", Value: "testColl"}}).Decode(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result.Cursor.FirstBatch), "should be equal")
		for _, idx := range result.Cursor.FirstBatch {
			if idx.Name == "a_1" {
				assert.Equal(t, true, idx.Hidden, "should be equal")
			}
		}
		// dropIndexes
		eventDropIndexes := Event{
			OperationType: "dropIndexes",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{
					Key: "indexes",
					Value: bson.A{
						bson.D{
							bson.E{Key: "v", Value: 2},
							bson.E{Key: "key", Value: bson.D{bson.E{Key: "a", Value: 1}}},
							bson.E{Key: "name", Value: "a_1"},
						},
					},
				},
			},
		}
		out, err = bson.Marshal(eventDropIndexes)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("testDb").RunCommand(context.Background(),
			bson.D{bson.E{Key: "listIndexes", Value: "testColl"}}).Decode(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(result.Cursor.FirstBatch), "should be equal")
		assert.Equal(t, "_id_", result.Cursor.FirstBatch[0].Name, "should be equal")
		assert.Equal(t, "b_1", result.Cursor.FirstBatch[1].Name, "should be equal")
	}

	// test simple transaction
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		txnN := []int64{0, 1, 2}
		var err error
		client, err = newMongoClient(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.E{
					Key:   "_id",
					Value: "1",
				},
				bson.E{
					Key:   "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			TxnNumber: &txnN[1],
			LSID: marshalData(bson.M{
				"id":  "70c47e76-7f48-46cb-ad07-cbeefd29d664",
				"uid": "Y5mrDaxi8gv8RmdTsQ+1j7fmkr7JUsabhNmXAheU0fg=",
			}),
		}
		out, err := bson.Marshal(eventInsert1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		all := getAllDoc("testDb", "testColl")
		assert.Equal(t, 1, len(all), "should be equal")
		assert.Equal(t, "1", all["1"]["a"], "should be equal")

		// drop testDb
		eventRename1 := Event{
			OperationType: "dropDatabase",
			Ns: bson.M{
				"db": "testDb",
			},
			TxnNumber: &txnN[2],
			LSID: marshalData(bson.M{
				"id":  "70c47e76-7f48-46cb-ad07-cbeefd29d664",
				"uid": "Y5mrDaxi8gv8RmdTsQ+1j7fmkr7JUsabhNmXAheU0fg=",
			}),
		}
		out, err = bson.Marshal(eventRename1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		list, err := client.Database("testDb").ListCollectionNames(context.Background(), bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test update without FullDocument field but with IncrSyncChangeStreamWatchFullDocument
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		eventUpdate1 := Event{
			OperationType: "update",
			DocumentKey: bson.D{
				{"_id", "2"},
			},
			UpdateDescription: bson.M{
				"updatedFields": bson.M{
					"b": "300",
					"c": "400",
				},
				"removedFields": []string{"a"},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
		}
		out, err := bson.Marshal(eventUpdate1)
		assert.Equal(t, nil, err, "should be equal")

		oplog, err := ConvertEvent2Oplog(out, true)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, oplog.Object != nil && len(oplog.Object) > 0, "should be equal")
		fmt.Println(oplog)
	}

	// test sharding events: shardCollection/reshardCollection/refineCollectionShardKey
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++
		var err error
		_ = client.Disconnect(context.Background())
		client, err = newMongoClient(testMongoShardingAddress)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("testDb").Drop(context.Background())
		assert.Equal(t, nil, err, "should be equal")
		// shardCollection
		eventShardCollection := Event{
			OperationType: "shardCollection",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{Key: "shardKey", Value: bson.D{bson.E{Key: "_id", Value: "hashed"}}},
				bson.E{Key: "unique", Value: false},
				bson.E{Key: "numInitialChunks", Value: json.NumberLong(0)},
				bson.E{Key: "presplitHashedZones", Value: false},
			},
		}
		out, err := bson.Marshal(eventShardCollection)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		collNames, err := client.Database("testDb").ListCollectionNames(context.Background(),
			bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, len(collNames), "should be equal")
		assert.Equal(t, collNames[0], "testColl", "should be equal")
		count, err := client.Database("config").Collection("collections").CountDocuments(context.Background(),
			bson.M{"_id": "testDb.testColl"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(1), count, "should be equal")
		// insert some data
		for i := 0; i < 1000; i++ {
			_, err = client.Database("testDb").Collection("testColl").InsertOne(context.Background(),
				bson.M{"_id": i, "a": i, "msg": "hello world test data"})
			assert.Equal(t, nil, err, "should be equal")
		}
		// reshardCollection
		eventReShardCollection := Event{
			OperationType: "reshardCollection",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{Key: "shardKey", Value: bson.D{bson.E{Key: "_id", Value: 1}}},
				bson.E{Key: "oldShardKey", Value: bson.D{bson.E{Key: "_id", Value: "hashed"}}},
				bson.E{Key: "unique", Value: false},
			},
		}
		out, err = bson.Marshal(eventReShardCollection)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		var document DocCollections
		err = client.Database("config").Collection("collections").FindOne(context.Background(),
			bson.M{"_id": "testDb.testColl"}).Decode(&document)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.D{bson.E{Key: "_id", Value: int32(1)}}, document.ShardKey, "should be equal")
		// refineCollectionShardKey, have to create corresponding index first
		indexModel := mongo.IndexModel{
			Keys: bson.D{
				{Key: "_id", Value: 1},
				{Key: "a", Value: 1},
			},
			Options: options.Index().SetUnique(false).SetName("_id_1_a_1"),
		}
		_, err = client.Database("testDb").Collection("testColl").
			Indexes().CreateOne(context.Background(), indexModel)
		eventRefineShardKey := Event{
			OperationType: "refineCollectionShardKey",
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			OperationDescription: bson.D{
				bson.E{Key: "shardKey", Value: bson.D{
					bson.E{Key: "_id", Value: 1},
					bson.E{Key: "a", Value: 1},
				}},
				bson.E{Key: "oldShardKey", Value: bson.E{Key: "_id", Value: 1}},
			},
		}
		out, err = bson.Marshal(eventRefineShardKey)
		assert.Equal(t, nil, err, "should be equal")
		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")
		err = client.Database("config").Collection("collections").FindOne(context.Background(),
			bson.M{"_id": "testDb.testColl"}).Decode(&document)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, bson.D{bson.E{Key: "_id", Value: int32(1)}, bson.E{Key: "a", Value: int32(1)}}, document.ShardKey, "should be equal")
	}
}
