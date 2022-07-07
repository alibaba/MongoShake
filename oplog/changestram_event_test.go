package oplog

import (
	"context"
	"fmt"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"testing"

	"github.com/alibaba/MongoShake/v2/unit_test_common"

	nimo "github.com/gugemichael/nimo4go"
	"github.com/stretchr/testify/assert"
)

const (
	testUrl = unit_test_common.TestUrl
)

var (
	client *mongo.Client
)

func marshalData(input bson.M) bson.Raw {
	var dataRaw bson.Raw
	if data, err := bson.Marshal(input); err != nil {
		return nil
	} else {
		dataRaw = data[:]
	}

	return dataRaw
}

func newMongoClient(url string) (*mongo.Client, error) {
	clientOps := options.Client().ApplyURI(url)

	client, err := mongo.NewClient(clientOps)
	if err != nil {
		return nil, fmt.Errorf("new client failed: %v", err)
	}
	if err := client.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect to %s failed: %v", url, err)
	}

	return client, nil
}

// copy from executor.RunCommand
func ApplyOpsFilter(key string) bool {
	// convert to map if has more later
	k := strings.TrimSpace(key)
	if k == "$db" {
		// 40621, $db is not allowed in OP_QUERY requests
		return true
	} else if k == "ui" {
		return true
	}

	return false
}
func RunCommand(database, operation string, log *PartialLog, client *mongo.Client) error {
	defer LOG.Debug("RunCommand run DDL: %v", log.Dump(nil, true))
	dbHandler := client.Database(database)
	LOG.Info("RunCommand run DDL with type[%s]", operation)
	var err error
	switch operation {
	case "createIndexes":
		/*
		 * after v3.6, the given oplog should have uuid when run applyOps with createIndexes.
		 * so we modify oplog base this ref:
		 * https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes
		 */
		var innerBsonD, indexes bson.D
		for i, ele := range log.Object {
			if i == 0 {
				nimo.AssertTrue(ele.Key == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else {
				innerBsonD = append(innerBsonD, ele)
			}
		}
		indexes = append(indexes, log.Object[0]) // createIndexes
		indexes = append(indexes, primitive.E{
			Key: "indexes",
			Value: []bson.D{ // only has 1 bson.D
				innerBsonD,
			},
		})
		err = dbHandler.RunCommand(nil, indexes).Err()
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applysOps nested case, but it is
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
						v[i] = RemoveFiled(doc, "ui")
					}
				case bson.D:
					ret := make(bson.D, 0, len(v))
					for _, ele := range v {
						if ele.Key == "ui" {
							continue
						}
						ret = append(ret, ele)
					}
					ele.Value = ret
				case []bson.M:
					for _, ele := range v {
						if _, ok := ele["ui"]; ok {
							delete(ele, "ui")
						}
					}
				}

			}
			store = append(store, ele)
		}
		err = dbHandler.RunCommand(nil, store).Err()
	case "dropDatabase":
		err = dbHandler.Drop(nil)
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
		fallthrough
	case "convertToCapped":
		fallthrough
	case "renameCollection":
		fallthrough
	case "emptycapped":
		if !IsRunOnAdminCommand(operation) {
			err = dbHandler.RunCommand(nil, log.Object).Err()
		} else {
			err = client.Database("admin").RunCommand(nil, log.Object).Err()
		}
	default:
		LOG.Info("type[%s] not found, use applyOps", operation)

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
	return nil
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
		cursor.Decode(&result)
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
}
