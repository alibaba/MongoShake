package oplog

import (
	"testing"
	"fmt"
	"strings"

	"mongoshake/unit_test_common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"github.com/vinllen/mgo"
	"github.com/gugemichael/nimo4go"
)

const (
	testUrl = unit_test_common.TestUrl
)

var (
	session *mgo.Session
)

// copy from executor.RunCommand
func RunCommand(database, operation string, log *PartialLog, session *mgo.Session) error {
	dbHandler := session.DB(database)
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
				nimo.AssertTrue(ele.Name == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else {
				innerBsonD = append(innerBsonD, ele)
			}
		}
		indexes = append(indexes, log.Object[0]) // createIndexes
		indexes = append(indexes, bson.DocElem{
			Name: "indexes",
			Value: []bson.D{ // only has 1 bson.D
				innerBsonD,
			},
		})
		err = dbHandler.Run(indexes, nil)
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applysOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		var store bson.D
		for _, ele := range log.Object {
			/*if utils.ApplyOpsFilter(ele.Name) {
				continue
			}*/
			if ele.Name == "applyOps" {
				arr := ele.Value.([]interface{})
				for i, ele := range arr {
					doc := ele.(bson.D)
					arr[i] = RemoveFiled(doc, "ui")
				}
			}
			store = append(store, ele)
		}
		err = dbHandler.Run(store, nil)
	case "dropDatabase":
		err = dbHandler.DropDatabase()
	case "create":
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
			err = dbHandler.Run(log.Object, nil)
		} else {
			err = session.DB("admin").Run(log.Object, nil)
		}
	default:
		// filter log.Object
		var rec bson.D
		for _, ele := range log.Object {
			/*if utils.ApplyOpsFilter(ele.Name) {
				continue
			}*/

			rec = append(rec, ele)
		}
		log.Object = rec // reset log.Object

		var store bson.D
		store = append(store, bson.DocElem{
			Name: "applyOps",
			Value: []bson.D{
				log.Dump(nil, true),
			},
		})
		err = dbHandler.Run(store, nil)
	}

	return err
}

func runOplog(data *PartialLog) error {
	ns := strings.Split(data.Namespace, ".")
	switch data.Operation {
	case "i":
		return session.DB(ns[0]).C(ns[1]).Insert(data.Object)
	case "d":
		return session.DB(ns[0]).C(ns[1]).Remove(data.Object)
	case "u":
		return session.DB(ns[0]).C(ns[1]).Update(data.Query, data.Object)
	case "c":
		operation, found := ExtraCommandName(data.Object)
		if !found {
			return fmt.Errorf("extract command failed")
		}

		return RunCommand(ns[0], operation, data, session)
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
	it := session.DB(db).C(coll).Find(bson.M{}).Iter()
	ret := make(map[string]bson.M)
	result := make(bson.M)
	for it.Next(&result) {
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
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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
				bson.DocElem{
					Name:  "_id",
					Value: "2",
				},
				bson.DocElem{
					Name:  "a",
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
			DocumentKey: bson.M{
				"_id": "2",
			},
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "2",
				},
				bson.DocElem{
					Name:  "a",
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
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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
				bson.DocElem{
					Name:  "_id",
					Value: "2",
				},
				bson.DocElem{
					Name:  "a",
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
			DocumentKey: bson.M{
				"_id": "2",
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
			DocumentKey: bson.M{
				"_id": "1",
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
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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

		list, err := session.DB("testDb").CollectionNames()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test insert & rename collection
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		_ = session.DB("testDb2").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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

		list, err := session.DB("testDb").CollectionNames()
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
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
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

		list, err := session.DB("testDb").CollectionNames()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test simple transaction
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		var err error
		session, err = mgo.Dial(testUrl)
		assert.Equal(t, nil, err, "should be equal")

		err = session.DB("testDb").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert a:1
		eventInsert1 := Event{
			OperationType: "insert",
			FullDocument: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "1",
				},
				bson.DocElem{
					Name:  "a",
					Value: "1",
				},
			},
			Ns: bson.M{
				"db":   "testDb",
				"coll": "testColl",
			},
			TxnNumber: 1,
			Lsid: bson.M{
				"id" : "70c47e76-7f48-46cb-ad07-cbeefd29d664",
				"uid" : "Y5mrDaxi8gv8RmdTsQ+1j7fmkr7JUsabhNmXAheU0fg=",
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
			TxnNumber: 2,
			Lsid: bson.M{
				"id" : "70c47e76-7f48-46cb-ad07-cbeefd29d664",
				"uid" : "Y5mrDaxi8gv8RmdTsQ+1j7fmkr7JUsabhNmXAheU0fg=",
			},
		}
		out, err = bson.Marshal(eventRename1)
		assert.Equal(t, nil, err, "should be equal")

		err = runByte(out)
		assert.Equal(t, nil, err, "should be equal")

		list, err := session.DB("testDb").CollectionNames()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(list), "should be equal")
	}

	// test update without FullDocument field but with IncrSyncChangeStreamWatchFullDocument
	{
		fmt.Printf("TestConvertEvent2Oplog case %d.\n", nr)
		nr++

		eventUpdate1 := Event{
			OperationType: "update",
			DocumentKey: bson.M{
				"_id": "2",
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