package oplog

import (
	"fmt"
	"encoding/json"

	"github.com/vinllen/mgo/bson"
)

const (
	// field in oplog
	OplogTsName          = "ts"
	OplogOperationName   = "op"
	OplogGidName         = "g" // useless in change stream
	OplogNamespaceName   = "ns"
	OplogObjectName      = "o"
	OplogQueryName       = "o2"
	OplogUniqueKeyName   = "uk" // useless in change stream
	OplogLsidName        = "lsid"
	OplogFromMigrateName = "fromMigrate"
)

/*
 * example:
	{
	    _id : { // 存储元信息
	        "_data" : <BinData|hex string> // resumeToken
	    },
	    "operationType" : "<operation>", // insert, delete, replace, update, drop, rename, dropDatabase, invalidate
	    "fullDocument" : { <document> }, // 修改后的数据，出现在insert, replace, delete, update. 相当于原来的o字段
	    "ns" : { // 就是ns
	        "db" : "<database>",
	        "coll" : "<collection"
	    },
	    "to" : { // 只在operationType==rename的时候有效，表示改名以后的ns
	        "db" : "<database>",
	        "coll" : "<collection"
	    },
	    "documentKey" : { "_id" : <value> }, // 相当于o2字段。出现在insert, replace, delete, update。正常只包含_id，对于sharded collection，还包括shard key。
	    "updateDescription" : { // 只在operationType==update的时候出现，相当于是增量的修改，而replace是替换。
	        "updatedFields" : { <document> }, // 更新的field的值
	        "removedFields" : [ "<field>", ... ] // 删除的field列表
	    },
        "FullDocument" : { //永不为 nil
            "fullDocument" : { <document> }, // 开启full_document之后，为updateLookup，不开启则为default
        }
	    "clusterTime" : <Timestamp>, // 相当于ts字段
	    "txnNumber" : <NumberLong>, // 相当于oplog里面的txnNumber，只在事务里面出现。事务号在一个事务里面单调递增
	    "lsid" : { // 相当于lsid字段，只在事务里面出现。logic session id，请求所在的session的id。
	        "id" : <UUID>,
	        "uid" : <BinData>
	    }
	}
 */
type Event struct {
	Id                bson.M              `bson:"_id" json:"_id"`
	OperationType     string              `bson:"operationType" json:"operationType"`
	FullDocument      bson.D              `bson:"fullDocument" json:"fullDocument"`  // exists on "insert", "replace", "delete", "update"
	Ns                bson.M              `bson:"ns" json:"ns"`
	To                bson.M              `bson:"to" json:"to"`
	DocumentKey       bson.M              `bson:"documentKey" json:"documentKey"`  // exists on "insert", "replace", "delete", "update"
	UpdateDescription bson.M              `bson:"updateDescription" json:"updateDescription"`
	ClusterTime       bson.MongoTimestamp `bson:"clusterTime" json:"clusterTime"`
	TxnNumber         uint64              `bson:"txnNumber" json:"txnNumber"`
	Lsid              bson.M              `bson:"lsid" json:"lsid"`
}

func (e *Event) String() string {
	if ret, err := json.Marshal(e); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

func ConvertEvent2Oplog(input []byte, fulldoc bool) (*PartialLog, error) {
	event := new(Event)
	if err := bson.Unmarshal(input, event); err != nil {
		return nil, fmt.Errorf("unmarshal raw bson[%s] failed: %v", input, err)
	}

	oplog := new(PartialLog)

	// ts
	oplog.Timestamp = event.ClusterTime

	ns := event.Ns

	// do nothing for "g", "uk", "fromMigrate"

	// handle different operation type
	switch event.OperationType {
	case "insert":
		// insert zz.test {"kick":1}
		/* event:
		{
		    "_id" : {
		        "_data" : "825E4FA224000000012B022C0100296E5A100420D9F949CFC7496EA80E32BA633701A846645F696400645E4FA224A6717632D6EE2E850004"
		    },
		    "operationType" : "insert",
		    "clusterTime" : Timestamp(1582277156, 1),
		    "fullDocument" : {
		        "_id" : ObjectId("5e4fa224a6717632d6ee2e85"),
		        "kick" : 1
		    },
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    },
		    "documentKey" : {
		        "_id" : ObjectId("5e4fa224a6717632d6ee2e85")
		    }
		}
		*/
		/* oplog:
		{
		    "ts" : Timestamp(1582277156, 1),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "i",
		    "ns" : "zz.test",
		    "ui" : UUID("20d9f949-cfc7-496e-a80e-32ba633701a8"),
		    "wall" : ISODate("2020-02-21T09:25:56.570Z"),
		    "o" : {
		        "_id" : ObjectId("5e4fa224a6717632d6ee2e85"),
		        "kick" : 1
		    }
		}*/
		oplog.Namespace = fmt.Sprintf("%s.%s", event.Ns["db"], event.Ns["coll"])
		oplog.Operation = "i"
		oplog.Object = event.FullDocument
	case "delete":
		// remove zz.test {"kick":1}
		/* event:
		{
		    "_id" : {
		        "_data" : "825E537D02000000012B022C0100296E5A1004EE9B60D8845F42FF989D09018A730D6046645F696400645E537CF27DC0F30426F01B770004"
		    },
		    "operationType" : "delete",
		    "clusterTime" : Timestamp(1582529794, 1),
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    },
		    "documentKey" : {
		        "_id" : ObjectId("5e537cf27dc0f30426f01b77")
		    }
		}
		*/
		/* oplog
		{
		    "ts" : Timestamp(1582529794, 1),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "d",
		    "ns" : "zz.test",
		    "ui" : UUID("ee9b60d8-845f-42ff-989d-09018a730d60"),
		    "wall" : ISODate("2020-02-24T07:36:34.063Z"),
		    "o" : {
		        "_id" : ObjectId("5e537cf27dc0f30426f01b77")
		    }
		}
		*/
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "d"
		oplog.Object = ConvertBsonM2D(event.DocumentKey)
	case "replace":
		// db.test.update({"kick":1}, {"kick":10, "ok":true})
		/* event
		{
		    "_id" : {
		        "_data" : "825E538501000000012B022C0100296E5A1004EE9B60D8845F42FF989D09018A730D6046645F696400645E5384F97DC0F30426F01B790004"
		    },
		    "operationType" : "replace",
		    "clusterTime" : Timestamp(1582531841, 1),
		    "fullDocument" : {
		        "_id" : ObjectId("5e5384f97dc0f30426f01b79"),
		        "kick" : 10,
		        "ok" : true
		    },
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    },
		    "documentKey" : {
		        "_id" : ObjectId("5e5384f97dc0f30426f01b79")
		    }
		}
		*/
		/*
			{
			    "ts" : Timestamp(1582531841, 1),
			    "t" : NumberLong(1),
			    "h" : NumberLong(0),
			    "v" : 2,
			    "op" : "u",
			    "ns" : "zz.test",
			    "ui" : UUID("ee9b60d8-845f-42ff-989d-09018a730d60"),
			    "o2" : {
			        "_id" : ObjectId("5e5384f97dc0f30426f01b79")
			    },
			    "wall" : ISODate("2020-02-24T08:10:41.636Z"),
			    "o" : {
			        "_id" : ObjectId("5e5384f97dc0f30426f01b79"),
			        "kick" : 10,
			        "ok" : true
			    }
			}
		*/
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "u"
		oplog.Query = event.DocumentKey
		oplog.Object = event.FullDocument
	case "update":
		/*
		 * mgset-xxx:PRIMARY> db.test.find()
		 * { "_id" : ObjectId("5e5384f97dc0f30426f01b79"), "kick" : 10, "ok" : true }
		 * mgset-xxx:PRIMARY> db.test.update({"kick":10}, {$set:{"plus_field":2}, $unset:{"ok":1}})
		 * mgset-xxx:PRIMARY> db.test.find()
		 * { "_id" : ObjectId("5e5384f97dc0f30426f01b79"), "kick" : 10, "plus_field" : 2 }
		 */
		/* event:
		{
		    "_id" : {
		        "_data" : "825E5389D5000000022B022C0100296E5A1004EE9B60D8845F42FF989D09018A730D6046645F696400645E5384F97DC0F30426F01B790004"
		    },
		    "operationType" : "update",
		    "clusterTime" : Timestamp(1582533077, 2),
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    },
		    "documentKey" : {
		        "_id" : ObjectId("5e5384f97dc0f30426f01b79")
		    },
		    "updateDescription" : {
		        "updatedFields" : {
		            "plus_field" : 2
		        },
		        "removedFields" : [ "ok" ]
		    }
		}
		*/
		/* oplog
		{
		    "ts" : Timestamp(1582533077, 2),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "u",
		    "ns" : "zz.test",
		    "ui" : UUID("ee9b60d8-845f-42ff-989d-09018a730d60"),
		    "o2" : {
		        "_id" : ObjectId("5e5384f97dc0f30426f01b79")
		    },
		    "wall" : ISODate("2020-02-24T08:31:17.681Z"),
		    "o" : {
		        "$v" : 1,
		        "$unset" : {
		            "ok" : true
		        },
		        "$set" : {
		            "plus_field" : 2
		        }
		    }
		}
		*/
		oplog.Namespace = fmt.Sprintf("%s.%s", ns["db"], ns["coll"])
		oplog.Operation = "u"
		oplog.Query = event.DocumentKey

		if fulldoc {
			oplog.Object = event.FullDocument
		} else {
			oplog.Object = make(bson.D, 0, 2)
			if updatedFields, ok := event.UpdateDescription["updatedFields"]; ok && len(updatedFields.(bson.M)) > 0 {
				oplog.Object = append(oplog.Object, bson.DocElem{
					Name:  "$set",
					Value: updatedFields,
				})
			}
			if removedFields, ok := event.UpdateDescription["removedFields"]; ok && len(removedFields.([]interface{})) > 0 {
				removedFieldsMap := make(bson.M)
				for _, ele := range removedFields.([]interface{}) {
					removedFieldsMap[ele.(string)] = 1
				}
				oplog.Object = append(oplog.Object, bson.DocElem{
					Name:  "$unset",
					Value: removedFieldsMap,
				})
			}
		}

	case "drop":
		// mgset-xxx:PRIMARY> db.test.drop()
		/* event:
		{
		    "_id" : {
		        "_data" : "825E538DF7000000012B022C0100296E5A1004EE9B60D8845F42FF989D09018A730D6004"
		    },
		    "operationType" : "drop",
		    "clusterTime" : Timestamp(1582534135, 1),
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    }
		}
		*/
		/* oplog:
		{
		    "ts" : Timestamp(1582534135, 1),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "c",
		    "ns" : "zz.$cmd",
		    "ui" : UUID("ee9b60d8-845f-42ff-989d-09018a730d60"),
		    "o2" : {
		        "numRecords" : 3
		    },
		    "wall" : ISODate("2020-02-24T08:48:55.148Z"),
		    "o" : {
		        "drop" : "test"
		    }
		}
		*/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{
			bson.DocElem{
				Name: "drop",
				Value: ns["coll"],
			},
		}
		// ignore o2
	case "rename":
		// mgset-22785363:PRIMARY> db.test.renameCollection("test_collection_rename")
		/* event:
		{
		    "to" : {
		        "db" : "zz",
		        "coll" : "test_collection_rename"
		    },
		    "_id" : {
		        "_data" : "825E53910A000000012B022C0100296E5A1004DA21CB04A4C846AD8B3C3E8E9314F4B504"
		    },
		    "operationType" : "rename",
		    "clusterTime" : Timestamp(1582534922, 1),
		    "ns" : {
		        "db" : "zz",
		        "coll" : "test"
		    }
		}
		*/
		/* oplog:
		{
		    "ts" : Timestamp(1582534922, 1),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "c",
		    "ns" : "zz.$cmd",
		    "ui" : UUID("da21cb04-a4c8-46ad-8b3c-3e8e9314f4b5"),
		    "wall" : ISODate("2020-02-24T09:02:02.685Z"),
		    "o" : {
		        "renameCollection" : "zz.test",
		        "to" : "zz.test_collection_rename",
		        "stayTemp" : false
				"dropTarget": UUID("52c1c147-2408-4d96-9d0f-889a759ab079"), // field only exists when target collection is exists
		    }
		}
		*/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{ // should enable drop_database option on the replayer by default
			bson.DocElem{
				Name: "renameCollection",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
			bson.DocElem{
				Name: "to",
				Value: fmt.Sprintf("%s.%s", event.To["db"], event.To["coll"]),
			},
		}
	case "dropDatabase":
		// mgset-22785363:PRIMARY> db.dropDatabase()
		/* event:
		// before the next event, there'll be several "drop" events about the inner collection remove
		{
		    "_id" : {
		        "_data" : "825E5393A5000000042B022C0100296E04"
		    },
		    "operationType" : "dropDatabase",
		    "clusterTime" : Timestamp(1582535589, 4),
		    "ns" : {
		        "db" : "zz"
		    }
		}
		*/
		/* oplog
		{
		    "ts" : Timestamp(1582535589, 4),
		    "t" : NumberLong(1),
		    "h" : NumberLong(0),
		    "v" : 2,
		    "op" : "c",
		    "ns" : "zz.$cmd",
		    "wall" : ISODate("2020-02-24T09:13:09.165Z"),
		    "o" : {
		        "dropDatabase" : 1
		    }
		}
		*/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.Operation = "c"
		oplog.Object = bson.D{
			bson.DocElem{
				Name: "dropDatabase",
				Value: 1,
			},
		}
	case "invalidate":
		/*
		 * this case shouldn't happen because we watch the whole MongoDB, so we need to panic here
		 * once happen to find the root cause.
		 */
		return nil, fmt.Errorf("invalidate event happen, should be handle manually: %s", event)
	default:
		return nil, fmt.Errorf("unknown event[%v]", event.OperationType)
	}

	return oplog, nil
}
