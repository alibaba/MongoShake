package oplog

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	// fields in oplog
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

// Event example:
/*
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
	"fullDocument" : { <document> }, // 开启full_document之后，为updateLookup，不开启则为default
	"clusterTime" : <Timestamp>, // 相当于ts字段
	"txnNumber" : <NumberLong>, // 相当于oplog里面的txnNumber，只在事务里面出现。txnNumber在一个logical session里面单调递增
	"lsid" : { // 相当于oplog里的lsid字段，只在事务里面出现。logic session id，请求所在的session的id。
   		"id" : <UUID>,
   		"uid" : <BinData>
   	},
	"operationDescription": { // DDL操作(createIndexes/dropIndexes/collMod等)里存储index相关信息
      "index": {
         "name": "age_1",
         "hidden": true
      }
    },
	"stateBeforeChange": { // 仅存在于 modify event中，记录了collMod命令修改前的状态
      "collectionOptions": { // namespace options
          "uuid": UUID("47d6baac-eeaa-488b-98ae-893f3abaaf25")
      },
      "indexOptions": { // index options
         "hidden": false
      }
   },
	"wallTime" : <Date>, // 相当于wall字段
}
*/
type Event struct {
	Id                   bson.M              `bson:"_id" json:"_id"`
	OperationType        string              `bson:"operationType" json:"operationType"`
	FullDocument         bson.D              `bson:"fullDocument,omitempty" json:"fullDocument,omitempty"` // exists on "insert", "replace", "delete", "update"
	Ns                   bson.M              `bson:"ns" json:"ns"`
	To                   bson.M              `bson:"to,omitempty" json:"to,omitempty"`
	DocumentKey          bson.D              `bson:"documentKey,omitempty" json:"documentKey,omitempty"` // exists on "insert", "replace", "delete", "update"
	UpdateDescription    bson.M              `bson:"updateDescription,omitempty" json:"updateDescription,omitempty"`
	ClusterTime          primitive.Timestamp `bson:"clusterTime,omitempty" json:"clusterTime,omitempty"`
	TxnNumber            *int64              `bson:"txnNumber,omitempty" json:"txnNumber,omitempty"`
	LSID                 bson.Raw            `bson:"lsid,omitempty" json:"lsid,omitempty"`
	CollectionUUID       *primitive.Binary   `bson:"UUID,omitempty" json:"UUID,omitempty"`                                 // only exists when the change occurred on a collection, new in 6.0
	OperationDescription bson.D              `bson:"operationDescription,omitempty" json:"operationDescription,omitempty"` // only exists when the change stream uses expanded events, new in 6.0
	WallTime             primitive.DateTime  `bson:"wallTime,omitempty" json:"wallTime,omitempty"`                         // server date and time of the database operation, diffs from clusterTime, new in 6.0
	StateBeforeChange    bson.D              `bson:"stateBeforeChange,omitempty" json:"stateBeforeChange,omitempty"`       // only exists when the change stream uses expanded events('modify' only), new in 6.0
	//NsType               string              `bson:"nsType,omitempty" json:"nsType,omitempty"`                             // shows the namespace type of the collection created by this event, new in 8.0.5
}

func (e *Event) String() string {
	if ret, err := json.Marshal(e); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

func ConvertEvent2Oplog(input []byte, fullDoc bool) (*PartialLog, error) {
	event := new(Event)
	if err := bson.Unmarshal(input, event); err != nil {
		return nil, fmt.Errorf("unmarshal raw bson[%s] failed: %v", input, err)
	}

	oplog := new(PartialLog)
	// ts
	oplog.Timestamp = event.ClusterTime
	// transaction number
	oplog.TxnNumber = event.TxnNumber
	// lsid
	oplog.LSID = event.LSID
	// documentKey
	if len(event.DocumentKey) > 0 {
		oplog.DocumentKey = event.DocumentKey
	}

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
		oplog.Object = event.DocumentKey
	case "replace":
		// PRIMARY> db.test.update({"kick":1}, {"kick":10, "ok":true})
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
		oplog.Object = bson.D{bson.E{Key: "$set", Value: event.FullDocument}}
	case "update":
		/*
		 * PRIMARY> db.test.find()
		 * { "_id" : ObjectId("5e5384f97dc0f30426f01b79"), "kick" : 10, "ok" : true }
		 * PRIMARY> db.test.update({"kick":10}, {$set:{"plus_field":2}, $unset:{"ok":1}})
		 * PRIMARY> db.test.find()
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

		if fullDoc && event.FullDocument != nil && len(event.FullDocument) > 0 {
			oplog.Object = event.FullDocument
		} else {
			oplog.Object = make(bson.D, 0, 2)
			if updatedFields, ok := event.UpdateDescription["updatedFields"]; ok && len(updatedFields.(bson.M)) > 0 {
				oplog.Object = append(oplog.Object, primitive.E{
					Key:   "$set",
					Value: updatedFields,
				})
			}
			if removedFields, ok := event.UpdateDescription["removedFields"]; ok && len(removedFields.(primitive.A)) > 0 {
				removedFieldsMap := make(bson.M)
				for _, ele := range removedFields.(primitive.A) {
					removedFieldsMap[ele.(string)] = 1
				}
				oplog.Object = append(oplog.Object, primitive.E{
					Key:   "$unset",
					Value: removedFieldsMap,
				})
			}
		}

	case "drop":
		// PRIMARY> db.test.drop()
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
			primitive.E{
				Key:   "drop",
				Value: ns["coll"],
			},
		}
		// ignore o2
	case "rename":
		// PRIMARY> db.test.renameCollection("test_collection_rename")
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
			primitive.E{
				Key:   "renameCollection",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
			primitive.E{
				Key:   "to",
				Value: fmt.Sprintf("%s.%s", event.To["db"], event.To["coll"]),
			},
		}
	case "dropDatabase":
		// PRIMARY> db.dropDatabase()
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
			primitive.E{
				Key:   "dropDatabase",
				Value: 1,
			},
		}

		// below events except for 'invalidate' are added since MongoDB6.0
	case "create":
		// PRIMARY> db.createCollection(
		//			    "test_create_collection",
		//				{ changeStreamPreAndPostImages: { enabled: true } }
		//			);
		/* event:
		{
		    "_id": {
		        "_data": "8268381564000000012B042C0100296E5A10047A321D5777D94A898403A1CE31511300463C6F7065726174696F6E54797065003C63726561746500466F7065726174696F6E4465736372697074696F6E0046466368616E676553747265616D507265416E64506F7374496D6167657300466E656E61626C6564006F00466964496E64657800461E76002B04466B657900461E5F6964002B02003C6E616D65003C5F69645F0000000004"
		    },
		    "operationType": "create",
		    "clusterTime": Timestamp(1748505956, 1),
		    "collectionUUID": UUID("7a321d57-77d9-4a89-8403-a1ce31511300"),
		    "wallTime": ISODate("2025-05-29T08:05:56.479Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "test_create_collection"
		    },
		    "operationDescription": {
		        "changeStreamPreAndPostImages": {
		            "enabled": true
		        },
		        "idIndex": {
		            "v": 2,
		            "key": {
		                "_id": 1
		            },
		            "name": "_id_"
		        }
		    }
		}
		*/
		/* oplog:
		{
		    "op": "c",
		    "ns": "zhongli.$cmd",
		    "ui": UUID("7a321d57-77d9-4a89-8403-a1ce31511300"),
		    "o": {
		        "create": "test_create_collection",
		        "changeStreamPreAndPostImages": {
		            "enabled": true
		        },
		        "idIndex": {
		            "v": 2,
		            "key": {
		                "_id": 1
		            },
		            "name": "_id_"
		        }
		    },
		    "ts": Timestamp(1748505956, 1),
		    "t": NumberLong(64),
		    "v": NumberLong(2),
		    "wall": ISODate("2025-05-29T08:05:56.479Z"),
		}
		*/
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/create/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "create",
				Value: ns["coll"],
			},
		}
		for _, ele := range event.OperationDescription {
			if ele.Key == "idIndex" {
				continue
			} else {
				oplog.Object = append(oplog.Object, ele)
			}
		}
	case "createIndexes":
		// PRIMARY> db.test.createIndexes([{a:1,b:1}])
		/* event:
		{
		    "_id": {
		        "_data": "82687A09F4000000062B042C0100296E5A100435E288933C8245C6B0E9DE829C0E5056463C6F7065726174696F6E54797065003C637265617465496E646578657300466F7065726174696F6E4465736372697074696F6E004650696E64657865730050461E76002B04466B657900461E61002B02003C6E616D65003C615F310000461E76002B04466B657900461E62002B02003C6E616D65003C625F31000000000004",
		        "_typeBits": BinData(0, "gwBCAA==")
		    },
		    "operationType": "createIndexes",
		    "clusterTime": Timestamp(1752828404, 6),
		    "collectionUUID": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "wallTime": ISODate("2025-07-18T08:46:44.506Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "test"
		    },
		    "operationDescription": {
		        "indexes": [
		            {
		                "v": 2,
		                "key": {
		                    "a": 1
		                },
		                "name": "a_1"
		            },
		            {
		                "v": 2,
		                "key": {
		                    "b": 1
		                },
		                "name": "b_1"
		            }
		        ]
		    }
		}
		*/
		/* oplog:
		{
		    "op": "c",
		    "ns": "zhongli.$cmd",
		    "ui": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "o": {
		        "startIndexBuild": "test",   // 2PC, also have a 'commitIndexBuild' oplog
		        "indexBuildUUID": UUID("9a3b9266-1db3-441d-a6ae-49834897d26d"),
		        "indexes": [
		            {
		                "v": 2,
		                "key": {
		                    "a": 1
		                },
		                "name": "a_1"
		            },
		            {
		                "v": 2,
		                "key": {
		                    "b": 1
		                },
		                "name": "b_1"
		            }
		        ]
		    },
		    "ts": Timestamp(1752828404, 2),
		    "t": NumberLong(101),
		    "v": NumberLong(2),
		    "wall": ISODate("2025-07-18T08:46:44.225Z"),
		}
		*/
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/createIndexes/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "createIndexes",
				Value: ns["coll"],
			},
		}
		opDesc, _ := ConvertBsonD2M(event.OperationDescription)
		var idxElems bson.A
		if indexes, exists := opDesc["indexes"]; exists {
			if idx, ok := indexes.(bson.A); ok {
				idxElems = idx
			} else {
				return nil, fmt.Errorf("unexpected 'operationDescription.indexes', event.opDes: %v",
					event.OperationDescription)
			}
		} else {
			return nil, fmt.Errorf("no 'operationDescription.indexes' found in event:%v", event)
		}
		oplog.Object = append(oplog.Object, primitive.E{Key: "indexes", Value: idxElems})
	case "dropIndexes":
		// PRIMARY> db.test.dropIndex("a_1") // `dropIndexes(["a_1","b_1"])` will generate 2 oplogs & events
		/* event:
		{
		    "_id": {
		        "_data": "82687A101D000000022B042C0100296E5A100435E288933C8245C6B0E9DE829C0E5056463C6F7065726174696F6E54797065003C64726F70496E646578657300466F7065726174696F6E4465736372697074696F6E004650696E64657865730050461E76002B04466B657900461E61002B02003C6E616D65003C615F31000000000004",
		        "_typeBits": BinData(0, "ggAC")
		    },
		    "operationType": "dropIndexes",
		    "clusterTime": Timestamp(1752829981, 2),
		    "collectionUUID": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "wallTime": ISODate("2025-07-18T09:13:01.975Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "test"
		    },
		    "operationDescription": {
		        "indexes": [
		            {
		                "v": 2,
		                "key": {
		                    "a": 1
		                },
		                "name": "a_1"
		            }
		        ]
		    }
		}
		*/
		/* oplog:
		{
		    "op": "c",
		    "ns": "zhongli.$cmd",
		    "ui": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "o": {
		        "dropIndexes": "test",
		        "index": "a_1"
		    },
		    "o2": {
		        "v": 2,
		        "key": {
		            "a": 1
		        },
		        "name": "a_1"
		    },
		    "ts": Timestamp(1752830096, 1),
		    "t": NumberLong(101),
		    "v": NumberLong(2),
		    "wall": ISODate("2025-07-18T09:14:56.302Z"),
		}
		*/
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/dropIndexes/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		opDesc, _ := ConvertBsonD2M(event.OperationDescription)
		var idxElems bson.A
		if indexes, exists := opDesc["indexes"]; exists {
			if idx, ok := indexes.(bson.A); ok {
				idxElems = idx
			} else {
				return nil, fmt.Errorf("unexpected 'operationDescription.indexes', event.opDes: %v",
					event.OperationDescription)
			}
		} else {
			return nil, fmt.Errorf("no 'operationDescription.indexes' found in event:%v", event)
		}
		idxElem, _ := idxElems[0].(bson.D)
		idxName := GetKey(idxElem, "name")
		oplog.Object = bson.D{
			primitive.E{
				Key:   "dropIndexes",
				Value: ns["coll"],
			},
			primitive.E{
				Key:   "index",
				Value: idxName,
			},
		}
		oplog.Query = idxElem
	case "modify":
		// PRIMARY> db.runCommand( {collMod: "test",index: { name: "a_1", hidden:true}})
		/* event:
		{
		    "_id": {
		        "_data": "82687A1A60000000012B042C0100296E5A100435E288933C8245C6B0E9DE829C0E5056463C6F7065726174696F6E54797065003C6D6F6469667900466F7065726174696F6E4465736372697074696F6E004646696E64657800463C6E616D65003C615F31006E68696464656E006F00000004"
		    },
		    "operationType": "modify",
		    "clusterTime": Timestamp(1752832608, 1),
		    "collectionUUID": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "wallTime": ISODate("2025-07-18T09:56:48.368Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "test"
		    },
		    "operationDescription": {
		        "index": {
		            "name": "a_1",
		            "hidden": true
		        }
		    },
		    "stateBeforeChange": {
		        "collectionOptions": {
		            "uuid": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056")
		        },
		        "indexOptions": {
		            "hidden": false
		        }
		    }
		}
		*/
		/* oplog:
		{
		    "op": "c",
		    "ns": "zhongli.$cmd",
		    "ui": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056"),
		    "o": {
		        "collMod": "test",
		        "index": {
		            "name": "a_1",
		            "hidden": true
		        }
		    },
		    "o2": {
		        "collectionOptions_old": {
		            "uuid": UUID("35e28893-3c82-45c6-b0e9-de829c0e5056")
		        },
		        "indexOptions_old": {
		            "hidden": false
		        }
		    },
		    "ts": Timestamp(1752832608, 1),
		    "t": NumberLong(105),
		    "v": NumberLong(2),
		    "wall": ISODate("2025-07-18T09:56:48.368Z"),
		}
		*/
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/modify/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "collMod",
				Value: ns["coll"],
			},
		}
		idxElem := GetKey(event.OperationDescription, "index")
		// o2 is unnecessary
		oplog.Object = append(oplog.Object, primitive.E{Key: "index", Value: idxElem})
	case "shardCollection":
		// PRIMARY> sh.shardCollection("zhongli.shard_coll", {"_id":"hashed"})
		/* event:
		{
		    "_id": {
		        "_data": "82687A6EC9000000232B042C0100296E5A100427FA47D5CB1849FCAA4446F4EF84EA28463C6F7065726174696F6E54797065003C7368617264436F6C6C656374696F6E00466F7065726174696F6E4465736372697074696F6E00464673686172644B657900463C5F6964003C68617368656400006E756E69717565006E1E6E756D496E697469616C4368756E6B7300296E70726573706C69744861736865645A6F6E6573006E000004",
		        "_typeBits": BinData(0, "ggAC")
		    },
		    "operationType": "shardCollection",
		    "clusterTime": Timestamp(1752854217, 35),
		    "collectionUUID": UUID("27fa47d5-cb18-49fc-aa44-46f4ef84ea28"),
		    "wallTime": ISODate("2025-07-18T15:56:57.510Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "shard_coll"
		    },
		    "operationDescription": {
		        "shardKey": {
		            "_id": "hashed"
		        },
		        "unique": false,
		        "numInitialChunks": NumberLong(0),
		        "presplitHashedZones": false
		    }
		}
		*/
		/* oplog: no oplog matched, generate a fake one, will handle it later in BasicWriter
		 */
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/shardCollection/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "shardCollection",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
		}
		for _, ele := range event.OperationDescription {
			if ele.Key == "shardKey" {
				oplog.Object = append(oplog.Object, bson.E{Key: "key", Value: ele.Value})
			} else {
				oplog.Object = append(oplog.Object, ele)
			}
		}
	case "reshardCollection": // 6.0.14+
		// PRIMARY> db.adminCommand({reshardCollection: "zhongli.shard_coll", key: { "_id": 1 }})
		/* event:
		{
			"_id" : {
				"_data" : "82687DD562000000852B042C0100296E5A100427FA47D5CB1849FCAA4446F4EF84EA28463C6F7065726174696F6E54797065003C72657368617264436F6C6C656374696F6E00466F7065726174696F6E4465736372697074696F6E00465A7265736861726455554944005A100487B05E8160CF453389CCB05FC2E28BBE4673686172644B657900461E5F6964002B0200466F6C6453686172644B657900463C5F6964003C68617368656400006E756E69717565006E000004",
				"_typeBits" : BinData(0, "goAA")
			},
			"operationType" : "reshardCollection",
			"clusterTime" : Timestamp(1753077090, 133),
			"collectionUUID" : UUID("27fa47d5-cb18-49fc-aa44-46f4ef84ea28"),
			"wallTime" : ISODate("2025-07-21T05:51:30.465Z"),
			"ns" : {
				"db" : "zhongli",
				"coll" : "shard_coll"
			},
			"operationDescription" : {
				"reshardUUID" : UUID("87b05e81-60cf-4533-89cc-b05fc2e28bbe"),
				"shardKey" : {
					"_id" : 1
				},
				"oldShardKey" : {
					"_id" : "hashed"
				},
				"unique" : false
			}
		}
		*/
		/* oplog: no oplog matched, generate a fake one, will handle it later in BasicWriter
		 */
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/reshardCollection/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "reshardCollection",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
		}
		for _, ele := range event.OperationDescription {
			// skip UUID & oldShardKey, unnecessary for reshardCollection command
			if ele.Key == "reshardUUID" || ele.Key == "oldShardKey" {
				continue
			} else if ele.Key == "shardKey" {
				oplog.Object = append(oplog.Object, bson.E{Key: "key", Value: ele.Value})
			} else {
				oplog.Object = append(oplog.Object, ele)
			}
		}
	case "refineCollectionShardKey": // 7.0+
		// PRIMARY> db.adminCommand( {refineCollectionShardKey: "zhongli.shard_coll",key: { _id: 1, id: 1 }} )
		/* event:
		{
		    "_id": {
		        "_data": "82687DE64F000000672B042C0100296E5A100487B05E8160CF453389CCB05FC2E28BBE463C6F7065726174696F6E54797065003C726566696E65436F6C6C656374696F6E53686172644B657900466F7065726174696F6E4465736372697074696F6E00464673686172644B657900461E5F6964002B021E6964002B0200466F6C6453686172644B657900461E5F6964002B0200000004",
		        "_typeBits": BinData(0, "goAK")
		    },
		    "operationType": "refineCollectionShardKey",
		    "clusterTime": Timestamp(1753081423, 103),
		    "collectionUUID": UUID("87b05e81-60cf-4533-89cc-b05fc2e28bbe"),
		    "wallTime": ISODate("2025-07-21T07:03:43.453Z"),
		    "ns": {
		        "db": "zhongli",
		        "coll": "shard_coll"
		    },
		    "operationDescription": {
		        "shardKey": {
		            "_id": 1,
		            "id": 1
		        },
		        "oldShardKey": {
		            "_id": 1
		        }
		    }
		}
		*/
		/* oplog: no oplog matched, generate a fake one, will handle it later in BasicWriter
		 */
		// see more details at https://www.mongodb.com/docs/manual/reference/change-events/refineCollectionShardKey/
		oplog.Namespace = fmt.Sprintf("%s.$cmd", ns["db"])
		oplog.UI = event.CollectionUUID
		oplog.Operation = "c"
		oplog.Object = bson.D{
			primitive.E{
				Key:   "refineCollectionShardKey",
				Value: fmt.Sprintf("%s.%s", ns["db"], ns["coll"]),
			},
		}
		newShardKey := GetKey(event.OperationDescription, "shardKey")
		// 'oldShardKey' is not necessary
		oplog.Object = append(oplog.Object, primitive.E{Key: "key", Value: newShardKey})
	case "invalidate":
		/*
		 * this case shouldn't happen because we watch the whole MongoDB, so we need to panic here
		 * once happen to find the root cause.
		 */
		return nil, fmt.Errorf("invalidate event happen, should be handle manually: %s", event)
	default:
		return nil, fmt.Errorf("unknown event type[%v] org_event[%v]", event.OperationType, event)
	}

	// set default for "o", "o2"
	if oplog.Object == nil {
		oplog.Object = bson.D{}
	}
	if oplog.Query == nil {
		oplog.Query = bson.D{}
	}

	l.Logger.Debugf("ConvertEvent2Oplog Event[%v] to Oplog[%v]", event, oplog)
	return oplog, nil
}
