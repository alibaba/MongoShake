package executor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/mongo-tools-common/json"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

var (
	testMongoAddress         = unit_test_common.TestUrl
	testMongoShardingAddress = unit_test_common.TestUrlSharding
)

const (
	testDb         = "writer_test"
	testCollection = "a"
)

func mockDeleteOplogRecord(oId interface{}) *OplogRecord {
	or := &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Object: bson.D{
						primitive.E{
							Key:   "_id",
							Value: oId,
						},
					},
				},
			},
		},
	}
	return or
}
func mockOplogRecord(oId, oX interface{}, o2Id int) *OplogRecord {
	or := &OplogRecord{
		original: &PartialLogWithCallback{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Object: bson.D{
						primitive.E{
							Key:   "_id",
							Value: oId,
						},
						primitive.E{
							Key:   "x",
							Value: oX,
						},
					},
				},
			},
		},
	}

	if o2Id != -1 {
		or.original.partialLog.ParsedLog.Query = bson.D{
			{"_id", o2Id},
		}
	}

	return or
}

func objectIdFromInt(num int64) primitive.ObjectID {
	objectId, err := primitive.ObjectIDFromHex(fmt.Sprintf("%024s", strconv.FormatInt(num, 10)))
	if err != nil {
		return primitive.ObjectID{}
	}

	return objectId
}

func TestSingleWriter(t *testing.T) {
	// test single writer

	_ = utils.InitialLogger("", "", "debug", true, 1)

	var nr int

	// simple test
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 0)

		inserts := []*OplogRecord{mockOplogRecord(1, 1, -1)}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update 1->2
		err = writer.doUpdate(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockOplogRecord(1, 10, 1),
		}, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, nil)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, len(result), "should be equal")
		assert.Equal(t, int32(10), result[0]["x"], "should be equal")
		assert.Equal(t, int32(1), result[0]["_id"], "should be equal")

		// delete 2
		err = writer.doDelete(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockDeleteOplogRecord(1),
		})
		assert.Equal(t, nil, err, "should be equal")

		// query
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, nil)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(result), "should be equal")
	}

	// simple upsert
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// write 1
		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// upsert write 2(update do not exit, then insert)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 2 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(result), "should be equal")
		assert.Equal(t, int32(10000), result[0]["x"], "should be equal")
	}

	// upsert with duplicate key error
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 1)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// build index on filed 'x'
		indexOptions := options.Index().SetUnique(true)
		_, err = conn.Client.Database(testDb).Collection(testCollection).Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, nil, err, "should be equal")

		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
			mockOplogRecord(objectIdFromInt(123456789012), 10, -1),
		}
		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 10, -1),
		}
		// write 1
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(result), "should be equal")
		if result[0]["_id"] == objectIdFromInt(123456789011) {
			assert.Equal(t, true, result[0]["x"] == int32(1), "should be equal")
		}
		if result[0]["_id"] == objectIdFromInt(123456789012) {
			assert.Equal(t, true, result[0]["x"] == int32(10), "should be equal")
		}
	}

	// test upsert, dupInsert
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update 6->10
		err = writer.doUpdate(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockOplogRecord(6, 10, 6),
		}, false)
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Printf("err:%v\n", err)

		// upsert 6->10
		err = writer.doUpdate(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockOplogRecord(6, 10, 6),
		}, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6, len(result), "should be equal")
		assert.Equal(t, int32(1), result[0]["x"], "should be equal")
		assert.Equal(t, int32(2), result[1]["x"], "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")
		assert.Equal(t, int32(4), result[3]["x"], "should be equal")
		assert.Equal(t, int32(5), result[4]["x"], "should be equal")
		assert.Equal(t, int32(10), result[5]["x"], "should be equal")
		assert.Equal(t, int32(6), result[5]["_id"], "should be equal")

		// dupInsert but ignore
		err = writer.doInsert(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockOplogRecord(1, 30, 1),
		}, false)
		assert.Equal(t, nil, err, "should be equal")

		// dupInsert -> update
		err = writer.doInsert(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockOplogRecord(1, 30, 1),
		}, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6, len(result), "should be equal")
		assert.Equal(t, int32(30), result[0]["x"], "should be equal")

		// delete not found
		err = writer.doDelete(testDb, testCollection, bson.E{}, []*OplogRecord{
			mockDeleteOplogRecord(20),
		})
		assert.Equal(t, nil, err, "should be equal")
	}

	// test ignore error
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Object: bson.D{
								primitive.E{
									Key:   "_id",
									Value: 110011,
								},
								primitive.E{
									Key:   "x",
									Value: nil,
								},
							},
						},
					},
				},
			},
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Timestamp: primitive.Timestamp{T: 0, I: 1},
							Object: bson.D{
								primitive.E{
									Key:   "$v",
									Value: 1,
								},
								primitive.E{
									Key: "$set",
									Value: bson.M{
										"x.0.y": 123,
									},
								},
							},
							Query: bson.D{{"_id", 110011}},
						},
					},
				},
			},
			mockOplogRecord(2, 20, 2),
			mockOplogRecord(3, 30, 3),
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")
		assert.Equal(t, int32(10), result[0]["x"], "should be equal")
		assert.Equal(t, int32(20), result[1]["x"], "should be equal")
		assert.Equal(t, int32(30), result[2]["x"], "should be equal")
	}

	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conf.Options.IncrSyncExecutorUpsert = true

		conn, err := utils.NewMongoCommunityConn(testMongoShardingAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, false, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)

		// enable sharding
		result := conn.Client.Database("admin").RunCommand(context.Background(), bson.D{{"enablesharding", testDb}})
		assert.Equal(t, nil, result.Err(), "should be equal")

		// shard collection
		ns := fmt.Sprintf("%s.%s", testDb, testCollection)
		result = conn.Client.Database("admin").RunCommand(context.Background(), bson.D{
			{"shardCollection", ns},
			{"key", bson.M{"x": 1}},
			{"unique", true},
		}, nil)
		assert.Equal(t, nil, result.Err(), "should be equal")

		// 1-2(shardkey is x, so Query(upsert) only have _id field will failed, must have shardkey)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, 1),
			mockOplogRecord(2, 2, 1),
		}

		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Equal(t, true, strings.Contains(err.Error(), "Failed to target upsert by query"), "should be equal")
		fmt.Println(err)

		inserts[0].original.partialLog.DocumentKey = bson.D{
			{"_id", 1},
			{"x", 1},
		}
		inserts[1].original.partialLog.DocumentKey = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		res, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(2), res[1]["x"], "should be equal")

		fmt.Println("---------------")
		// 2-3
		inserts2 := []*OplogRecord{
			mockOplogRecord(2, 20, -1),
			mockOplogRecord(3, 3, -1),
		}
		inserts2[0].original.partialLog.DocumentKey = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		inserts2[1].original.partialLog.DocumentKey = bson.D{
			{"_id", 3},
			{"x", 3},
		}

		// see https://github.com/alibaba/MongoShake/issues/380 (go-driver is in session(transaction) by default)
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts2, true)
		assert.Equal(t, nil, err, "should be equal")
		// assert.Equal(t, true, strings.Contains(err.Error(), "Must run update to shard key"), "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		res, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(20), res[1]["x"], "should be equal")
		assert.Equal(t, int32(3), res[2]["x"], "should be equal")
	}
}

func TestBulkWriter(t *testing.T) {
	// test bulk writer

	_ = utils.InitialLogger("", "", "debug", true, 1)

	var nr int

	// basic test
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// 4-8
		inserts = []*OplogRecord{
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
			mockOplogRecord(6, 6, -1),
			mockOplogRecord(7, 7, -1),
			mockOplogRecord(8, 8, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 8, len(result), "should be equal")
		assert.Equal(t, int32(1), result[0]["x"], "should be equal")
		assert.Equal(t, int32(2), result[1]["x"], "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")
		assert.Equal(t, int32(4), result[3]["x"], "should be equal")
		assert.Equal(t, int32(5), result[4]["x"], "should be equal")
		assert.Equal(t, int32(6), result[5]["x"], "should be equal")
		assert.Equal(t, int32(7), result[6]["x"], "should be equal")
		assert.Equal(t, int32(8), result[7]["x"], "should be equal")

		// 8-10
		inserts = []*OplogRecord{
			mockOplogRecord(8, 80, -1),
			mockOplogRecord(9, 90, -1),
			mockOplogRecord(10, 100, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 10, len(result), "should be equal")
		assert.Equal(t, int32(80), result[7]["x"], "should be equal")
		assert.Equal(t, int32(90), result[8]["x"], "should be equal")
		assert.Equal(t, int32(100), result[9]["x"], "should be equal")

		// delete 8-11
		deletes := []*OplogRecord{
			mockDeleteOplogRecord(8),
			mockDeleteOplogRecord(9),
			mockDeleteOplogRecord(10),
			mockDeleteOplogRecord(11), // not found
		}
		err = writer.doDelete(testDb, testCollection, bson.E{}, deletes)
		assert.Equal(t, nil, err, "should be equal") // won't throw error if not found

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
	}

	// simple upsert
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// write 1
		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10001), -1),
		}
		inserts[0].original.partialLog.DocumentKey = bson.D{
			{"_id", objectIdFromInt(123456789011)},
			{"x", int32(10000)},
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// upsert write 2(update do not exit, then insert)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 2 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// upsert + update
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20001), -1),
			mockOplogRecord(objectIdFromInt(123456789013), int32(30000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result), "should be equal")
		assert.Equal(t, int32(10001), result[0]["x"], "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update not exist
		updates := []*OplogRecord{
			mockOplogRecord(5, 50, 5),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// not work
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, int32(50), result[4]["x"], "should be equal")

		// updates
		updates = []*OplogRecord{
			mockOplogRecord(4, 40, 4),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, int32(40), result[3]["x"], "should be equal")
		assert.Equal(t, int32(50), result[4]["x"], "should be equal")
		assert.Equal(t, int32(100), result[5]["x"], "should be equal")
		assert.Equal(t, int32(110), result[6]["x"], "should be equal")

		// deletes
		deletes := []*OplogRecord{
			mockDeleteOplogRecord(1),
			mockDeleteOplogRecord(2),
			mockDeleteOplogRecord(999), // not exist
		}

		err = writer.doDelete(testDb, testCollection, bson.E{}, deletes)
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// build index
		indexOptions := options.Index().SetUnique(true)
		_, err = conn.Client.Database(testDb).Collection(testCollection).Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, nil, err, "should be equal")

		// updates
		updates := []*OplogRecord{
			mockOplogRecord(3, 5, 3), // dup
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert = false
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, false)
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Println(err)

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")
		assert.Equal(t, int32(100), result[5]["x"], "should be equal")
		assert.Equal(t, int32(110), result[6]["x"], "should be equal")
	}

	// test ignore error
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		_ = utils.InitialLogger("", "", "info", true, 1)

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Object: bson.D{
								primitive.E{
									Key:   "_id",
									Value: 110011,
								},
								primitive.E{
									Key:   "x",
									Value: nil,
								},
							},
						},
					},
				},
			},
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Timestamp: primitive.Timestamp{T: 0, I: 1},
							Object: bson.D{
								primitive.E{
									Key: "$set",
									Value: bson.M{
										"x.0.y": 123,
									},
								},
							},
							Query: bson.D{
								{"_id", 110011},
							},
						},
					},
				},
			},
			mockOplogRecord(2, 20, 2),
			mockOplogRecord(3, 30, 3),
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")
		assert.Equal(t, int32(10), result[0]["x"], "should be equal")
		assert.Equal(t, int32(20), result[1]["x"], "should be equal")
		assert.Equal(t, int32(30), result[2]["x"], "should be equal")
	}

	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conf.Options.IncrSyncExecutorUpsert = true

		conn, err := utils.NewMongoCommunityConn(testMongoShardingAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{}, true, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)

		// enable sharding
		result := conn.Client.Database("admin").RunCommand(context.Background(),
			bson.D{{"enablesharding", testDb}})
		assert.Equal(t, nil, result.Err(), "should be equal")

		// shard collection
		ns := fmt.Sprintf("%s.%s", testDb, testCollection)
		result = conn.Client.Database("admin").RunCommand(context.Background(), bson.D{
			{"shardCollection", ns},
			{"key", bson.M{"x": 1}},
			{"unique", true},
		}, nil)
		assert.Equal(t, nil, result.Err(), "should be equal")

		// 1-2
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, 1),
			mockOplogRecord(2, 2, 2),
		}

		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Equal(t, true, strings.Contains(err.Error(),
			"Failed to target upsert by query"), "should be equal")
		fmt.Println(err)

		inserts[0].original.partialLog.DocumentKey = bson.D{
			{"_id", 1},
			{"x", 1},
		}
		inserts[1].original.partialLog.DocumentKey = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		res, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(2), res[1]["x"], "should be equal")

		fmt.Println("---------------")
		// 2-3
		inserts2 := []*OplogRecord{
			mockOplogRecord(2, 20, -1),
			mockOplogRecord(3, 3, -1),
		}
		inserts2[0].original.partialLog.DocumentKey = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		inserts2[1].original.partialLog.DocumentKey = bson.D{
			{"_id", 3},
			{"x", 3},
		}

		// see https://github.com/alibaba/MongoShake/issues/380(go-driver is in session(transaction) by default)
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts2, true)
		fmt.Printf("err:%v\n", err)
		assert.Equal(t, nil, err, "should be equal")
		// assert.Equal(t, true, strings.Contains(err.Error(), "Must run update to shard key"), "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		res, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(20), res[1]["x"], "should be equal")
		assert.Equal(t, int32(3), res[2]["x"], "should be equal")
	}
}

func TestCommandWriter(t *testing.T) {

	_ = utils.InitialLogger("", "", "debug", true, 1)

	var nr int

	// basic test
	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// 4-8
		inserts = []*OplogRecord{
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
			mockOplogRecord(6, 6, -1),
			mockOplogRecord(7, 7, -1),
			mockOplogRecord(8, 8, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 8, len(result), "should be equal")
		assert.Equal(t, int32(1), result[0]["x"], "should be equal")
		assert.Equal(t, int32(2), result[1]["x"], "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")
		assert.Equal(t, int32(4), result[3]["x"], "should be equal")
		assert.Equal(t, int32(5), result[4]["x"], "should be equal")
		assert.Equal(t, int32(6), result[5]["x"], "should be equal")
		assert.Equal(t, int32(7), result[6]["x"], "should be equal")
		assert.Equal(t, int32(8), result[7]["x"], "should be equal")

		// 8-10
		inserts = []*OplogRecord{
			mockOplogRecord(8, 80, -1),
			mockOplogRecord(9, 90, -1),
			mockOplogRecord(10, 100, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 10, len(result), "should be equal")
		assert.Equal(t, int32(80), result[7]["x"], "should be equal")
		assert.Equal(t, int32(90), result[8]["x"], "should be equal")
		assert.Equal(t, int32(100), result[9]["x"], "should be equal")

		// delete 8-11
		deletes := []*OplogRecord{
			mockDeleteOplogRecord(8),
			mockDeleteOplogRecord(9),
			mockDeleteOplogRecord(10),
			mockDeleteOplogRecord(11), // not found
		}
		err = writer.doDelete(testDb, testCollection, bson.E{}, deletes)
		assert.Equal(t, nil, err, "should be equal") // won't throw error if not found

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
	}

	// simple upsert
	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// write 1
		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10001), -1),
		}
		inserts[0].original.partialLog.DocumentKey = bson.D{
			{"_id", objectIdFromInt(123456789011)},
			{"x", int32(10000)},
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// upsert write 2(update do not exit, then insert)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 2 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// upsert + update
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20001), -1),
			mockOplogRecord(objectIdFromInt(123456789013), int32(30000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result), "should be equal")
		assert.Equal(t, int32(10001), result[0]["x"], "should be equal")
		assert.Equal(t, int32(20001), result[1]["x"], "should be equal")
		assert.Equal(t, int32(30000), result[2]["x"], "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update not exist
		updates := []*OplogRecord{
			mockOplogRecord(5, 50, 5),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// not work
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, int32(50), result[4]["x"], "should be equal")

		// updates
		updates = []*OplogRecord{
			mockOplogRecord(4, 40, 4),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, int32(40), result[3]["x"], "should be equal")
		assert.Equal(t, int32(50), result[4]["x"], "should be equal")
		assert.Equal(t, int32(100), result[5]["x"], "should be equal")
		assert.Equal(t, int32(110), result[6]["x"], "should be equal")

		// deletes
		deletes := []*OplogRecord{
			mockDeleteOplogRecord(1),
			mockDeleteOplogRecord(2),
			mockDeleteOplogRecord(999), // not exist
		}

		err = writer.doDelete(testDb, testCollection, bson.E{}, deletes)
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// build index
		indexOptions := options.Index().SetUnique(true)
		_, err = conn.Client.Database(testDb).Collection(testCollection).Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, nil, err, "should be equal")

		// updates
		updates := []*OplogRecord{
			mockOplogRecord(3, 5, 3), // dup
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert = false(doUpdate will ignore dup error)
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, false)
		assert.Equal(t, nil, err, "should be equal")
		fmt.Println(err)

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")
		assert.Equal(t, int32(100), result[5]["x"], "should be equal")
		assert.Equal(t, int32(110), result[6]["x"], "should be equal")
	}

	// test ignore error
	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		_ = utils.InitialLogger("", "", "info", true, 1)

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Object: bson.D{
								primitive.E{
									Key:   "_id",
									Value: 110011,
								},
								primitive.E{
									Key:   "x",
									Value: nil,
								},
							},
						},
					},
				},
			},
		}
		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Timestamp: primitive.Timestamp{T: 0, I: 1},
							Object: bson.D{
								primitive.E{
									Key:   "x.0.y",
									Value: 123,
								},
							},
							Query: bson.D{
								{"_id", 110011},
							},
						},
					},
				},
			},
			mockOplogRecord(2, 20, 2),
			mockOplogRecord(3, 30, 3),
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		result, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")
		assert.Equal(t, int32(10), result[0]["x"], "should be equal")
		assert.Equal(t, int32(20), result[1]["x"], "should be equal")
		assert.Equal(t, int32(30), result[2]["x"], "should be equal")
	}

	{
		fmt.Printf("TestCommandWriter case %d.\n", nr)
		nr++

		conf.Options.IncrSyncExecutorUpsert = true

		conn, err := utils.NewMongoCommunityConn(testMongoShardingAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)

		// enable sharding
		result := conn.Client.Database("admin").RunCommand(context.Background(),
			bson.D{{"enablesharding", testDb}})
		assert.Equal(t, nil, result.Err(), "should be equal")

		// shard collection
		ns := fmt.Sprintf("%s.%s", testDb, testCollection)
		result = conn.Client.Database("admin").RunCommand(context.Background(), bson.D{
			{"shardCollection", ns},
			{"key", bson.M{"x": 1}},
			{"unique", true},
		}, nil)
		assert.Equal(t, nil, result.Err(), "should be equal")

		// 1-2
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, 1),
			mockOplogRecord(2, 2, 2),
		}

		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Equal(t, true, strings.Contains(err.Error(),
			"Failed to target upsert by query"), "should be equal")
		fmt.Println(err)

		// filter have _id & shardKey will update successfully
		inserts[0].original.partialLog.Query = bson.D{
			{"_id", 1},
			{"x", 1},
		}
		inserts[1].original.partialLog.Query = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		err = writer.doUpdate(testDb, testCollection, bson.E{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		res, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(2), res[1]["x"], "should be equal")

		fmt.Println("---------------")
		// 2-3
		inserts2 := []*OplogRecord{
			mockOplogRecord(2, 20, -1),
			mockOplogRecord(3, 3, -1),
		}
		inserts2[0].original.partialLog.DocumentKey = bson.D{
			{"_id", 2},
			{"x", 2},
		}
		inserts2[1].original.partialLog.DocumentKey = bson.D{
			{"_id", 3},
			{"x", 3},
		}

		err = writer.doInsert(testDb, testCollection, bson.E{}, inserts2, true)
		fmt.Printf("err:%v\n", err)
		// assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		res, err = unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		// assert.Equal(t, int32(20), res[1]["x"], "should be equal")
		assert.Equal(t, int32(3), res[2]["x"], "should be equal")
	}
}

func TestRunCommand(t *testing.T) {
	// test RunCommand

	_ = utils.InitialLogger("", "", "debug", true, 1)
	txnN := []int64{0, 1}
	term := []int64{1}
	multiOpType := []int{0, 1}

	var nr int

	// applyOps
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database("zz").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.M{
							{
								"ns": "zz.y",
								"op": "i",
								"ui": "xxxx",
								"o": bson.M{
									"_id":   "567",
									"hello": "world",
								},
							},
							{
								"ns": "zz.y",
								"op": "i",
								"ui": "xxxx2",
								"o": bson.D{
									bson.E{
										Key:   "_id",
										Value: "789",
									},
									bson.E{
										Key:   "hello",
										Value: "w2",
									},
								},
							},
						},
					},
				},
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, "zz", "y", opts)
		assert.Equal(t, nil, err, "should be equal")
		fmt.Printf("result:%v\n", result)
		assert.Equal(t, "567", result[0]["_id"].(string), "should be equal")
		assert.Equal(t, "world", result[0]["hello"].(string), "should be equal")
		assert.Equal(t, "789", result[1]["_id"].(string), "should be equal")
		assert.Equal(t, "w2", result[1]["hello"].(string), "should be equal")
		assert.Equal(t, int32(1), result[2]["x"], "should be equal")
	}

	// applyOps with []bson.D
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Client.Database("zz").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.D{
							{
								{Key: "ns", Value: "zz.y"},
								{Key: "op", Value: "i"},
								{Key: "ui", Value: "xxxx"},
								{Key: "o", Value: bson.D{
									{Key: "_id", Value: "bson-d-1"},
									{Key: "hello", Value: "world"},
								}},
							},
						},
					},
				},
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, "zz", "y", opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "bson-d-1", result[0]["_id"].(string), "should be equal")
		assert.Equal(t, "world", result[0]["hello"].(string), "should be equal")
	}

	// applyOps with bson.A
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Client.Database("zz").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								{Key: "ns", Value: "zz.y"},
								{Key: "op", Value: "i"},
								{Key: "ui", Value: "xxxx"},
								{Key: "o", Value: bson.D{
									{Key: "_id", Value: "bson-a-1"},
									{Key: "hello", Value: "world-a"},
								}},
							},
						},
					},
				},
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, "zz", "y", opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "bson-a-1", result[0]["_id"].(string), "should be equal")
		assert.Equal(t, "world-a", result[0]["hello"].(string), "should be equal")
	}

	// applyOps with illegal type
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "applyOps",
						Value: 1,
					},
				},
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.EqualError(t, err, "applyOps field has unsupported type int")
	}

	// applyOps with {multiOpType:1} which should not be treated as txn.
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++
		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")
		// drop database
		err = conn.Client.Database("zz").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")
		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				TxnNumber: &txnN[0],
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.M{
							{
								"ns": "zz.y",
								"op": "i",
								"ui": primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								},
								"o": bson.M{"_id": int64(4), "x": json.NumberLong(-20), "y": json.NumberLong(5)},
							},
							{
								"ns": "zz.y",
								"op": "i",
								"ui": primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								},
								"o": bson.D{
									bson.E{Key: "_id", Value: int64(5)},
									bson.E{Key: "x", Value: json.NumberLong(-30)},
									bson.E{Key: "y", Value: json.NumberLong(11)},
								},
							},
						},
					},
				},
				Timestamp:   utils.TimeToTimestamp(time.Now().Unix()),
				Term:        &term[0],
				Version:     2,
				PrevOpTime:  utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
				MultiOpType: &multiOpType[1], // 1 for vectored insert oplog format
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, "zz", "y", opts)
		assert.Equal(t, nil, err, "should be equal")
		fmt.Printf("result:%v\n", result)
		assert.Equal(t, int64(4), result[0]["_id"].(int64), "should be equal")
		assert.Equal(t, int64(-20), result[0]["x"].(int64), "should be equal")
		assert.Equal(t, int64(5), result[0]["y"].(int64), "should be equal")
		assert.Equal(t, int64(5), result[1]["_id"].(int64), "should be equal")
		assert.Equal(t, int64(-30), result[1]["x"].(int64), "should be equal")
		assert.Equal(t, int64(11), result[1]["y"].(int64), "should be equal")
		assert.Equal(t, int32(1), result[2]["x"], "should be equal")
	}

	// applyOps with drop database
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.M{
							{
								"ns": "zz.$cmd",
								"op": "c",
								"ui": "xxxx",
								"o": bson.M{
									"dropDatabase": 1,
								},
							},
						},
					},
				},
			},
		}
		err = RunCommand(testDb, "applyOps", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		dbs, err := conn.Client.ListDatabaseNames(context.Background(), bson.D{})
		fmt.Printf("dbs:%v\n", dbs)
		assert.Equal(t, nil, err, "should be equal")
		exist := false
		for _, db := range dbs {
			if db == "zz" {
				exist = true
			}
		}
		assert.Equal(t, false, exist, "should be equal")
	}

	// normal drop database
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "zz.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "dropDatabase",
						Value: 1,
					},
				},
			},
		}

		err = RunCommand("zz", "dropDatabase", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		dbs, err := conn.Client.ListDatabaseNames(context.Background(), bson.D{})
		fmt.Printf("dbs:%v\n", dbs)
		assert.Equal(t, nil, err, "should be equal")
		exist := false
		for _, db := range dbs {
			if db == "zz" {
				exist = true
			}
		}
		assert.Equal(t, false, exist, "should be equal")
	}

	// create index
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "zz.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "createIndexes",
						Value: "y",
					},
					bson.E{
						Key:   "unique",
						Value: true,
					},
					bson.E{
						Key:   "v",
						Value: 2,
					},
					bson.E{
						Key:   "name",
						Value: "x_1",
					},
					bson.E{
						Key: "key",
						Value: bson.M{
							"x": 1,
						},
					},
				},
			},
		}

		err = RunCommand("zz", "createIndexes", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		cursor, err := conn.Client.Database("zz").Collection("y").Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		indexes := make([]bson.M, 0)
		_ = cursor.All(nil, &indexes)
		fmt.Printf("indexes:%v\n", indexes)

		exist := false
		for _, index := range indexes {
			if index["name"] == "x_1" && index["unique"] == true {
				exist = true
				break
			}
		}
		assert.Equal(t, true, exist, "should be equal")
	}

	// create index
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conf.Options.FilterDDLEnable = true

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, 0)

		// drop database
		err = conn.Client.Database("zz").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("zz").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "zz.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "createIndexes",
						Value: "y",
					},
					bson.E{
						Key:   "unique",
						Value: true,
					},
					bson.E{
						Key:   "v",
						Value: 2,
					},
					bson.E{
						Key:   "name",
						Value: "x_1",
					},
					bson.E{
						Key: "key",
						Value: bson.M{
							"x": 1,
						},
					},
				},
			},
		}
		oplogRecord := &OplogRecord{original: &PartialLogWithCallback{
			partialLog: log,
		}}

		err = writer.doCommand("zz", bson.E{}, []*OplogRecord{oplogRecord})
		assert.Equal(t, nil, err, "should be equal")

		cursor, err := conn.Client.Database("zz").Collection("y").Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		indexes := make([]bson.M, 0)
		_ = cursor.All(nil, &indexes)
		fmt.Printf("indexes:%v\n", indexes)

		exist := false
		for _, index := range indexes {
			if index["name"] == "x_1" && index["unique"] == true {
				exist = true
				break
			}
		}
		assert.Equal(t, true, exist, "should be equal")
	}

	// bulkWrite create index by commitIndexBuild
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database("hh").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("hh").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "hh.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "commitIndexBuild",
						Value: "y",
					},
					bson.E{
						Key: "indexes",
						Value: []bson.D{
							{
								{"unique", true},
								{"v", 2},
								{"name", "x_1"},
								{"key", bson.D{{"x", 1}}},
							},
							{
								{"unique", true},
								{"v", 2},
								{"name", "id_x_1"},
								{"key", bson.D{{"_id", 1}, {"x", 1}}},
							},
						},
					},
				},
			},
		}

		err = RunCommand("hh", "commitIndexBuild", log, conn.Client)
		assert.Equal(t, nil, err, "should be equal")

		cursor, err := conn.Client.Database("hh").Collection("y").Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		indexes := make([]bson.M, 0)
		_ = cursor.All(nil, &indexes)
		fmt.Printf("indexes:%v\n", indexes)

		exist := false
		for _, index := range indexes {
			if index["name"] == "x_1" && index["unique"] == true {
				exist = true
				break
			}
		}
		assert.Equal(t, true, exist, "should be equal")
	}

	// commandWrite create index by commitIndexBuild
	{
		fmt.Printf("TestRunCommand case %d.\n", nr)
		nr++

		conf.Options.FilterDDLEnable = true

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.E{Key: "g", Value: "1"}, true, 0)

		// drop database
		err = conn.Client.Database("hh").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database("hh").Collection("y").InsertOne(context.Background(), bson.M{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "hh.$cmd",
				Object: bson.D{
					bson.E{
						Key:   "commitIndexBuild",
						Value: "y",
					},
					bson.E{
						Key: "indexes",
						Value: []bson.D{
							{
								{"unique", true},
								{"v", 2},
								{"name", "x_1"},
								{"key", bson.D{{"x", 1}}},
							},
						},
					},
				},
			},
		}
		oplogRecord := &OplogRecord{original: &PartialLogWithCallback{
			partialLog: log,
		}}

		err = writer.doCommand("hh", bson.E{}, []*OplogRecord{oplogRecord})
		assert.Equal(t, nil, err, "should be equal")

		cursor, err := conn.Client.Database("hh").Collection("y").Indexes().List(context.Background())
		assert.Equal(t, nil, err, "should be equal")

		indexes := make([]bson.M, 0)
		_ = cursor.All(nil, &indexes)
		fmt.Printf("indexes:%v\n", indexes)

		exist := false
		for _, index := range indexes {
			if index["name"] == "x_1" && index["unique"] == true {
				exist = true
				break
			}
		}
		assert.Equal(t, true, exist, "should be equal")
	}
}

func TestIgnoreError(t *testing.T) {
	// test IgnoreError

	var nr int

	// applyOps
	{
		fmt.Printf("TestIgnoreError case %d.\n", nr)
		nr++

		var err error = &mongo.WriteError{Code: 26}
		ignore := IgnoreError(err, "d", false)
		assert.Equal(t, true, ignore, "should be equal")

		err = &mongo.WriteError{Code: 280}
		ignore = IgnoreError(err, "d", false)
		assert.Equal(t, false, ignore, "should be equal")
	}
}

func TestHasOriginalSpec(t *testing.T) {
	var nr int
	// Case1: no indexes field → false
	{
		fmt.Printf("TestHasOriginalSpec case %d. no indexes field\n", nr)
		nr++
		obj := bson.D{
			{Key: "commitIndexBuild", Value: "system.buckets.weather"},
		}
		assert.Equal(t, false, hasOriginalSpec(obj))
	}
	// Case2: indexes without originalSpec → false
	{
		fmt.Printf("TestHasOriginalSpec case %d. indexes without originalSpec\n", nr)
		nr++
		obj := bson.D{
			{Key: "commitIndexBuild", Value: "system.buckets.weather"},
			{Key: "indexes", Value: bson.A{
				bson.D{
					{Key: "key", Value: bson.D{
						{Key: "meta", Value: 1},
						{Key: "control.min.temperature", Value: 1},
						{Key: "control.max.temperature", Value: 1},
					}},
					{Key: "name", Value: "sensor_1_temperature_1"},
				},
			}},
		}
		assert.Equal(t, false, hasOriginalSpec(obj))
	}
	// Case3: indexes with originalSpec (bson.A) → true
	{
		fmt.Printf("TestHasOriginalSpec case %d. indexes with originalSpec bson.A\n", nr)
		nr++
		obj := bson.D{
			{Key: "commitIndexBuild", Value: "system.buckets.weather"},
			{Key: "indexes", Value: bson.A{
				bson.D{
					{Key: "key", Value: bson.D{
						{Key: "meta", Value: 1},
						{Key: "control.min.temperature", Value: 1},
						{Key: "control.max.temperature", Value: 1},
					}},
					{Key: "name", Value: "sensor_1_temperature_1"},
					{Key: "originalSpec", Value: bson.D{
						{Key: "key", Value: bson.D{
							{Key: "sensor", Value: 1},
							{Key: "temperature", Value: 1},
						}},
						{Key: "name", Value: "sensor_1_temperature_1"},
					}},
				},
			}},
		}
		assert.Equal(t, true, hasOriginalSpec(obj))
	}
	// Case4: indexes with originalSpec ([]bson.D) → true
	{
		fmt.Printf("TestHasOriginalSpec case %d. indexes with originalSpec []bson.D\n", nr)
		nr++
		obj := bson.D{
			{Key: "commitIndexBuild", Value: "system.buckets.weather"},
			{Key: "indexes", Value: []bson.D{
				{
					{Key: "key", Value: bson.D{
						{Key: "meta", Value: 1},
						{Key: "control.min.temperature", Value: 1},
						{Key: "control.max.temperature", Value: 1},
					}},
					{Key: "name", Value: "sensor_1_temperature_1"},
					{Key: "originalSpec", Value: bson.D{
						{Key: "key", Value: bson.D{
							{Key: "sensor", Value: 1},
							{Key: "temperature", Value: 1},
						}},
						{Key: "name", Value: "sensor_1_temperature_1"},
					}},
				},
			}},
		}
		assert.Equal(t, true, hasOriginalSpec(obj))
	}
	// Case5: multiple indexes, only second has originalSpec → true
	{
		fmt.Printf("TestHasOriginalSpec case %d. multiple indexes partial originalSpec\n", nr)
		nr++
		obj := bson.D{
			{Key: "commitIndexBuild", Value: "system.buckets.weather"},
			{Key: "indexes", Value: bson.A{
				bson.D{
					{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
					{Key: "name", Value: "_id_"},
				},
				bson.D{
					{Key: "key", Value: bson.D{
						{Key: "meta", Value: 1},
						{Key: "control.min.temperature", Value: 1},
						{Key: "control.max.temperature", Value: 1},
					}},
					{Key: "name", Value: "sensor_1_temperature_1"},
					{Key: "originalSpec", Value: bson.D{
						{Key: "key", Value: bson.D{
							{Key: "sensor", Value: 1},
							{Key: "temperature", Value: 1},
						}},
						{Key: "name", Value: "sensor_1_temperature_1"},
					}},
				},
			}},
		}
		assert.Equal(t, true, hasOriginalSpec(obj))
	}
}

// TestTimeSeriesIntegration tests time-series collection support against a real MongoDB instance.
// It covers:
//   - doInsert: system.views insert bypassed via 'applyOps' command
//   - doUpdate: system.buckets $v:2 diff with unparseable binary falls back to 'applyOps' command
//   - RunCommand: commitIndexBuild with originalSpec converted to createIndexes on logical collection
//   instead of system.buckets.xx
func TestTimeSeriesIntegration(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	assert.Equal(t, nil, err, "should be equal")
	tsTestDb := "ts_test"
	tsCollection := "weather"
	// Clean up first
	_ = conn.Client.Database(tsTestDb).Drop(nil)
	// Create a time-series collection
	err = conn.Client.Database(tsTestDb).RunCommand(nil, bson.D{
		{Key: "create", Value: tsCollection},
		{Key: "timeseries", Value: bson.D{
			{Key: "timeField", Value: "timestamp"},
			{Key: "metaField", Value: "metadata"},
			{Key: "granularity", Value: "hours"},
		}},
	}).Err()
	if err != nil {
		// If server doesn't support time-series (< 5.0), skip
		t.Skipf("skip time-series test: server may not support time-series collections: %v", err)
		return
	}
	var nr int
	// ---- doInsert: system.views applyOps bypass ----
	{
		fmt.Printf("TestTimeSeriesIntegration case %d. doInsert system.views applyOps bypass\n", nr)
		nr++
		// Drop ts_test2 to test inserting a view definition via applyOps
		viewTestDb := "ts_test2"
		_ = conn.Client.Database(viewTestDb).Drop(nil)
		// applyOps insert requires the target namespace (system.views) to exist.
		// In real replay, "create" DDL for the time-series collection runs first,
		// which automatically creates system.views. Simulate that here.
		err = conn.Client.Database(viewTestDb).RunCommand(nil, bson.D{
			{Key: "create", Value: "dummy_ts"},
			{Key: "timeseries", Value: bson.D{
				{Key: "timeField", Value: "t"},
			}},
		}).Err()
		assert.Equal(t, nil, err, "create dummy time-series to init system.views")
		// Drop the dummy ts collection but keep system.views alive
		_ = conn.Client.Database(viewTestDb).Collection("system.buckets.dummy_ts").Drop(nil)
		writer := NewDbWriter(conn, bson.E{}, false, 0)
		// Mock an oplog that inserts a view definition into system.views
		viewDoc := bson.D{
			{Key: "_id", Value: viewTestDb + "." + tsCollection},
			{Key: "viewOn", Value: "system.buckets." + tsCollection},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$_internalUnpackBucket", Value: bson.D{
					{Key: "timeField", Value: "timestamp"},
					{Key: "metaField", Value: "metadata"},
					{Key: "bucketMaxSpanSeconds", Value: 2592000},
				}}},
			}},
		}
		insertOplogs := []*OplogRecord{
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Operation: "i",
							Namespace: viewTestDb + ".system.views",
							Object:    viewDoc,
						},
					},
				},
			},
		}
		err = writer.doInsert(viewTestDb, "system.views", bson.E{}, insertOplogs, false)
		if err != nil {
			// applyOps require special privileges ('__system' role).
			// If we get an auth error, it confirms the applyOps code path was taken.
			if strings.Contains(err.Error(), "Unauthorized") ||
				strings.Contains(err.Error(), "not authorized") {
				fmt.Printf("[ok] system.views insert correctly use applyOps(auth denied as expected): %v\n", err)
			} else {
				t.Errorf("doInsert system.views unexpected error: %v", err)
			}
		} else {
			// Verify the view was inserted
			result, fetchErr := unit_test_common.FetchAllDocumentBsonM(conn.Client, viewTestDb, "system.views", nil)
			assert.Equal(t, nil, fetchErr, "should be equal")
			assert.Equal(t, 1, len(result), "should have 1 view definition")
		}
		_ = conn.Client.Database(viewTestDb).Drop(nil)
	}
	// ---- doUpdate: system.buckets applyOps fallback ----
	{
		fmt.Printf("TestTimeSeriesIntegration case %d. doUpdate system.buckets applyOps fallback\n", nr)
		nr++
		// Insert some data into the time-series collection first
		tsColl := conn.Client.Database(tsTestDb).Collection(tsCollection)
		_, err = tsColl.InsertOne(nil, bson.D{
			{Key: "timestamp", Value: time.Now()},
			{Key: "metadata", Value: bson.D{{Key: "sensorId", Value: 1}}},
			{Key: "temperature", Value: 22.5},
		})
		assert.Equal(t, nil, err, "insert time-series data should succeed")
		// Find the bucket document to get its _id
		bucketResults, err := unit_test_common.FetchAllDocumentBsonM(
			conn.Client, tsTestDb, "system.buckets."+tsCollection, nil)
		assert.Equal(t, nil, err, "should be equal")
		assert.True(t, len(bucketResults) > 0, "should have at least 1 bucket")
		bucketId := bucketResults[0]["_id"]
		// Mock a $v:2 diff update oplog with only scontrol section (parseable).
		// Reference: real time-series update oplog from MongoDB 5.0+
		bucketNs := tsTestDb + ".system.buckets." + tsCollection
		updateObj := bson.D{
			{Key: "$v", Value: int32(2)},
			{Key: "diff", Value: bson.D{
				{Key: "scontrol", Value: bson.D{
					{Key: "u", Value: bson.D{
						{Key: "count", Value: int32(5)},
					}},
					{Key: "smin", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "humidity", Value: 50}}},
					}},
					{Key: "smax", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "temperature", Value: 30}}},
					}},
				}},
			}},
		}
		updateOplogs := []*OplogRecord{
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Operation: "u",
							Namespace: bucketNs,
							Object:    updateObj,
							Query:     bson.D{{Key: "_id", Value: bucketId}},
						},
					},
				},
			},
		}
		writer := NewDbWriter(conn, bson.E{}, false, 0)
		// This should succeed via normal $v:2 diff parsing (scontrol fields are parseable)
		err = writer.doUpdate(tsTestDb, "system.buckets."+tsCollection, bson.E{}, updateOplogs, false)
		assert.Equal(t, nil, err, "doUpdate with parseable $v:2 diff should succeed")
		// Now test with scontrol + sdata.b that triggers applyOps fallback.
		// sdata.b contains column-store binary diffs which DiffUpdateOplogToNormal cannot parse.
		updateObjWithBinary := bson.D{
			{Key: "$v", Value: int32(2)},
			{Key: "diff", Value: bson.D{
				{Key: "scontrol", Value: bson.D{
					{Key: "u", Value: bson.D{
						{Key: "count", Value: int32(5)},
					}},
					{Key: "smin", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "humidity", Value: 50}}},
					}},
					{Key: "smax", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "temperature", Value: 30}}},
					}},
				}},
				{Key: "sdata", Value: bson.D{
					{Key: "b", Value: bson.D{
						{Key: "temperature", Value: bson.D{
							{Key: "o", Value: int32(10)},
							{Key: "d", Value: primitive.Binary{Subtype: 0, Data: []byte("oGsAMAAoAh4BAA==")}},
						}},
						{Key: "time", Value: bson.D{
							{Key: "o", Value: int32(10)},
							{Key: "d", Value: primitive.Binary{Subtype: 0, Data: []byte("gQx8ksAn+XuSDgAAAAAAAAAA")}},
						}},
						{Key: "humidity", Value: bson.D{
							{Key: "o", Value: int32(10)},
							{Key: "d", Value: primitive.Binary{Subtype: 0, Data: []byte("kBsACAAQADoAAA==")}},
						}},
						{Key: "pressure", Value: bson.D{
							{Key: "o", Value: int32(10)},
							{Key: "d", Value: primitive.Binary{Subtype: 0, Data: []byte("sJsASABQABIAAA==")}},
						}},
					}},
				}},
			}},
		}
		updateOplogsWithBinary := []*OplogRecord{
			{
				original: &PartialLogWithCallback{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Operation: "u",
							Namespace: bucketNs,
							Object:    updateObjWithBinary,
							Query:     bson.D{{Key: "_id", Value: bucketId}},
						},
					},
				},
			},
		}
		// This should fall back to applyOps because sdata.b cannot be parsed.
		// The applyOps may fail on the server side (auth or because the diff is fabricated),
		// but it should NOT fail with the "unknow Key[b]" parse error.
		err = writer.doUpdate(tsTestDb, "system.buckets."+tsCollection, bson.E{}, updateOplogsWithBinary, false)
		if err != nil {
			assert.NotContains(t, err.Error(), "unknow Key",
				"error should NOT be from DiffUpdateOplogToNormal parse; should be from applyOps server-side")
			if strings.Contains(err.Error(), "Unauthorized") ||
				strings.Contains(err.Error(), "not authorized") {
				fmt.Printf("  [ok] system.buckets update correctly routed to applyOps (auth denied): %v\n", err)
			} else {
				fmt.Printf("  [expected] applyOps fallback returned server-side error: %v\n", err)
			}
		} else {
			fmt.Println("  applyOps fallback succeeded (server accepted the fabricated diff)")
		}
	}
	// ---- CommandWriter.doUpdate: system.buckets $v:2 diff handling ----
	{
		fmt.Printf("TestTimeSeriesIntegration case %d. CommandWriter doUpdate system.buckets\n", nr)
		nr++
		// Reuse the bucket doc from previous case
		bucketResults, err := unit_test_common.FetchAllDocumentBsonM(
			conn.Client, tsTestDb, "system.buckets."+tsCollection, nil)
		assert.Equal(t, nil, err)
		assert.True(t, len(bucketResults) > 0, "should have at least 1 bucket")
		bucketId := bucketResults[0]["_id"]
		bucketNs := tsTestDb + ".system.buckets." + tsCollection
		// CommandWriter is created when metadata has "g" (gid)
		gid := bson.E{Key: "g", Value: "test-gid"}
		cmdWriter := NewDbWriter(conn, gid, false, 0)
		// Test 1: parseable scontrol-only diff should succeed via DiffUpdateOplogToNormal
		controlOnlyObj := bson.D{
			{Key: "$v", Value: int32(2)},
			{Key: "diff", Value: bson.D{
				{Key: "scontrol", Value: bson.D{
					{Key: "u", Value: bson.D{
						{Key: "count", Value: int32(5)},
					}},
					{Key: "smin", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "humidity", Value: 50}}},
					}},
					{Key: "smax", Value: bson.D{
						{Key: "u", Value: bson.D{{Key: "temperature", Value: 30}}},
					}},
				}},
			}},
		}
		controlOplogs := []*OplogRecord{{
			original: &PartialLogWithCallback{
				partialLog: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Operation: "u",
						Namespace: bucketNs,
						Object:    controlOnlyObj,
						Query:     bson.D{{Key: "_id", Value: bucketId}},
					},
				},
			},
		}}
		err = cmdWriter.doUpdate(tsTestDb, "system.buckets."+tsCollection, gid, controlOplogs, false)
		assert.Equal(t, nil, err, "CommandWriter doUpdate with parseable scontrol diff should succeed")
		// Test 2: scontrol + sdata.b should fall back to applyOps
		binaryObj := bson.D{
			{Key: "$v", Value: int32(2)},
			{Key: "diff", Value: bson.D{
				{Key: "scontrol", Value: bson.D{
					{Key: "u", Value: bson.D{
						{Key: "count", Value: int32(5)},
					}},
				}},
				{Key: "sdata", Value: bson.D{
					{Key: "b", Value: bson.D{
						{Key: "temperature", Value: bson.D{
							{Key: "o", Value: int32(10)},
							{Key: "d", Value: primitive.Binary{Subtype: 0, Data: []byte("oGsAMAAoAh4BAA==")}},
						}},
					}},
				}},
			}},
		}
		binaryOplogs := []*OplogRecord{{
			original: &PartialLogWithCallback{
				partialLog: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Operation: "u",
						Namespace: bucketNs,
						Object:    binaryObj,
						Query:     bson.D{{Key: "_id", Value: bucketId}},
					},
				},
			},
		}}
		err = cmdWriter.doUpdate(tsTestDb, "system.buckets."+tsCollection, gid, binaryOplogs, false)
		if err != nil {
			assert.NotContains(t, err.Error(), "unknow Key",
				"CommandWriter error should NOT be from DiffUpdateOplogToNormal parse")
			fmt.Printf("  [expected] CommandWriter applyOps fallback returned: %v\n", err)
		} else {
			fmt.Println("  CommandWriter applyOps fallback succeeded")
		}
	}
	// ---- RunCommand: commitIndexBuild with originalSpec ----
	{
		fmt.Printf("TestTimeSeriesIntegration case %d. RunCommand commitIndexBuild with originalSpec\n", nr)
		nr++
		// Create a fresh time-series collection for index test
		idxTestDb := "ts_test_idx"
		_ = conn.Client.Database(idxTestDb).Drop(nil)
		err = conn.Client.Database(idxTestDb).RunCommand(nil, bson.D{
			{Key: "create", Value: tsCollection},
			{Key: "timeseries", Value: bson.D{
				{Key: "timeField", Value: "timestamp"},
				{Key: "metaField", Value: "metadata"},
				{Key: "granularity", Value: "hours"},
			}},
		}).Err()
		assert.Equal(t, nil, err, "create time-series collection should succeed")
		// Test that hasOriginalSpec correctly detects the flag and RunCommand
		// takes the applyOps path. We simulate a commitIndexBuild oplog with
		// originalSpec using the DDL applyOps approach.
		// First, create an index via createIndexes on the logical collection
		err = conn.Client.Database(idxTestDb).RunCommand(nil, bson.D{
			{Key: "createIndexes", Value: tsCollection},
			{Key: "indexes", Value: bson.A{
				bson.D{
					{Key: "key", Value: bson.D{{Key: "metadata.sensorId", Value: 1}}},
					{Key: "name", Value: "metadata.sensorId_1"},
				},
			}},
		}).Err()
		assert.Equal(t, nil, err, "createIndexes on time-series should succeed")
		// Verify the index exists
		cursor, err := conn.Client.Database(idxTestDb).Collection("system.buckets." + tsCollection).Indexes().List(nil)
		assert.Equal(t, nil, err)
		var indexes []bson.M
		err = cursor.All(nil, &indexes)
		assert.Equal(t, nil, err)
		fmt.Printf("  indexes on system.buckets.%s: %d\n", tsCollection, len(indexes))
		// Now test RunCommand with a commitIndexBuild oplog that has originalSpec.
		// Drop the index first, then replay via RunCommand.
		_ = conn.Client.Database(idxTestDb).RunCommand(nil, bson.D{
			{Key: "dropIndexes", Value: "system.buckets." + tsCollection},
			{Key: "index", Value: "metadata.sensorId_1"},
		}).Err()
		// Simulate a commitIndexBuild oplog with originalSpec
		pLog := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: idxTestDb + ".$cmd",
				Version:   2,
				Object: bson.D{
					{Key: "commitIndexBuild", Value: "system.buckets." + tsCollection},
					{Key: "indexes", Value: bson.A{
						bson.D{
							{Key: "v", Value: int32(2)},
							{Key: "key", Value: bson.D{{Key: "meta.sensorId", Value: int32(1)}}},
							{Key: "name", Value: "metadata.sensorId_1"},
							{Key: "originalSpec", Value: bson.D{
								{Key: "key", Value: bson.D{{Key: "metadata.sensorId", Value: int32(1)}}},
								{Key: "name", Value: "metadata.sensorId_1"},
								{Key: "v", Value: int32(2)},
							}},
						},
					}},
				},
			},
		}
		// Verify hasOriginalSpec detects it
		assert.True(t, hasOriginalSpec(pLog.Object), "should detect originalSpec")
		// Verify extractOriginalSpecs extracts the logical index spec
		specs := extractOriginalSpecs(pLog.Object)
		assert.Equal(t, 1, len(specs), "should extract 1 originalSpec")
		// RunCommand should convert to createIndexes on the logical collection
		err = RunCommand(idxTestDb, "commitIndexBuild", pLog, conn.Client)
		assert.Equal(t, nil, err, "commitIndexBuild with originalSpec should succeed via createIndexes")
		// Verify the index was re-created on system.buckets
		cursor, listErr := conn.Client.Database(idxTestDb).Collection("system.buckets." + tsCollection).Indexes().List(nil)
		assert.Equal(t, nil, listErr)
		var newIndexes []bson.M
		listErr = cursor.All(nil, &newIndexes)
		assert.Equal(t, nil, listErr)
		fmt.Printf("  indexes after commitIndexBuild replay: %d\n", len(newIndexes))
		assert.True(t, len(newIndexes) >= 2, "should have at least 2 indexes (default + replayed)")
		_ = conn.Client.Database(idxTestDb).Drop(nil)
	}
	// Final cleanup
	_ = conn.Client.Database(tsTestDb).Drop(nil)
}

func TestParseDupKeyIndexName(t *testing.T) {
	tests := []struct {
		errMsg   string
		expected string
	}{
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: account_1 dup key: { account: "abc" }`,
			expected: "account_1",
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: _id_ dup key: { _id: ObjectId("123") }`,
			expected: "_id_",
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: a_1_b_1 dup key: { a: "x", b: "y" }`,
			expected: "a_1_b_1",
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: my_custom_name dup key: { x: 1 }`,
			expected: "my_custom_name",
		},
		{
			errMsg:   `some other error`,
			expected: "",
		},
	}

	for i, tt := range tests {
		fmt.Printf("TestParseDupKeyIndexName case %d.\n", i)
		err := fmt.Errorf("%s", tt.errMsg)
		result := parseDupKeyIndexName(err)
		assert.Equal(t, tt.expected, result)
	}
	assert.Equal(t, "", parseDupKeyIndexName(nil))
}

func TestGetFieldValue(t *testing.T) {
	doc := bson.D{
		{"name", "alice"},
		{"age", 30},
		{"address", bson.D{
			{"city", "shanghai"},
			{"zip", "200000"},
		}},
		{"nullable", nil},
	}

	val, found := getFieldValue(doc, "name")
	assert.True(t, found)
	assert.Equal(t, "alice", val)

	val, found = getFieldValue(doc, "age")
	assert.True(t, found)
	assert.Equal(t, 30, val)

	val, found = getFieldValue(doc, "address.city")
	assert.True(t, found)
	assert.Equal(t, "shanghai", val)

	val, found = getFieldValue(doc, "nonexist")
	assert.False(t, found)
	assert.Nil(t, val)

	val, found = getFieldValue(doc, "address.nonexist")
	assert.False(t, found)
	assert.Nil(t, val)

	// null value should be distinguishable from missing field
	val, found = getFieldValue(doc, "nullable")
	assert.True(t, found)
	assert.Nil(t, val)
}

func TestSplitDotted(t *testing.T) {
	assert.Equal(t, []string{"a"}, splitDotted("a"))
	assert.Equal(t, []string{"a", "b"}, splitDotted("a.b"))
	assert.Equal(t, []string{"a", "b", "c"}, splitDotted("a.b.c"))
	assert.Nil(t, splitDotted(""))
}

func TestParseDupKeyFields(t *testing.T) {
	tests := []struct {
		errMsg   string
		expected []string
	}{
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: account_1 dup key: { account: "abc" }`,
			expected: []string{"account"},
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: a_1_b_1 dup key: { a: "x", b: "y" }`,
			expected: []string{"a", "b"},
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: addr_idx dup key: { address.city: "shanghai" }`,
			expected: []string{"address.city"},
		},
		{
			errMsg:   `E11000 duplicate key error collection: db.coll index: _id_ dup key: { _id: ObjectId("123") }`,
			expected: []string{"_id"},
		},
		{
			errMsg:   `some other error`,
			expected: nil,
		},
	}

	for i, tt := range tests {
		fmt.Printf("TestParseDupKeyFields case %d.\n", i)
		err := fmt.Errorf("%s", tt.errMsg)
		result := parseDupKeyFields(err)
		assert.Equal(t, tt.expected, result)
	}
	assert.Nil(t, parseDupKeyFields(nil))
}

func TestResolveConflictFilter(t *testing.T) {
	doc := bson.D{{"_id", "test1"}, {"x", "hello"}, {"y", 42}, {"z", "extra"}}

	// single field
	err := fmt.Errorf(`E11000 duplicate key error collection: db.coll index: x_1 dup key: { x: "hello" }`)
	filter := resolveConflictFilter(err, doc)
	assert.Equal(t, bson.D{{"x", "hello"}}, filter)

	// compound index
	err = fmt.Errorf(`E11000 duplicate key error collection: db.coll index: x_1_y_1 dup key: { x: "hello", y: 42 }`)
	filter = resolveConflictFilter(err, doc)
	assert.Equal(t, bson.D{{"x", "hello"}, {"y", 42}}, filter)

	// missing field in doc returns nil
	docMissing := bson.D{{"_id", "test2"}, {"x", "hello"}}
	err = fmt.Errorf(`E11000 duplicate key error collection: db.coll index: x_1_y_1 dup key: { x: "hello", y: 42 }`)
	filter = resolveConflictFilter(err, docMissing)
	assert.Nil(t, filter)

	// unparseable error returns nil
	err = fmt.Errorf(`some other error`)
	filter = resolveConflictFilter(err, doc)
	assert.Nil(t, filter)

	// dotted path
	docNested := bson.D{{"_id", "n1"}, {"address", bson.D{{"city", "shanghai"}}}}
	err = fmt.Errorf(`E11000 duplicate key error collection: db.coll index: addr_idx dup key: { address.city: "shanghai" }`)
	filter = resolveConflictFilter(err, docNested)
	assert.Equal(t, bson.D{{"address.city", "shanghai"}}, filter)
}

// dupKeyErrLike returns a real mongo.WriteException with code 11000 and the
// given E11000 message — the same error shape that mongo driver returns from
// a real failing insert. Tests that exercise handleDupKeyOnInsert /
// shouldSkipDupKeyOnInsert must use this rather than fmt.Errorf, since
// utils.DuplicateKey -> mongo.IsDuplicateKeyError checks the ServerError
// interface (HasErrorCode 11000), not the error string.
func dupKeyErrLike(msg string) error {
	return mongo.WriteException{
		WriteErrors: mongo.WriteErrors{{Code: 11000, Message: msg}},
	}
}

func TestShouldSkipDupKeyOnInsert(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	err := dupKeyErrLike(`E11000 duplicate key error collection: db.coll index: x_1 dup key: { x: 1 }`)
	conf.Options = conf.Configuration{
		IncrSyncExecutorDupKeyStrategy: utils.VarIncrSyncExecutorDupKeyStrategySkip,
		IncrSyncExecutorDupKeySkipRulesMap: map[string]map[string]struct{}{
			"db.coll": {"x_1": {}},
		},
	}

	skip, indexName := shouldSkipDupKeyOnInsert("db", "coll", err)
	assert.True(t, skip, "should be equal")
	assert.Equal(t, "x_1", indexName, "should be equal")

	skip, _ = shouldSkipDupKeyOnInsert("db", "other", err)
	assert.False(t, skip, "should be equal")

	conf.Options.IncrSyncExecutorDupKeySkipRulesMap = map[string]map[string]struct{}{
		"db.coll": {"y_1": {}},
	}
	skip, indexName = shouldSkipDupKeyOnInsert("db", "coll", err)
	assert.False(t, skip, "should be equal")
	assert.Equal(t, "x_1", indexName, "should be equal")

	conf.Options.IncrSyncExecutorDupKeySkipRulesMap = map[string]map[string]struct{}{
		"db.coll": {"*": {}},
	}
	skip, indexName = shouldSkipDupKeyOnInsert("db", "coll", err)
	assert.True(t, skip, "should be equal")
	assert.Equal(t, "x_1", indexName, "should be equal")
	assert.True(t, shouldSkipDupKeyIndex("db", "coll", "x_1"), "should be equal")
	assert.False(t, shouldSkipDupKeyIndex("db", "other", "x_1"), "should be equal")

	idErr := dupKeyErrLike(`E11000 duplicate key error collection: db.coll index: _id_ dup key: { _id: 1 }`)
	skip, indexName = shouldSkipDupKeyOnInsert("db", "coll", idErr)
	assert.False(t, skip, "_id duplicate is handled by writer-level already-applied logic")
	assert.Equal(t, "_id_", indexName, "should be equal")
}

func TestHandleDupKeyOnInsertStrategy(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	err := dupKeyErrLike(`E11000 duplicate key error collection: db.coll index: x_1 dup key: { x: 1 }`)

	conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore}
	assert.NoError(t, handleDupKeyOnInsert(nil, "db", "coll", nil, err, "test"), "should be equal")

	conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: utils.VarIncrSyncExecutorDupKeyStrategyError}
	assert.Error(t, handleDupKeyOnInsert(nil, "db", "coll", nil, err, "test"), "should be equal")

	conf.Options = conf.Configuration{
		IncrSyncExecutorDupKeyStrategy: utils.VarIncrSyncExecutorDupKeyStrategySkip,
		IncrSyncExecutorDupKeySkipRulesMap: map[string]map[string]struct{}{
			"db.coll": {"x_1": {}},
		},
	}
	assert.NoError(t, handleDupKeyOnInsert(nil, "db", "coll", nil, err, "test"), "should be equal")

	conf.Options.IncrSyncExecutorDupKeySkipRulesMap = map[string]map[string]struct{}{
		"db.coll": {"y_1": {}},
	}
	assert.Error(t, handleDupKeyOnInsert(nil, "db", "coll", nil, err, "test"), "should be equal")

	// Non-dup-key errors must always be returned untouched, regardless of
	// strategy — the routing in handleDupKeyOnInsert depends on
	// IsDuplicateKeyError, so a server-side error of a different code must
	// not be swallowed.
	conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore}
	otherErr := mongo.WriteException{
		WriteErrors: mongo.WriteErrors{{Code: 121, Message: "DocumentValidationFailure"}},
	}
	assert.Error(t, handleDupKeyOnInsert(nil, "db", "coll", nil, otherErr, "test"),
		"non-dup-key errors must propagate even under Ignore")
}

func TestSingleWriterDeleteOnNonIdDupKey(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("skip integration test, cannot connect to MongoDB: %v", err)
	}

	conf.Options.IncrSyncExecutorDupKeyStrategy = utils.VarIncrSyncExecutorDupKeyStrategyDeleteAndRetry
	defer func() { conf.Options.IncrSyncExecutorDupKeyStrategy = utils.VarIncrSyncExecutorDupKeyStrategyError }()

	_ = utils.InitialLogger("", "", "debug", true, 1)
	writer := NewDbWriter(conn, bson.E{}, false, -1)

	_ = conn.Client.Database(testDb).Drop(nil)
	coll := conn.Client.Database(testDb).Collection(testCollection)

	// create unique index on field 'x'
	_, err = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{"x", 1}},
		Options: options.Index().SetUnique(true),
	})
	assert.NoError(t, err)

	// insert doc {_id: A, x: 1}
	idA := primitive.NewObjectID()
	idB := primitive.NewObjectID()
	inserts := []*OplogRecord{mockOplogRecord(idA, int32(1), -1)}
	err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
	assert.NoError(t, err)

	// upsert {_id: B, x: 1} — different _id, same unique field value
	upserts := []*OplogRecord{mockOplogRecord(idB, int32(1), -1)}
	err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, upserts, true)
	assert.NoError(t, err)

	// verify: should have exactly 1 document with _id=B, x=1
	result, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, testDb, testCollection, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, idB, result[0]["_id"])
	assert.Equal(t, int32(1), result[0]["x"])

	_ = conn.Client.Database(testDb).Drop(nil)
}

func TestSingleWriterDeleteOnNonIdDupKeyDisabled(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("skip integration test, cannot connect to MongoDB: %v", err)
	}

	conf.Options.IncrSyncExecutorDupKeyStrategy = utils.VarIncrSyncExecutorDupKeyStrategyError
	_ = utils.InitialLogger("", "", "debug", true, 1)
	writer := NewDbWriter(conn, bson.E{}, false, -1)

	_ = conn.Client.Database(testDb).Drop(nil)
	coll := conn.Client.Database(testDb).Collection(testCollection)

	_, err = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{"x", 1}},
		Options: options.Index().SetUnique(true),
	})
	assert.NoError(t, err)

	idA := primitive.NewObjectID()
	idB := primitive.NewObjectID()
	inserts := []*OplogRecord{mockOplogRecord(idA, int32(1), -1)}
	err = writer.doInsert(testDb, testCollection, bson.E{}, inserts, false)
	assert.NoError(t, err)

	// with feature disabled, upsert should fail
	upserts := []*OplogRecord{mockOplogRecord(idB, int32(1), -1)}
	err = writer.doUpdateOnInsert(testDb, testCollection, bson.E{}, upserts, true)
	assert.Error(t, err)

	_ = conn.Client.Database(testDb).Drop(nil)
}
