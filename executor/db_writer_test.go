package executor

import (
	"context"
	"fmt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"strings"
	"testing"
)

const (
	testMongoAddress         = unit_test_common.TestUrl
	testMongoShardingAddress = unit_test_common.TestUrlSharding
	testDb                   = "writer_test"
	testCollection           = "a"
)

func mockOplogRecord(oId, oX interface{}, o2Id int) *OplogRecord {
	or := &OplogRecord{
		original: &PartialLogWithCallbak{
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

// TODO(jianyou) deprecate
func objectIdFromInt(num int64) primitive.ObjectID {
	objectId, err := primitive.ObjectIDFromHex(fmt.Sprintf("%024s", strconv.FormatInt(num, 16)))
	if err != nil {
		return primitive.ObjectID{}
	}

	return objectId
}

func TestSingleWriter(t *testing.T) {
	// test single writer

	utils.InitialLogger("", "", "debug", true, 1)

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

		writer := NewDbWriter(conn, bson.M{}, false, 0)

		inserts := []*OplogRecord{mockOplogRecord(1, 1, -1)}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update 1->2
		err = writer.doUpdate(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(1, 10, 1),
		}, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, nil)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, len(result), "should be equal")
		assert.Equal(t, int32(10), result[0]["x"], "should be equal")
		assert.Equal(t, int32(1), result[0]["_id"], "should be equal")

		// delete 2
		err = writer.doDelete(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(1, 1, 1),
		})
		assert.Equal(t, nil, err, "should be equal")

		// query
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, nil)
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

		writer := NewDbWriter(conn, bson.M{}, false, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// write 1
		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
		}
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// upsert write 2(update do not exit, then insert)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 2 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(result), "should be equal")
		assert.Equal(t, int32(10000), result[0]["x"], "should be equal")
	}

	// upsert with duplicate key error
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, false, 1)

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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 10, -1),
		}
		// write 1
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, false, 0)

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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update 6->10
		err = writer.doUpdate(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(6, 10, 6),
		}, false)
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Printf("err:%v\n", err)

		// upsert 6->10
		err = writer.doUpdate(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(6, 10, 6),
		}, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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
		err = writer.doInsert(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(1, 30, 1),
		}, false)
		assert.Equal(t, nil, err, "should be equal")

		// dupInsert -> update
		err = writer.doInsert(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(1, 30, 1),
		}, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6, len(result), "should be equal")
		assert.Equal(t, int32(30), result[0]["x"], "should be equal")

		// delete not found
		err = writer.doDelete(testDb, testCollection, bson.M{}, []*OplogRecord{
			mockOplogRecord(20, 20, 20),
		})
		assert.Equal(t, nil, err, "should be equal")
	}

	// test ignore error
	{
		fmt.Printf("TestSingleWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, false, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallbak{
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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallbak{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Timestamp: 1,
							Object: bson.D{
								primitive.E{
									Key:   "x.0.y",
									Value: 123,
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		writer := NewDbWriter(conn, bson.M{}, false, 0)

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

		err = writer.doUpdate(testDb, testCollection, bson.M{}, inserts, true)
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		res, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		// see https://github.com/alibaba/MongoShake/issues/380 (need retryWrites: true)
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts2, true)
		assert.NotEqual(t, nil, err, "should be equal")
		// assert.Equal(t, true, strings.Contains(err.Error(), "Must run update to shard key"), "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		res, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(res), "should be equal")
		assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		assert.Equal(t, int32(2), res[1]["x"], "should be equal")
		assert.Equal(t, int32(3), res[2]["x"], "should be equal")
	}
}

func TestBulkWriter(t *testing.T) {
	// test bulk writer

	utils.InitialLogger("", "", "debug", true, 1)

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

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 10, len(result), "should be equal")
		assert.Equal(t, int32(80), result[7]["x"], "should be equal")
		assert.Equal(t, int32(90), result[8]["x"], "should be equal")
		assert.Equal(t, int32(100), result[9]["x"], "should be equal")

		// delete 8-11
		deletes := []*OplogRecord{
			mockOplogRecord(8, 80, -1),
			mockOplogRecord(9, 90, -1),
			mockOplogRecord(10, 100, -1),
			mockOplogRecord(11, 110, -1), // not found
		}
		err = writer.doDelete(testDb, testCollection, bson.M{}, deletes)
		assert.Equal(t, nil, err, "should be equal") // won't throw error if not found

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		writer := NewDbWriter(conn, bson.M{}, true, 0)

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// write 1
		inserts := []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), 1, -1),
		}
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 1 again(do update)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789011), int32(10001), -1),
		}
		inserts[0].original.partialLog.DocumentKey = bson.D{
			{"_id", objectIdFromInt(123456789011)},
			{"x", int32(10000)},
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// upsert write 2(update do not exit, then insert)
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(10000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// write 2 again
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// upsert + update
		inserts = []*OplogRecord{
			mockOplogRecord(objectIdFromInt(123456789012), int32(20001), -1),
			mockOplogRecord(objectIdFromInt(123456789013), int32(30000), -1),
		}
		err = writer.doUpdateOnInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(result), "should be equal")
		assert.Equal(t, int32(10001), result[0]["x"], "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update not exist
		updates := []*OplogRecord{
			mockOplogRecord(5, 50, 5),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// not work
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, int32(40), result[3]["x"], "should be equal")
		assert.Equal(t, int32(50), result[4]["x"], "should be equal")
		assert.Equal(t, int32(100), result[5]["x"], "should be equal")
		assert.Equal(t, int32(110), result[6]["x"], "should be equal")

		// deletes
		deletes := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(999, 999, -1), // not exist
		}

		err = writer.doDelete(testDb, testCollection, bson.M{}, deletes)
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, false)
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Println(err)

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, int32(3), result[2]["x"], "should be equal")

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		opts = options.Find().SetSort(bson.D{{"_id", 1}})
		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		utils.InitialLogger("", "", "info", true, 1)

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallbak{
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
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		result, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3]["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallbak{
					partialLog: &oplog.PartialLog{
						ParsedLog: oplog.ParsedLog{
							Timestamp: 1,
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		result, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		conn, err := utils.NewMongoCommunityConn(testMongoShardingAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, 0)

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

		// 1-2
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, 1),
			mockOplogRecord(2, 2, 2),
		}

		err = writer.doUpdate(testDb, testCollection, bson.M{}, inserts, true)
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
		err = writer.doUpdate(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		opts := options.Find().SetSort(bson.D{{"_id", 1}})
		res, err := unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
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

		//// see https://github.com/alibaba/MongoShake/issues/380
		//err = writer.doInsert(testDb, testCollection, bson.M{}, inserts2, true)
		//fmt.Printf("err:%v\n", err)
		//assert.NotEqual(t, nil, err, "should be equal")
		//assert.Equal(t, true, strings.Contains(err.Error(), "Must run update to shard key"), "should be equal")
		//
		//// query
		//opts = options.Find().SetSort(bson.D{{"_id", 1}})
		//res, err = unit_test_common.FetchAllDocumentbsonM(conn, testDb, testCollection, opts)
		//assert.Equal(t, nil, err, "should be equal")
		//assert.Equal(t, 3, len(res), "should be equal")
		//assert.Equal(t, int32(1), res[0]["x"], "should be equal")
		//assert.Equal(t, int32(2), res[1]["x"], "should be equal")
		//assert.Equal(t, int32(3), res[2]["x"], "should be equal")
	}
}

//func TestRunCommand(t *testing.T) {
//	// test RunCommand
//
//	var nr int
//
//	// applyOps
//	{
//		fmt.Printf("TestRunCommand case %d.\n", nr)
//		nr++
//
//		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
//		assert.Equal(t, nil, err, "should be equal")
//
//		// drop database
//		err = conn.Session.DB("zz").DropDatabase()
//		assert.Equal(t, nil, err, "should be equal")
//
//		err = conn.Session.DB("zz").C("y").Insert(bson.M{"x": 1})
//		assert.Equal(t, nil, err, "should be equal")
//
//		log := &oplog.PartialLog{
//			ParsedLog: oplog.ParsedLog{
//				Operation: "i",
//				Namespace: "admin.$cmd",
//				Object: bson.D{
//					bson.DocElem{
//						Name: "applyOps",
//						Value: []bson.M{
//							{
//								"ns": "zz.y",
//								"op": "i",
//								"ui": "xxxx",
//								"o": bson.M{
//									"_id":   "567",
//									"hello": "world",
//								},
//							},
//							{
//								"ns": "zz.y",
//								"op": "i",
//								"ui": "xxxx2",
//								"o": bson.D{
//									bson.DocElem{
//										Name:  "_id",
//										Value: "789",
//									},
//									bson.DocElem{
//										Name:  "hello",
//										Value: "w2",
//									},
//								},
//							},
//						},
//					},
//				},
//			},
//		}
//		err = RunCommand(testDb, "applyOps", log, conn.Session)
//		assert.Equal(t, nil, err, "should be equal")
//
//		result := make(map[string]interface{})
//		err = conn.Session.DB("zz").C("y").Find(bson.M{"hello": "world"}).One(&result)
//		assert.Equal(t, nil, err, "should be equal")
//		assert.Equal(t, "567", result["_id"].(string), "should be equal")
//		assert.Equal(t, "world", result["hello"].(string), "should be equal")
//
//		result = make(map[string]interface{})
//		err = conn.Session.DB("zz").C("y").Find(bson.M{"hello": "w2"}).One(&result)
//		assert.Equal(t, nil, err, "should be equal")
//		assert.Equal(t, "789", result["_id"].(string), "should be equal")
//		assert.Equal(t, "w2", result["hello"].(string), "should be equal")
//	}
//
//	// applyOps with drop database
//	{
//		fmt.Printf("TestRunCommand case %d.\n", nr)
//		nr++
//
//		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
//		assert.Equal(t, nil, err, "should be equal")
//
//		err = conn.Session.DB("zz").C("y").Insert(bson.M{"x": 1})
//		assert.Equal(t, nil, err, "should be equal")
//
//		log := &oplog.PartialLog{
//			ParsedLog: oplog.ParsedLog{
//				Operation: "c",
//				Namespace: "admin.$cmd",
//				Object: bson.D{
//					bson.DocElem{
//						Name: "applyOps",
//						Value: []bson.M{
//							{
//								"ns": "zz.$cmd",
//								"op": "c",
//								"ui": "xxxx",
//								"o": bson.M{
//									"dropDatabase": 1,
//								},
//							},
//						},
//					},
//				},
//			},
//		}
//		err = RunCommand(testDb, "applyOps", log, conn.Session)
//		assert.Equal(t, nil, err, "should be equal")
//
//		dbs, err := conn.Session.DatabaseNames()
//		assert.Equal(t, nil, err, "should be equal")
//		exist := false
//		for _, db := range dbs {
//			if db == "zz" {
//				exist = true
//			}
//		}
//		assert.Equal(t, false, exist, "should be equal")
//	}
//
//	// normal drop database
//	{
//		fmt.Printf("TestRunCommand case %d.\n", nr)
//		nr++
//
//		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
//		assert.Equal(t, nil, err, "should be equal")
//
//		err = conn.Session.DB("zz").C("y").Insert(bson.M{"x": 1})
//		assert.Equal(t, nil, err, "should be equal")
//
//		log := &oplog.PartialLog{
//			ParsedLog: oplog.ParsedLog{
//				Operation: "c",
//				Namespace: "zz.$cmd",
//				Object: bson.D{
//					bson.DocElem{
//						Name:  "dropDatabase",
//						Value: 1,
//					},
//				},
//			},
//		}
//
//		err = RunCommand("zz", "dropDatabase", log, conn.Session)
//		assert.Equal(t, nil, err, "should be equal")
//
//		dbs, err := conn.Session.DatabaseNames()
//		assert.Equal(t, nil, err, "should be equal")
//		exist := false
//		for _, db := range dbs {
//			if db == "zz" {
//				exist = true
//			}
//		}
//		assert.Equal(t, false, exist, "should be equal")
//	}
//
//	// create index
//	{
//		fmt.Printf("TestRunCommand case %d.\n", nr)
//		nr++
//
//		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
//		assert.Equal(t, nil, err, "should be equal")
//
//		err = conn.Session.DB("zz").C("y").Insert(bson.M{"x": 1})
//		assert.Equal(t, nil, err, "should be equal")
//
//		log := &oplog.PartialLog{
//			ParsedLog: oplog.ParsedLog{
//				Operation: "c",
//				Namespace: "zz.$cmd",
//				Object: bson.D{
//					bson.DocElem{
//						Name:  "createIndexes",
//						Value: "y",
//					},
//					bson.DocElem{
//						Name:  "unique",
//						Value: "true",
//					},
//					bson.DocElem{
//						Name:  "v",
//						Value: 2,
//					},
//					bson.DocElem{
//						Name:  "name",
//						Value: "x_1",
//					},
//					bson.DocElem{
//						Name: "key",
//						Value: bson.M{
//							"x": 1,
//						},
//					},
//				},
//			},
//		}
//
//		err = RunCommand("zz", "createIndexes", log, conn.Session)
//		assert.Equal(t, nil, err, "should be equal")
//
//		indexes, err := conn.Session.DB("zz").C("y").Indexes()
//		exist := false
//		for _, index := range indexes {
//			if index.Name == "x_1" && index.Unique == true {
//				exist = true
//				break
//			}
//		}
//		assert.Equal(t, true, exist, "should be equal")
//	}
//}
//
//func TestIgnoreError(t *testing.T) {
//	// test IgnoreError
//
//	var nr int
//
//	// applyOps
//	{
//		fmt.Printf("TestIgnoreError case %d.\n", nr)
//		nr++
//
//		var err error = &mgo.LastError{Code: 26}
//		ignore := IgnoreError(err, "d", false)
//		assert.Equal(t, true, ignore, "should be equal")
//
//		err = &mgo.QueryError{Code: 280}
//		ignore = IgnoreError(err, "d", false)
//		assert.Equal(t, false, ignore, "should be equal")
//	}
//}
