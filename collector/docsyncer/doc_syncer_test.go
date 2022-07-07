package docsyncer

import (
	"fmt"
	"github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	"github.com/alibaba/MongoShake/v2/collector/transform"
	"github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/sharding"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	testMongoAddress           = unit_test_common.TestUrl
	testMongoAddressServerless = unit_test_common.TestUrlServerlessTenant
	testDb                     = "test_db"
	testCollection             = "test_coll"
)

var (
	testNs = strings.Join([]string{testDb, testCollection}, ".")
)

func marshalData(input []bson.D) []*bson.Raw {
	output := make([]*bson.Raw, 0, len(input))
	for _, ele := range input {
		if data, err := bson.Marshal(ele); err != nil {
			return nil
		} else {
			var dataRaw bson.Raw
			dataRaw = data[:]
			output = append(output, &dataRaw)
		}
	}
	return output
}

func fetchAllDocument(conn *utils.MongoCommunityConn) ([]bson.D, error) {
	return unit_test_common.FetchAllDocumentbsonD(conn.Client, testDb, testCollection, nil)
}

func TestDbSync(t *testing.T) {
	// test doSync

	utils.InitialLogger("", "", "debug", true, 1)
	conf.Options.LogLevel = "debug"

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModePrimary, false,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	assert.Equal(t, nil, err, "should be equal")

	// init DocExecutor, ignore DBSyncer here
	var meaningless int64 = 0
	de := NewDocExecutor(0, &CollectionExecutor{
		ns: utils.NS{Database: testDb, Collection: testCollection},
	}, conn, &DBSyncer{
		qos: utils.StartQoS(0, 1, &meaningless),
	})
	assert.NotEqual(t, nil, de.syncer, "should be equal")
	assert.NotEqual(t, nil, de.syncer.qos, "should be equal")

	var nr int

	// test "full_sync.executor.insert_on_dup_update"
	{
		fmt.Printf("TestDbSync case %d.\n", nr)
		nr++

		// drop db
		err := conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		input := []bson.D{
			{
				{"_id", 1},
				{"x", 1},
			},
			{
				{"_id", 2},
				{"x", 2},
			},
		}

		inputMarshal := marshalData(input)
		assert.NotEqual(t, nil, inputMarshal, "should be equal")

		err = de.doSync(inputMarshal)
		assert.Equal(t, nil, err, "should be equal")

		// fetch result
		output, err := fetchAllDocument(conn)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 2, len(output), "should be equal")
		for _, ele := range output {
			var idVal, xVal int32
			if ele[0].Key == "_id" {
				idVal = ele[0].Value.(int32)
				xVal = ele[1].Value.(int32)
			} else {
				idVal = ele[1].Value.(int32)
				xVal = ele[0].Value.(int32)
			}

			assert.Equal(t, xVal, idVal, "should be equal")
		}

		/*------------------------------------------------------------*/

		// insert duplicate document
		input = []bson.D{
			{
				{"_id", 3},
				{"x", 3},
			},
			{
				{"_id", 2},
				{"x", 20},
			},
			{
				{"_id", 4},
				{"x", 4},
			},
		}

		inputMarshal = marshalData(input)
		assert.NotEqual(t, nil, inputMarshal, "should be equal")

		err = de.doSync(inputMarshal)
		fmt.Println(err)
		assert.NotEqual(t, nil, err, "should be equal")

		conf.Options.FullSyncExecutorInsertOnDupUpdate = true
		err = de.doSync(inputMarshal)
		assert.Equal(t, nil, err, "should be equal")

		// fetch result
		output, err = fetchAllDocument(conn)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(output), "should be equal")
		for _, ele := range output {
			var idVal, xVal int32
			if ele[0].Key == "_id" {
				idVal = ele[0].Value.(int32)
				xVal = ele[1].Value.(int32)
			} else {
				idVal = ele[1].Value.(int32)
				xVal = ele[0].Value.(int32)
			}

			if idVal != 2 {
				assert.Equal(t, xVal, idVal, "should be equal")
			} else {
				assert.Equal(t, int32(20), xVal, "should be equal")
			}
		}
	}

	// test "full_sync.executor.filter.orphan_document"
	{
		fmt.Printf("TestDbSync case %d.\n", nr)
		nr++

		// set orphan filter
		of := filter.NewOrphanFilter("test-replica", sharding.DBChunkMap{
			fmt.Sprintf("%s.%s", testDb, testCollection): &sharding.ShardCollection{
				Chunks: []*sharding.ChunkRange{
					{
						Mins: []interface{}{
							1,
						},
						Maxs: []interface{}{
							10,
						},
					},
					{
						Mins: []interface{}{
							50,
						},
						Maxs: []interface{}{
							100,
						},
					},
				},
				Keys:      []string{"x"},
				ShardType: sharding.RangedShard,
			},
		})
		dbSyncer := &DBSyncer{
			orphanFilter: of,
			qos:          utils.StartQoS(0, 1, &meaningless),
		}
		de.syncer = dbSyncer

		// drop db
		err := conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		input := []bson.D{
			{
				{"_id", 1},
				{"x", 1},
			},
			{
				{"_id", 11},
				{"x", 11},
			},
			{
				{"_id", 4},
				{"x", 4},
			},
		}

		inputMarshal := marshalData(input)
		assert.NotEqual(t, nil, inputMarshal, "should be equal")

		err = de.doSync(inputMarshal)
		assert.Equal(t, nil, err, "should be equal")

		// fetch result
		output, err := fetchAllDocument(conn)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(output), "should be equal")
		for _, ele := range output {
			var idVal, xVal int32
			if ele[0].Key == "_id" {
				idVal = ele[0].Value.(int32)
				xVal = ele[1].Value.(int32)
			} else {
				idVal = ele[1].Value.(int32)
				xVal = ele[0].Value.(int32)
			}

			assert.Equal(t, xVal, idVal, "should be equal")
		}

		/*------------------------------------------------------------*/

		conf.Options.FullSyncExecutorInsertOnDupUpdate = false
		conf.Options.FullSyncExecutorFilterOrphanDocument = true
		conf.Options.MongoUrls = []string{"xx0", "xx1"} // meaningless but only for judge
		input = []bson.D{
			{
				{"_id", 7},
				{"x", 7},
			},
			{
				{"_id", 11},
				{"x", 12},
			},
			{
				{"_id", 6},
				{"x", 6},
			},
		}

		inputMarshal = marshalData(input)
		assert.NotEqual(t, nil, inputMarshal, "should be equal")

		err = de.doSync(inputMarshal)
		assert.Equal(t, nil, err, "should be equal")

		// fetch result
		output, err = fetchAllDocument(conn)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(output), "should be equal")
		for _, ele := range output {
			var idVal, xVal int32
			if ele[0].Key == "_id" {
				idVal = ele[0].Value.(int32)
				xVal = ele[1].Value.(int32)
			} else {
				idVal = ele[1].Value.(int32)
				xVal = ele[0].Value.(int32)
			}

			assert.Equal(t, xVal, idVal, "should be equal")
		}
	}
}

func TestStartDropDestCollection(t *testing.T) {
	// test StartDropDestCollection

	var nr int

	// test drop
	{
		fmt.Printf("TestStartDropDestCollection case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop old db
		err = conn.Client.Database("test").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// create test.c1
		_, err = conn.Client.Database("test").Collection("c1").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		// create test.c2
		_, err = conn.Client.Database("test").Collection("c2").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		// create test2.c3
		_, err = conn.Client.Database("test").Collection("c3").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		nsSet := map[utils.NS]struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c1"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c4"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c5"}] = struct{}{}

		conf.Options.FullSyncCollectionDrop = true
		nsTrans := transform.NewNamespaceTransform([]string{"test.c4:test.c3"})

		err = StartDropDestCollection(nsSet, conn, nsTrans)
		assert.Equal(t, nil, err, "should be equal")

		list, err := conn.Client.Database("test").ListCollectionNames(nil, bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, len(list), "should be equal")
		assert.Equal(t, "c2", list[0], "should be equal")

	}

	// test no drop
	{
		fmt.Printf("TestStartDropDestCollection case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop old db
		err = conn.Client.Database("test").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// create test.c1
		_, err = conn.Client.Database("test").Collection("c1").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		// create test.c2
		_, err = conn.Client.Database("test").Collection("c2").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		// create test2.c3
		_, err = conn.Client.Database("test").Collection("c3").
			InsertOne(nil, bson.D{{"c", 1}})
		assert.Equal(t, nil, err, "should be equal")

		nsSet := map[utils.NS]struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c1"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c4"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection: "c5"}] = struct{}{}

		conf.Options.FullSyncCollectionDrop = false
		nsTrans := transform.NewNamespaceTransform([]string{"test.c4:test.c3"})

		err = StartDropDestCollection(nsSet, conn, nsTrans)
		assert.Equal(t, nil, err, "should be equal")

		list, err := conn.Client.Database("test").ListCollectionNames(nil, bson.M{"type": "collection"})
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(list), "should be equal")
		sort.Strings(list)
		assert.Equal(t, "c1", list[0], "should be equal")
		assert.Equal(t, "c2", list[1], "should be equal")
		assert.Equal(t, "c3", list[2], "should be equal")
	}
}

func TestStartIndexSync(t *testing.T) {
	// test StartIndexSync

	var nr int

	utils.InitialLogger("", "", "info", true, 1)

	// test drop
	{
		fmt.Printf("TestStartIndexSync case %d.\n", nr)
		nr++

		conf.Options.FullSyncReaderCollectionParallel = 4

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop old db
		err = conn.Client.Database("test_db").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		indexInput := []bson.D{
			{
				{"key", bson.D{{"_id", int32(1)}}},
				{"name", "_id_"},
			},
			{
				{"key", bson.D{{"hello", "hashed"}}},
				{"name", "hello_hashed"},
			},
			{
				{"key", bson.D{
					{"x", int32(1)},
					{"y", int32(1)},
				}},
				{"name", "x_1_y_1"},
			},
			{
				{"key", bson.D{{"z", int32(1)}}},
				{"name", "z_1"},
			},
		}
		indexMap := map[utils.NS][]bson.D{
			utils.NS{"test_db", "test_coll"}: indexInput,
		}
		err = StartIndexSync(indexMap, testMongoAddress, nil, true)
		assert.Equal(t, nil, err, "should be equal")

		cursor, err := conn.Client.Database("test_db").Collection("test_coll").Indexes().List(nil)
		assert.Equal(t, nil, err, "should be equal")

		indexes := make([]bson.D, 0)

		cursor.All(nil, &indexes)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, len(indexes), len(indexInput), "should be equal")
		isEqual(indexInput, indexes, t)
	}

	// serverless deprecate
	//{
	//	fmt.Printf("TestStartIndexSync case %d.\n", nr)
	//	nr++
	//
	//	conf.Options.FullSyncReaderCollectionParallel = 4
	//
	//	conn, err := utils.NewMongoCommunityConn(testMongoAddressServerless, utils.VarMongoConnectModePrimary, true,
	//		utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, "")
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	// drop old db
	//	err = conn.Client.Database("test_db").Drop(nil)
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	indexInput := []bson.M{
	//		{
	//			"key": bson.M{
	//				"_id": int32(1),
	//			},
	//			"name": "_id_",
	//			//"ns":   "test_db.test_coll",
	//		},
	//		{
	//			"key": bson.M{
	//				"hello": "hashed",
	//			},
	//			"name": "hello_hashed",
	//			//"ns":   "test_db.test_coll",
	//		},
	//		{
	//			"key": bson.M{
	//				"x": int32(1),
	//				"y": int32(1),
	//			},
	//			"name": "x_1_y_1",
	//			//"ns":   "test_db.test_coll",
	//		},
	//		{
	//			"key": bson.M{
	//				"z": int32(1),
	//			},
	//			"name": "z_1",
	//			//"ns":   "test_db.test_coll",
	//		},
	//	}
	//	indexMap := map[utils.NS][]bson.M{
	//		utils.NS{"test_db", "test_coll"}: indexInput,
	//	}
	//	err = StartIndexSync(indexMap, testMongoAddressServerless, nil, true)
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	cursor, err := conn.Client.Database("test_db").Collection("test_coll").Indexes().List(nil)
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	indexes := make([]bson.M, 0)
	//
	//	cursor.All(nil, &indexes)
	//	assert.Equal(t, nil, err, "should be equal")
	//	assert.Equal(t, len(indexes), len(indexInput), "should be equal")
	//	assert.Equal(t, isEqual(indexInput, indexes), true, "should be equal")
	//}
}

func isEqual(x, y []bson.D, t *testing.T) {
	assert.Equal(t, len(x), len(y), "should be equal")

	for i := 0; i < len(x); i += 1 {
		xKey := oplog.GetKey(x[i], "key")
		yKey := oplog.GetKey(y[i], "key")
		xName := oplog.GetKey(x[i], "name")
		yName := oplog.GetKey(y[i], "name")

		assert.Equal(t, xKey, yKey, "should be equal")
		assert.Equal(t, xName, yName, "should be equal")
	}
}

func removeField(x []bson.M) {
	for _, ele := range x {
		delete(ele, "v")
		delete(ele, "ns")
	}
}
