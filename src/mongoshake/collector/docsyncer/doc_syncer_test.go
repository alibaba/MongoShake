package docsyncer

import (
	"fmt"
	"testing"
	"strings"
	"sort"

	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/sharding"
	"mongoshake/collector/transform"
	"mongoshake/unit_test_common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

const (
	testMongoAddress = unit_test_common.TestUrl
	testDb           = "a"
	testCollection   = "b"
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
			output = append(output, &bson.Raw{
				Kind: 3,
				Data: data,
			})
		}
	}
	return output
}

func fetchAllDocument(conn *utils.MongoConn) ([]bson.D, error) {
	it := conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Iter()
	doc := new(bson.Raw)
	result := make([]bson.D, 0)
	for it.Next(doc) {
		var docD bson.D
		if err := bson.Unmarshal(doc.Data, &docD); err != nil {
			return nil, err
		}
		result = append(result, docD)
	}
	return result, nil
}

func TestDbSync(t *testing.T) {
	// test doSync

	conn, err := utils.NewMongoConn(testMongoAddress, utils.VarMongoConnectModePrimary, false,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault)
	assert.Equal(t, nil, err, "should be equal")

	// init DocExecutor, ignore DBSyncer here
	var meaningless int64 = 0
	de := NewDocExecutor(0, &CollectionExecutor{
		ns: utils.NS{Database: testDb, Collection: testCollection},
	}, conn.Session, &DBSyncer{
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
		err := conn.Session.DB(testDb).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		input := []bson.D {
			{
				{
					Name: "_id",
					Value: 1,
				},
				{
					Name: "x",
					Value: 1,
				},
			},
			{
				{
					Name: "_id",
					Value: 2,
				},
				{
					Name: "x",
					Value: 2,
				},
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
			var idVal, xVal int
			if ele[0].Name == "_id" {
				idVal = ele[0].Value.(int)
				xVal = ele[1].Value.(int)
			} else {
				idVal = ele[1].Value.(int)
				xVal = ele[0].Value.(int)
			}

			assert.Equal(t, xVal, idVal, "should be equal")
		}

		/*------------------------------------------------------------*/

		// insert duplicate document
		input = []bson.D {
			{
				{
					Name: "_id",
					Value: 3,
				},
				{
					Name: "x",
					Value: 3,
				},
			},
			{ // duplicate key with different value
				{
					Name: "_id",
					Value: 2,
				},
				{
					Name: "x",
					Value: 20,
				},
			},
			{
				{
					Name: "_id",
					Value: 4,
				},
				{
					Name: "x",
					Value: 4,
				},
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
			var idVal, xVal int
			if ele[0].Name == "_id" {
				idVal = ele[0].Value.(int)
				xVal = ele[1].Value.(int)
			} else {
				idVal = ele[1].Value.(int)
				xVal = ele[0].Value.(int)
			}

			if idVal != 2 {
				assert.Equal(t, xVal, idVal, "should be equal")
			} else {
				assert.Equal(t, 20, xVal, "should be equal")
			}
		}
	}

	// test "full_sync.executor.filter.orphan_document"
	{
		fmt.Printf("TestDbSync case %d.\n", nr)
		nr++

		// set orphan filter
		of := filter.NewOrphanFilter("test-replica", sharding.DBChunkMap{
			"a.b": &sharding.ShardCollection{
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
		err := conn.Session.DB(testDb).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		input := []bson.D {
			{
				{
					Name: "_id",
					Value: 1,
				},
				{
					Name: "x",
					Value: 1,
				},
			},
			{ // not in current chunks,
				{
					Name: "_id",
					Value: 11,
				},
				{
					Name: "x",
					Value: 11,
				},
			},
			{
				{
					Name: "_id",
					Value: 4,
				},
				{
					Name: "x",
					Value: 4,
				},
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
			var idVal, xVal int
			if ele[0].Name == "_id" {
				idVal = ele[0].Value.(int)
				xVal = ele[1].Value.(int)
			} else {
				idVal = ele[1].Value.(int)
				xVal = ele[0].Value.(int)
			}

			assert.Equal(t, xVal, idVal, "should be equal")
		}

		/*------------------------------------------------------------*/

		conf.Options.FullSyncExecutorInsertOnDupUpdate = false
		conf.Options.FullSyncExecutorFilterOrphanDocument = true
		conf.Options.MongoUrls = []string{"xx0", "xx1"} // meaningless but only for judge
		input = []bson.D {
			{
				{
					Name: "_id",
					Value: 7,
				},
				{
					Name: "x",
					Value: 7,
				},
			},
			{ // not in current chunks,
				{
					Name: "_id",
					Value: 11,
				},
				{
					Name: "x",
					Value: 12,
				},
			},
			{
				{
					Name: "_id",
					Value: 6,
				},
				{
					Name: "x",
					Value: 6,
				},
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
			var idVal, xVal int
			if ele[0].Name == "_id" {
				idVal = ele[0].Value.(int)
				xVal = ele[1].Value.(int)
			} else {
				idVal = ele[1].Value.(int)
				xVal = ele[0].Value.(int)
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

		conn, err := utils.NewMongoConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernMajority)
		assert.Equal(t, nil, err, "should be equal")

		// drop old db
		err = conn.Session.DB("test").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// create test.c1
		err = conn.Session.DB("test").C("c1").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		// create test.c2
		err = conn.Session.DB("test").C("c2").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		// create test2.c3
		err = conn.Session.DB("test").C("c3").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		nsSet := map[utils.NS]struct{}{}
		nsSet[utils.NS{Database: "test", Collection:"c1"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection:"c4"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection:"c5"}] = struct{}{}

		conf.Options.FullSyncCollectionDrop = true
		nsTrans := transform.NewNamespaceTransform([]string{"test.c4:test.c3"})

		err = StartDropDestCollection(nsSet, conn, nsTrans)
		assert.Equal(t, nil, err, "should be equal")

		list, err := conn.Session.DB("test").CollectionNames()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1, len(list), "should be equal")
		assert.Equal(t, "c2", list[0], "should be equal")
		// sort.Strings(list)

	}

	// test no drop
	{
		fmt.Printf("TestStartDropDestCollection case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernMajority)
		assert.Equal(t, nil, err, "should be equal")

		// drop old db
		err = conn.Session.DB("test").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// create test.c1
		err = conn.Session.DB("test").C("c1").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		// create test.c2
		err = conn.Session.DB("test").C("c2").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		// create test2.c3
		err = conn.Session.DB("test").C("c3").Insert(bson.M{"c":1})
		assert.Equal(t, nil, err, "should be equal")

		nsSet := map[utils.NS]struct{} {}
		nsSet[utils.NS{Database: "test", Collection:"c1"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection:"c4"}] = struct{}{}
		nsSet[utils.NS{Database: "test", Collection:"c5"}] = struct{}{}

		conf.Options.FullSyncCollectionDrop = false
		nsTrans := transform.NewNamespaceTransform([]string{"test.c4:test.c3"})

		err = StartDropDestCollection(nsSet, conn, nsTrans)
		assert.Equal(t, nil, err, "should be equal")

		list, err := conn.Session.DB("test").CollectionNames()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(list), "should be equal")
		sort.Strings(list)
		assert.Equal(t, "c1", list[0], "should be equal")
		assert.Equal(t, "c2", list[1], "should be equal")
		assert.Equal(t, "c3", list[2], "should be equal")
	}
}
