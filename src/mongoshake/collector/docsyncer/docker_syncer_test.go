package docsyncer

import (
	"fmt"
	"testing"
	"strings"

	"mongoshake/common"
	"mongoshake/collector/configure"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/filter"
	"mongoshake/sharding"
)

const (
	testMongoAddress = "mongodb://127.0.0.1:40441,127.0.0.1:40442,127.0.0.1:40443"
	testDb           = "a"
	testCollection   = "b"
)

var (
	testNs = strings.Join([]string{testDb, testCollection}, ".")
)

func TestDbSync(t *testing.T) {
	// test doSync

	conn, err := utils.NewMongoConn(testMongoAddress, utils.ConnectModePrimary, false)
	assert.Equal(t, nil, err, "should be equal")

	// init DocExecutor, ignore DBSyncer here
	de := NewDocExecutor(0, &CollectionExecutor{
		ns: utils.NS{Database: testDb, Collection: testCollection},
	}, conn.Session, nil)

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