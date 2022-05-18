package utils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alibaba/MongoShake/v2/unit_test_common"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testMongoAddress  = unit_test_common.TestUrl
	testUrlServerless = unit_test_common.TestUrlServerlessTenant
	testDb            = "test_db"
)

func TestGetAndCompareVersion(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestGetAndCompareVersion case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testMongoAddress, VarMongoConnectModeSecondaryPreferred, true,
			ReadWriteConcernDefault, ReadWriteConcernDefault, "")
		assert.Equal(t, err, nil, "")

		ok, err := GetAndCompareVersion(conn, "3.4.0", "")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")
	}

	{
		fmt.Printf("TestGetAndCompareVersion case %d.\n", nr)
		nr++

		ok, err := GetAndCompareVersion(nil, "3.4.0", "4.0.1")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		ok, err = GetAndCompareVersion(nil, "3.4.0", "3.4")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		ok, err = GetAndCompareVersion(nil, "3.4.0", "3.2")
		assert.Equal(t, err != nil, true, "")
		assert.Equal(t, ok, false, "")

		ok, err = GetAndCompareVersion(nil, "3.10.0", "3.1")
		assert.Equal(t, err != nil, true, "")
		assert.Equal(t, ok, false, "")

		ok, err = GetAndCompareVersion(nil, "3.6", "4.2.5")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		ok, err = GetAndCompareVersion(nil, "3.2.0", "4.2.7")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		ok, err = GetAndCompareVersion(nil, "4.2.0", "4.2.0")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		ok, err = GetAndCompareVersion(nil, "4.2.0", "4.2")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")
	}
}

func TestGetDbNamespace(t *testing.T) {
	// test GetDbNamespace

	var nr int

	// test drop
	{
		fmt.Printf("TestGetDbNamespace case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testMongoAddress, VarMongoConnectModePrimary, true,
			ReadWriteConcernLocal, ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Client.Database("db1").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Client.Database("db2").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database("db1").Collection("c1").InsertOne(nil, bson.M{"x": 1})
		conn.Client.Database("db1").Collection("c2").InsertOne(nil, bson.M{"x": 1})
		conn.Client.Database("db1").Collection("c3").InsertOne(nil, bson.M{"x": 1})
		conn.Client.Database("db2").Collection("c1").InsertOne(nil, bson.M{"x": 1})
		conn.Client.Database("db2").Collection("c4").InsertOne(nil, bson.M{"x": 1})

		filterFunc := func(name string) bool {
			list := strings.Split(name, ".")
			if len(list) > 0 && (list[0] == "db1" || list[0] == "db2") {
				return false
			}
			return true
		}
		nsList, nsMap, err := GetDbNamespace(testMongoAddress, filterFunc, "")
		fmt.Println(nsList, nsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(nsList), "should be equal")
		assert.Equal(t, 2, len(nsMap), "should be equal")
		assert.Equal(t, 3, len(nsMap["db1"]), "should be equal")
		assert.Equal(t, 2, len(nsMap["db2"]), "should be equal")
	}

	{
		fmt.Printf("TestGetDbNamespace case %d.\n", nr)
		nr++

		// drop all old table
		conn, err := NewMongoCommunityConn(testUrlServerless, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")
		err = conn.Client.Database(testDb).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index1, err := conn.Client.Database(testDb).Collection("c1").Indexes().
			CreateOne(context.Background(), mongo.IndexModel{
				Keys:    bson.D{{"x", 1}, {"y", 1}},
				Options: &options.IndexOptions{},
			})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index2, err := conn.Client.Database(testDb).Collection("c1").Indexes().
			CreateOne(context.Background(), mongo.IndexModel{
				Keys:    bson.D{{"wwwww", 1}},
				Options: &options.IndexOptions{},
			})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index3, err := conn.Client.Database(testDb).Collection("c2").Indexes().
			CreateOne(context.Background(), mongo.IndexModel{
				Keys:    bson.D{{"hello", "hashed"}},
				Options: &options.IndexOptions{},
			})
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database(testDb).Collection("c3").InsertOne(context.Background(),
			map[string]interface{}{"x": 1})
		assert.Equal(t, nil, err, "should be equal")

		fmt.Println(index1, index2, index3)

		filterFunc := func(name string) bool {
			list := strings.Split(name, ".")
			if len(list) > 0 && list[0] == testDb {
				return false
			}
			return true
		}
		nsList, nsMap, err := GetDbNamespace(testUrlServerless, filterFunc, "")
		fmt.Println(nsList, nsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(nsList), "should be equal")
		assert.Equal(t, 1, len(nsMap), "should be equal")
		assert.Equal(t, 3, len(nsMap[testDb]), "should be equal")
	}
}

func TestGetAllNamespace(t *testing.T) {
	var nr int

	InitialLogger("", "", "debug", true, 1)

	{
		fmt.Printf("TestGetAllNamespace case %d.\n", nr)
		nr++

		mgoSources := []*MongoSource{
			&MongoSource{
				URL:         testMongoAddress,
				ReplicaName: "replica",
			},
		}

		tsMap, biggestNew, smallestNew, biggestOld, smallestOld, err := GetAllTimestamp(mgoSources, "")
		assert.Equal(t, nil, err, "should be equal")
		fmt.Printf("TestGetAllNamespace biggestNew:%v, smallestNew:%v, biggestOld:%v, smallestOld:%v,"+
			", tsMap:%v\n",
			Int64ToTimestamp(biggestNew), Int64ToTimestamp(smallestNew),
			Int64ToTimestamp(biggestOld), Int64ToTimestamp(smallestOld), tsMap)
	}
}
