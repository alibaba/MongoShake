package utils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

var (
	testMongoAddress  = unit_test_common.TestUrl
	testUrlServerless = unit_test_common.TestUrlServerlessTenant
)

const (
	testDb = "test_db"
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

func TestListTimeSeriesNamespaces(t *testing.T) {
	conn, err := NewMongoCommunityConn(testMongoAddress, VarMongoConnectModePrimary, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()
	ctx := context.Background()

	const db = "ts_scan_test"
	_ = conn.Client.Database(db).Drop(ctx)
	// A time-series database left behind holds a dangling view definition, which makes
	// the server reject listCollections for that whole database and so breaks unrelated
	// tests that run afterwards.
	defer func() { _ = conn.Client.Database(db).Drop(ctx) }()

	err = conn.Client.Database(db).RunCommand(ctx, bson.D{
		{Key: "create", Value: "gps"},
		{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "ts"}}},
	}).Err()
	if err != nil {
		t.Skipf("server cannot create time-series collections (needs 5.0+): %v", err)
	}
	_, err = conn.Client.Database(db).Collection("plain").InsertOne(ctx, bson.M{"_id": 1})
	assert.Equal(t, nil, err, "insert into the ordinary control collection")

	// The server is shared, so other databases may hold time-series collections of their
	// own. Assert on membership rather than on the whole list.
	got, err := ListTimeSeriesNamespaces(conn, nil)
	assert.Equal(t, nil, err, "list time-series namespaces")
	assert.Contains(t, got, db+".gps", "the time-series collection is reported by its logical name")
	assert.NotContains(t, got, db+".plain", "an ordinary collection is not time-series")
	assert.NotContains(t, got, db+".system.buckets.gps",
		"the backing bucket collection is not reported separately")

	// filterFunc keeps only what it accepts, matching GetDbNamespace's convention
	kept, err := ListTimeSeriesNamespaces(conn, func(ns string) bool { return ns == db+".something-else" })
	assert.Equal(t, nil, err, "list time-series namespaces with a filter")
	assert.NotContains(t, kept, db+".gps", "a namespace the filter rejects must not be reported")
}
