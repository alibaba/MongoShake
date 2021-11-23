package utils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alibaba/MongoShake/v2/unit_test_common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"github.com/vinllen/mongo-go-driver/mongo"
	"github.com/vinllen/mongo-go-driver/mongo/options"
)

const (
	testMongoAddress  = unit_test_common.TestUrl
	testUrlServerless = unit_test_common.TestUrlServerlessTenant
	testDb            = "test_db"
)

// deprecated
func TestAdjustDBRef(t *testing.T) {
	return
	var nr int
	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": 1,
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{}

		output := AdjustDBRef(input, true)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  "1234",
				"$ref": "a.b",
			},
		}

		output := AdjustDBRef(input, false)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  1234,
				"$ref": "a.b",
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "$ref", output["a"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "a.b", output["a"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["a"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 1234, output["a"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["a"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "xxx", output["a"].(bson.D)[2].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  1234,
				"$ref": "a.b",
			},
			"b": bson.M{
				"c": bson.M{
					"$id":  5678,
					"$db":  "yyy",
					"$ref": "c.d",
				},
				"d": "hello-world",
				"e": bson.M{
					"$id":  910,
					"$db":  "zzz",
					"$ref": "e.f",
				},
				"f": 1,
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "$ref", output["a"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "a.b", output["a"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["a"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 1234, output["a"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["a"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "xxx", output["a"].(bson.D)[2].Value, "should be equal")

		assert.Equal(t, "hello-world", output["b"].(bson.M)["d"], "should be equal")
		assert.Equal(t, 1, output["b"].(bson.M)["f"], "should be equal")

		assert.Equal(t, "$ref", output["b"].(bson.M)["c"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "c.d", output["b"].(bson.M)["c"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["b"].(bson.M)["c"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 5678, output["b"].(bson.M)["c"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["b"].(bson.M)["c"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "yyy", output["b"].(bson.M)["c"].(bson.D)[2].Value, "should be equal")

		assert.Equal(t, "$ref", output["b"].(bson.M)["e"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "e.f", output["b"].(bson.M)["e"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["b"].(bson.M)["e"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 910, output["b"].(bson.M)["e"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["b"].(bson.M)["e"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "zzz", output["b"].(bson.M)["e"].(bson.D)[2].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"p": bson.D{
				{
					Name: "x",
					Value: bson.M{
						"$id":    10,
						"$db":    "zzz",
						"$ref":   "www",
						"others": "aaa",
						"fuck":   "hello",
					},
				},
				{
					Name: "y",
					Value: bson.M{
						"$id":    20,
						"$db":    "po",
						"$ref":   "po2",
						"others": "bbb",
						"fuck":   "world",
					},
				},
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "x", output["p"].(bson.D)[0].Name, "should be equal")

		assert.Equal(t, "$ref", output["p"].(bson.D)[0].Value.(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "www", output["p"].(bson.D)[0].Value.(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["p"].(bson.D)[0].Value.(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 10, output["p"].(bson.D)[0].Value.(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["p"].(bson.D)[0].Value.(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "zzz", output["p"].(bson.D)[0].Value.(bson.D)[2].Value, "should be equal")
		assert.Equal(t, "others", output["p"].(bson.D)[0].Value.(bson.D)[3].Name, "should be equal")
		assert.Equal(t, "aaa", output["p"].(bson.D)[0].Value.(bson.D)[3].Value, "should be equal")
		assert.Equal(t, "fuck", output["p"].(bson.D)[0].Value.(bson.D)[4].Name, "should be equal")
		assert.Equal(t, "hello", output["p"].(bson.D)[0].Value.(bson.D)[4].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"ts": 1560588963,
			"t":  8,
			"h":  -4461630918490158108,
			"v":  2,
			"op": "u",
			"ns": "test.zzz",
			"o2": bson.M{
				"_id": "5d04b02c27d5888ce0224fc8",
			},
			"o": bson.M{
				"_id": "5d04b02c27d5888ce0224fc8",
				"b":   7,
				"user": bson.M{
					"$ref": "xxx",
					"$id":  "40b6d79e507b2c613615f15d",
				},
			},
		}

		output := AdjustDBRef(input["o"].(bson.M), true)

		assert.Equal(t, "$ref", output["user"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "xxx", output["user"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["user"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, "40b6d79e507b2c613615f15d", output["user"].(bson.D)[1].Value, "should be equal")

	}
}

func TestGetAndCompareVersion(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestGetAndCompareVersion case %d.\n", nr)
		nr++

		conn, err := NewMongoConn(testMongoAddress, VarMongoConnectModeSecondaryPreferred, true,
			ReadWriteConcernDefault, ReadWriteConcernDefault, "")
		assert.Equal(t, err, nil, "")

		ok, err := GetAndCompareVersion(conn.Session, "3.4.0", "")
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

		conn, err := NewMongoConn(testMongoAddress, VarMongoConnectModePrimary, true,
			ReadWriteConcernLocal, ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Session.DB("db1").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Session.DB("db2").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB("db1").C("c1").Insert(bson.M{"x": 1})
		conn.Session.DB("db1").C("c2").Insert(bson.M{"x": 1})
		conn.Session.DB("db1").C("c3").Insert(bson.M{"x": 1})
		conn.Session.DB("db2").C("c1").Insert(bson.M{"x": 1})
		conn.Session.DB("db2").C("c4").Insert(bson.M{"x": 1})

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
		conn.Client.Database(testDb).Drop(nil)

		// create index
		index1, err := conn.Client.Database(testDb).Collection("c1").Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson2.D{{"x", 1}, {"y", 1}},
			Options: &options.IndexOptions{},
		})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index2, err := conn.Client.Database(testDb).Collection("c1").Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson2.D{{"wwwww", 1}},
			Options: &options.IndexOptions{},
		})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index3, err := conn.Client.Database(testDb).Collection("c2").Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson2.D{{"hello", "hashed"}},
			Options: &options.IndexOptions{},
		})
		assert.Equal(t, nil, err, "should be equal")

		_, err = conn.Client.Database(testDb).Collection("c3").InsertOne(context.Background(), map[string]interface{}{"x": 1})
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
