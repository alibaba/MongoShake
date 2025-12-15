package utils

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

const (
	testCollection           = "test"
	testTimeSeriesCollection = "weather"
)

func TestCommonFunctions(t *testing.T) {
	var nr int

	_ = InitialLogger("", "", "debug", true, 1)

	{
		fmt.Printf("TestCommonFunctions case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testMongoAddress, VarMongoConnectModeSecondaryPreferred, true,
			ReadWriteConcernDefault, ReadWriteConcernDefault, "")
		assert.Equal(t, err, nil, "")

		ok, err := GetAndCompareVersion(conn, "3.4.0", "")
		assert.Equal(t, err, nil, "")
		assert.Equal(t, ok, true, "")

		conn.Close()
	}

	{
		fmt.Printf("TestCommonFunctions case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testMongoAddress, VarMongoConnectModeSecondaryPreferred, true,
			ReadWriteConcernDefault, ReadWriteConcernDefault, "")
		assert.Equal(t, err, nil, "")

		ok := conn.IsGood()
		assert.Equal(t, ok, true, "")

		ok = conn.HasOplogNs(bson.M{"type": "collection"})
		assert.Equal(t, ok, true, "")

		name := conn.AcquireReplicaSetName()
		fmt.Printf("ReplicaSetName:%v\n", name)
		assert.NotEqual(t, name, nil, "")

		databases, err := conn.Client.ListDatabaseNames(nil, bson.M{})
		assert.Equal(t, nil, err, "should be equal")
		for _, db := range databases {
			if db != "admin" && db != "local" && db != "config" {
				fmt.Printf("delete database:%v\n", db)
				err = conn.Client.Database(db).Drop(nil)
				assert.Equal(t, nil, err, "should be equal")

			}
		}

		uniqueOk := conn.HasUniqueIndex(bson.M{"type": "collection"})
		assert.Equal(t, uniqueOk, false, "")

		_, err = conn.Client.Database(testDb).Collection(testCollection).InsertOne(context.Background(),
			bson.D{{"x", 11}, {"y", 12}})
		assert.Equal(t, err, nil, "")
		_, err = conn.Client.Database(testDb).Collection(testCollection).InsertOne(context.Background(),
			bson.D{{"x", 21}, {"y", 22}})
		assert.Equal(t, err, nil, "")

		// create unique index
		indexOptions := options.Index().SetUnique(true)
		str, err := conn.Client.Database(testDb).Collection(testCollection).Indexes().CreateOne(
			context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, err, nil, "")
		fmt.Printf("Create index:%v\n", str)

		uniqueOk = conn.HasUniqueIndex(bson.M{"type": "collection"})
		assert.Equal(t, uniqueOk, true, "")
	}

	{
		fmt.Printf("TestCommonFunctions case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(unit_test_common.TestUrl, VarMongoConnectModeSecondaryPreferred, true,
			ReadWriteConcernDefault, ReadWriteConcernDefault, "")
		assert.Equal(t, err, nil, "")

		err = conn.Client.Database(testDb).Drop(context.Background())
		assert.Equal(t, err, nil, "")

		// create normal collection
		_, err = conn.Client.Database(testDb).Collection(testCollection).InsertOne(context.Background(),
			bson.D{{"x", 11}, {"y", 12}})
		assert.Equal(t, err, nil, "")

		result := conn.IsTimeSeriesCollection(testDb, testCollection)
		assert.Equal(t, result, false, "")

		// create time series collection testTimeSeriesCollection
		var cco options.CreateCollectionOptions
		tso := options.TimeSeries()
		tso.SetTimeField("ts")
		tso.SetMetaField("meta")
		tso.SetGranularity("seconds")
		cco.SetTimeSeriesOptions(tso)
		err = conn.Client.Database(testDb).CreateCollection(context.Background(), testTimeSeriesCollection, &cco)
		assert.Equal(t, err, nil, "")

		result = conn.IsTimeSeriesCollection(testDb, testTimeSeriesCollection)
		assert.Equal(t, result, true, "")

		err = conn.Client.Database(testDb).Drop(context.Background())
		assert.Equal(t, err, nil, "")

		conn.Close()
	}
}

func TestEncodeMongoURI(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		err      string
	}{
		{
			input:    "mongodb://root:password001@localhost:27017/admin",
			expected: "mongodb://root:password001@localhost:27017/admin",
			err:      "",
		},
		{
			input:    "mongodb://root:1234@abcd@localhost:27017/admin",
			expected: "mongodb://root:1234%40abcd@localhost:27017/admin",
			err:      "",
		},
		{
			input:    "mongodb://root:dUM3!k&TdofokP0yl1@dds-xxx1.mongodb.rds.aliyuncs.com:3717,dds-xxx2.mongodb.rds.aliyuncs.com:3717",
			expected: "mongodb://root:dUM3%21k&TdofokP0yl1@dds-xxx1.mongodb.rds.aliyuncs.com:3717,dds-xxx2.mongodb.rds.aliyuncs.com:3717",
			err:      "",
		},
		{
			input:    "mongodb://root_special_char:MongoDB@%()!#&-=@localhost:27017/admin",
			expected: "mongodb://root_special_char:MongoDB%40%25%28%29%21%23&-=@localhost:27017/admin",
			err:      "",
		},
		{
			input:    "mongodb://root_special_char1:~!@#$^&*()_-=@localhost:27017/admin",
			expected: "mongodb://root_special_char1:~%21%40%23$%5E&%2A%28%29_-=@localhost:27017/admin",
			err:      "",
		},
		{
			input:    "mongodb://user:pass@localhost/db",
			expected: "mongodb://user:pass@localhost/db",
			err:      "",
		},
		{
			input:    "mongodb://user:pass:@localhost/db",
			expected: "mongodb://user:pass%3A@localhost/db",
			err:      "",
		},
		{
			input:    "mongodb://user:passwd@localhost:27017",
			expected: "mongodb://user:passwd@localhost:27017",
			err:      "",
		},
		{
			input:    "mongodb://localhost:27017,localhost:27018",
			expected: "mongodb://localhost:27017,localhost:27018",
			err:      "",
		},
		{
			input: "invalid://user:pass@host/db",
			err:   "unsupported scheme: invalid",
		},
		{
			input: "mongodb://user@host/db",
			err:   "missing ':' in username:password",
		},
		{
			input: "mongodb://:@host/db",
			err:   "missing username or password in username:password",
		},
	}

	for _, tt := range tests {
		encoded, err := EncodeMongoURI(tt.input)
		if tt.err != "" {
			if err == nil || err.Error() != tt.err {
				t.Errorf("expected error %q for %q, got %v", tt.err, tt.input, err)
			}
		} else {
			if encoded != tt.expected {
				t.Errorf("expected %q for %q, got %q", tt.expected, tt.input, encoded)
			}
		}
	}
}
