package coordinator

import (
	"context"
	"fmt"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

const (
	testMongoAddress = unit_test_common.TestUrl
)

func TestExtraJob(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	var nr int

	{
		fmt.Printf("TestExtraJob case %d.\n", nr)
		nr++

		mongoSources := []*utils.MongoSource{
			{
				URL:         testMongoAddress,
				ReplicaName: "replica",
			},
		}
		mongoCollections := []string{"test.c1", "test.c2"}

		conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true, utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop database
		err = conn.Client.Database("test").Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// insert data
		_, err = conn.Client.Database("test").Collection("c1").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")
		_, err = conn.Client.Database("test").Collection("c2").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")

		// build index
		indexOptions := options.Index().SetUnique(true)
		_, err = conn.Client.Database("test").Collection("c2").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, nil, err, "should be equal")

		job := NewCheckUniqueIndexExistsJob(1, mongoCollections, mongoSources)
		err = job.innerRun()
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Printf("innerRun err:%v\n", err)
	}
}
