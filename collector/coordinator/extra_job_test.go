package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

var (
	testMongoAddress = unit_test_common.TestUrl
)

// Each case gets a private database rather than the shared "test" one. Every package in
// this repo points at the same server, so a test that drops "test" while another reads it
// makes both results meaningless -- and for a job that polls until it sees an index, an
// index that briefly disappears means it never returns at all.
func extraJobFixture(t *testing.T, db string) *utils.MongoCommunityConn {
	t.Helper()

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	assert.Equal(t, nil, err, "should be equal")

	err = conn.Client.Database(db).Drop(context.Background())
	assert.Equal(t, nil, err, "should be equal")

	t.Cleanup(func() {
		_ = conn.Client.Database(db).Drop(context.Background())
		conn.Close()
	})
	return conn
}

func extraJobNamespaces(db string, collections ...string) []string {
	namespaces := make([]string, 0, len(collections))
	for _, collection := range collections {
		namespaces = append(namespaces, db+"."+collection)
	}
	return namespaces
}

func extraJobSources() []*utils.MongoSource {
	return []*utils.MongoSource{{URL: testMongoAddress, ReplicaName: "replica"}}
}

// extraJobCheckOnce drives a single pass of the job and returns when that pass is over,
// which is the property the old innerRun did not have.
func extraJobCheckOnce(t *testing.T, namespaces []string) error {
	t.Helper()

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, utils.VarMongoConnectModeSecondaryPreferred,
		true, utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault, "")
	assert.Equal(t, nil, err, "should be equal")
	t.Cleanup(conn.Close)

	nsList := make([]utils.NS, 0, len(namespaces))
	for _, ns := range namespaces {
		nsList = append(nsList, utils.NewNS(ns))
	}
	return NewCheckUniqueIndexExistsJob(1, namespaces, extraJobSources()).checkOnce(
		[]*utils.MongoCommunityConn{conn}, nsList)
}

// extraJobInnerRun runs the polling job and fails the test instead of hanging it if the
// job does not come back.
func extraJobInnerRun(t *testing.T, job *CheckUniqueIndexExistsJob) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- job.innerRun()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("innerRun did not return")
		return nil
	}
}

func TestExtraJob(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	var nr int

	{
		fmt.Printf("TestExtraJob case %d.\n", nr)
		nr++

		const db = "extra_job_ut0"
		conn := extraJobFixture(t, db)

		// insert data
		_, err := conn.Client.Database(db).Collection("c1").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")
		_, err = conn.Client.Database(db).Collection("c2").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")

		// build index
		indexOptions := options.Index().SetUnique(true)
		_, err = conn.Client.Database(db).Collection("c2").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}},
				Options: indexOptions,
			})
		assert.Equal(t, nil, err, "should be equal")

		err = extraJobCheckOnce(t, extraJobNamespaces(db, "c1", "c2"))
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Contains(t, err.Error(), "c2", "the error should name the offending collection")
		fmt.Printf("checkOnce err:%v\n", err)
	}

	{
		fmt.Printf("TestExtraJob case %d.\n", nr)
		nr++

		const db = "extra_job_ut1"
		conn := extraJobFixture(t, db)

		// a plain index is not the hazard shard_by_object_id creates
		_, err := conn.Client.Database(db).Collection("c1").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")
		_, err = conn.Client.Database(db).Collection("c1").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{Keys: bson.D{{"x", 1}}})
		assert.Equal(t, nil, err, "should be equal")

		assert.Equal(t, nil, extraJobCheckOnce(t, extraJobNamespaces(db, "c1")), "should be equal")
	}

	{
		fmt.Printf("TestExtraJob case %d.\n", nr)
		nr++

		const db = "extra_job_ut2"
		conn := extraJobFixture(t, db)

		// the _id index is unique but is not what the guard is looking for
		_, err := conn.Client.Database(db).Collection("c1").InsertOne(context.Background(),
			bson.M{"x": 1, "y": 2})
		assert.Equal(t, nil, err, "should be equal")

		assert.Equal(t, nil, extraJobCheckOnce(t, extraJobNamespaces(db, "c1")), "should be equal")
	}
}

// innerRun polls for the lifetime of the process, so finding a unique index is the only
// way it can return. That has to stay true: Run() panics on the returned error, and a
// guard that never reports is not a guard.
func TestCheckUniqueIndexExistsJobInnerRunReturnsOnUniqueIndex(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	const db = "extra_job_ut3"
	conn := extraJobFixture(t, db)

	_, err := conn.Client.Database(db).Collection("c2").InsertOne(context.Background(),
		bson.M{"x": 1, "y": 2})
	assert.Equal(t, nil, err, "should be equal")
	_, err = conn.Client.Database(db).Collection("c2").Indexes().CreateOne(context.Background(),
		mongo.IndexModel{
			Keys:    bson.D{{"x", 1}},
			Options: options.Index().SetUnique(true),
		})
	assert.Equal(t, nil, err, "should be equal")

	namespaces := extraJobNamespaces(db, "c2")
	job := NewCheckUniqueIndexExistsJob(1, namespaces, extraJobSources())

	err = extraJobInnerRun(t, job)
	assert.NotEqual(t, nil, err, "should be equal")
	assert.Contains(t, err.Error(), "c2", "the error should name the offending collection")
}

// The job exists to abort an unsafe configuration, so an unreachable source has to
// surface as an error. Returning nil left the guard silently disabled for the whole run.
func TestCheckUniqueIndexExistsJobReportsConnectFailure(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	job := NewCheckUniqueIndexExistsJob(1, extraJobNamespaces("extra_job_ut4", "c1"),
		[]*utils.MongoSource{{URL: "not-a-url", ReplicaName: "replica"}})

	err := extraJobInnerRun(t, job)
	assert.NotEqual(t, nil, err, "connect failure must not silently disable the guard")
	assert.Contains(t, err.Error(), "not-a-url", "the error should name the unreachable source")
}
