package ckpt

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

func TestHttpApiCheckpointInsertRejectsNonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	operation := &HttpApiCheckpoint{URL: server.URL}
	err := operation.Insert(&CheckpointContext{Name: "rs-http", Timestamp: 100})
	assert.Error(t, err)
}

func TestCheckpointManagerUpdateFailureDoesNotMutateInMemory(t *testing.T) {
	insertErr := errors.New("insert failed")
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:                   "rs-atomic",
			Timestamp:              100,
			Version:                1,
			FetchMethod:            "oplog",
			OplogDiskQueue:         "spool-old",
			OplogDiskQueueFinishTs: InitCheckpoint,
		},
		delegate: &stubCheckpointOperation{insertErr: insertErr},
	}
	manager.SetFetchMethod("change_stream")
	manager.SetOplogDiskQueueName("spool-new")
	manager.SetOplogDiskFinishTs(200)

	err := manager.Update(300)
	assert.ErrorIs(t, err, insertErr)
	assert.Equal(t, int64(100), manager.ctx.Timestamp, "should be equal")
	assert.Equal(t, 1, manager.ctx.Version, "should be equal")
	assert.Equal(t, "oplog", manager.ctx.FetchMethod, "should be equal")
	assert.Equal(t, "spool-old", manager.ctx.OplogDiskQueue, "should be equal")
	assert.Equal(t, InitCheckpoint, manager.ctx.OplogDiskQueueFinishTs, "should be equal")
}

func TestCheckpointManagerUpdatePreservesFieldsThatWereNotStaged(t *testing.T) {
	manager := &CheckpointManager{
		ctx: &CheckpointContext{
			Name:                   "rs-preserve",
			Timestamp:              100,
			Version:                1,
			FetchMethod:            "oplog",
			OplogDiskQueue:         "spool-old",
			OplogDiskQueueFinishTs: InitCheckpoint,
		},
		delegate: &stubCheckpointOperation{},
	}
	manager.SetOplogDiskQueueName("spool-new")

	err := manager.Update(200)
	assert.NoError(t, err)
	assert.Equal(t, int64(200), manager.ctx.Timestamp, "should be equal")
	assert.Equal(t, "oplog", manager.ctx.FetchMethod, "should be equal")
	assert.Equal(t, "spool-new", manager.ctx.OplogDiskQueue, "should be equal")
	assert.Equal(t, InitCheckpoint, manager.ctx.OplogDiskQueueFinishTs, "should be equal")
}

type stubCheckpointOperation struct {
	insertErr error
}

func (s *stubCheckpointOperation) Get() (*CheckpointContext, bool) {
	return nil, false
}

func (s *stubCheckpointOperation) Insert(*CheckpointContext) error {
	return s.insertErr
}

func (s *stubCheckpointOperation) String() string {
	return "stub"
}

var (
	testUrl = unit_test_common.TestUrl
)

func TestMongoCheckpoint(t *testing.T) {
	// only test MongoCheckpoint

	var nr int

	// test GetInMemory only
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageCollection = "ut_ckpt_table"
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase

		name := "ut_tet"
		conn, err := utils.NewMongoCommunityConn(testUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		ctx := ckptManager.GetInMemory()
		assert.Equal(t, true, ctx == nil, "should be equal")
	}

	// test get & update & get
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageCollection = "ut_ckpt_table"
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStorageDb = utils.VarCheckpointStorageDbReplicaDefault

		name := "ut_tet"
		conn, err := utils.NewMongoCommunityConn(testUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		// get remote
		ctx, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, utils.FcvCheckpoint.CurrentVersion, ctx.Version, "should be equal")
		assert.Equal(t, int64(100), ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")
		assert.Equal(t, InitCheckpoint, ctx.OplogDiskQueueFinishTs, "should be equal")

		// update
		newTime := int64(200)
		err = ckptManager.Update(newTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, utils.FcvCheckpoint.CurrentVersion, ctx.Version, "should be equal")
		assert.Equal(t, newTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")
		assert.Equal(t, InitCheckpoint, ctx.OplogDiskQueueFinishTs, "should be equal")
	}

	// test insert remote with incompatible version & get
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageCollection = "ut_ckpt_table"
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStorageDb = utils.VarCheckpointStorageDbReplicaDefault

		name := "ut_tet"
		conn, err := utils.NewMongoCommunityConn(testUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// insert remote with startTs == 300
		remoteTime := int64(300)
		conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).
			Collection(conf.Options.CheckpointStorageCollection).
			InsertOne(nil, bson.M{
				"name":                             name,
				"ckpt":                             remoteTime,
				"oplog_disk_queue":                 "",
				"oplog_disk_queue_apply_finish_ts": nil,
				"version":                          0,
			})

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		// get remote
		_, _, err = ckptManager.Get()
		// version not compatible
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// test insert remote & get & update & get
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageCollection = "ut_ckpt_table"
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStorageDb = utils.VarCheckpointStorageDbReplicaDefault
		utils.FcvCheckpoint.CurrentVersion = 1

		name := "ut_tet"
		conn, err := utils.NewMongoCommunityConn(testUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		// insert remote with startTs == 300
		remoteTime := int64(300)
		conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).
			Collection(conf.Options.CheckpointStorageCollection).
			InsertOne(nil, bson.M{
				"name":                             name,
				"ckpt":                             remoteTime,
				"oplog_disk_queue":                 "",
				"oplog_disk_queue_apply_finish_ts": nil,
				"version":                          1,
			})

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		// get remote
		ctx, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, remoteTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")
		// assert.Equal(t, InitCheckpoint, ctx.OplogDiskQueueFinishTs, "should be equal")

		// update with 400
		updateTime := int64(400)
		err = ckptManager.Update(updateTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, updateTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")
		// assert.Equal(t, InitCheckpoint, ctx.OplogDiskQueueFinishTs, "should be equal")
	}

	// test get & SetOplogDiskQueueName + SetOplogDiskFinishTs & get
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageCollection = "ut_ckpt_table"
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStorageDb = utils.VarCheckpointStorageDbReplicaDefault
		utils.FcvCheckpoint.CurrentVersion = 1

		name := "ut_tet"
		conn, err := utils.NewMongoCommunityConn(testUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernMajority, "")
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Client.Database(utils.VarCheckpointStorageDbReplicaDefault).Drop(nil)
		assert.Equal(t, nil, err, "should be equal")

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		startTime := int64(100)

		// get remote
		ctx, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, startTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")

		// call SetOplogDiskQueueName
		ckptManager.SetOplogDiskQueueName("ut_test_disk_queue_name")
		ctx = ckptManager.GetInMemory()
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, startTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")

		// the SetOplogDiskQueueName won't take effect until Update called
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, startTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")

		// update
		updateTime := int64(200)
		ckptManager.Update(updateTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, updateTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "ut_test_disk_queue_name", ctx.OplogDiskQueue, "should be equal")

		// call SetOplogDiskQueueName
		ckptManager.SetOplogDiskQueueName("ut_test_disk_queue_name_2")

		// update again
		updateTime = int64(300)
		ckptManager.Update(updateTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, updateTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "ut_test_disk_queue_name_2", ctx.OplogDiskQueue, "should be equal")

		// update again, test ctx.OplogDiskQueue is not clear
		updateTime = int64(400)
		ckptManager.Update(updateTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, updateTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "ut_test_disk_queue_name_2", ctx.OplogDiskQueue, "should be equal")

		// call SetOplogDiskFinishTs
		diskFinishTs := int64(450)
		ckptManager.SetOplogDiskFinishTs(diskFinishTs)

		// update again
		updateTime = int64(500)
		ckptManager.Update(updateTime)

		// get again
		ctx, exist, err = ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, 1, ctx.Version, "should be equal")
		assert.Equal(t, updateTime, ctx.Timestamp, "should be equal")
		assert.Equal(t, "ut_test_disk_queue_name_2", ctx.OplogDiskQueue, "should be equal")
		assert.Equal(t, diskFinishTs, ctx.OplogDiskQueueFinishTs, "should be equal")
	}
}
