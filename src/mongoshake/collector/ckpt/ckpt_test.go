package ckpt

import (
	"fmt"
	"testing"

	"mongoshake/common"
	"mongoshake/collector/configure"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

const (
	testUrl = "mongodb://100.81.164.177:40441,100.81.164.177:40442,100.81.164.177:40443"
)

func TestMongoCheckpoint(t *testing.T) {
	// only test MongoCheckpoint

	var nr int

	// test GetInMemory only
	{
		fmt.Printf("TestMongoCheckpoint case %d.\n", nr)
		nr++

		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageTable = "ut_ckpt_table"
		conf.Options.CheckpointStorage = StorageTypeDB

		name := "ut_tet"
		conn, err := utils.NewMongoConn(testUrl, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Session.DB(CheckpointDefaultDatabase).DropDatabase()
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
		conf.Options.CheckpointStorageTable = "ut_ckpt_table"
		conf.Options.CheckpointStorage = StorageTypeDB

		name := "ut_tet"
		conn, err := utils.NewMongoConn(testUrl, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Session.DB(CheckpointDefaultDatabase).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		// get remote
		ctx, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, exist, "should be equal")
		assert.Equal(t, name, ctx.Name, "should be equal")
		assert.Equal(t, utils.FcvCheckpoint.CurrentVersion, ctx.Version, "should be equal")
		assert.Equal(t, bson.MongoTimestamp(int64(100)<<32), ctx.Timestamp, "should be equal")
		assert.Equal(t, "", ctx.OplogDiskQueue, "should be equal")
		assert.Equal(t, InitCheckpoint, ctx.OplogDiskQueueFinishTs, "should be equal")

		// update
		newTime := bson.MongoTimestamp(int64(200) << 32)
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
		conf.Options.CheckpointStorageTable = "ut_ckpt_table"
		conf.Options.CheckpointStorage = StorageTypeDB

		name := "ut_tet"
		conn, err := utils.NewMongoConn(testUrl, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Session.DB(CheckpointDefaultDatabase).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert remote with startTs == 300
		remoteTime := bson.MongoTimestamp(int64(300) << 32)
		conn.Session.DB(CheckpointDefaultDatabase).C(conf.Options.CheckpointStorageTable).Insert(bson.M{
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
		conf.Options.CheckpointStorageTable = "ut_ckpt_table"
		conf.Options.CheckpointStorage = StorageTypeDB

		name := "ut_tet"
		conn, err := utils.NewMongoConn(testUrl, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Session.DB(CheckpointDefaultDatabase).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		// insert remote with startTs == 300
		remoteTime := bson.MongoTimestamp(int64(300) << 32)
		conn.Session.DB(CheckpointDefaultDatabase).C(conf.Options.CheckpointStorageTable).Insert(bson.M{
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
		updateTime := bson.MongoTimestamp(int64(400) << 32)
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
		conf.Options.CheckpointStorageTable = "ut_ckpt_table"
		conf.Options.CheckpointStorage = StorageTypeDB

		name := "ut_tet"
		conn, err := utils.NewMongoConn(testUrl, utils.ConnectModePrimary, true)
		assert.Equal(t, nil, err, "should be equal")

		// drop test db
		err = conn.Session.DB(CheckpointDefaultDatabase).DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		ckptManager := NewCheckpointManager(name, 100)
		assert.NotEqual(t, nil, ckptManager, "should be equal")

		startTime := bson.MongoTimestamp(int64(100) << 32)

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
		updateTime := bson.MongoTimestamp(int64(200) << 32)
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
		updateTime = bson.MongoTimestamp(int64(300) << 32)
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
		updateTime = bson.MongoTimestamp(int64(400) << 32)
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
		diskFinishTs := bson.MongoTimestamp(int64(450) << 32)
		ckptManager.SetOplogDiskFinishTs(diskFinishTs)

		// update again
		updateTime = bson.MongoTimestamp(int64(500) << 32)
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