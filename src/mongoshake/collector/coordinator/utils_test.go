package coordinator

import (
	"testing"
	"fmt"

	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/unit_test_common"
	"mongoshake/collector/ckpt"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

const (
	testUrl        = unit_test_common.TestUrl
	testDb         = "test_db"
	testCollection = "test_ut"
)

func TestSelectSyncMode(t *testing.T) {
	// only test selectSyncMode

	testSelectSyncMode = true

	utils.InitialLogger("", "", "info", true, true)

	var nr int

	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.Tunnel = utils.VarTunnelKafka

		coordinator := &ReplicationCoordinator{}
		syncMode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeFull)
		assert.Equal(t, utils.VarSyncModeFull, syncMode, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
		assert.Equal(t, true, err == nil, "should be equal")
	}

	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.Tunnel = utils.VarTunnelKafka

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase

		testReplicaName := "mockReplicaSet"
		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			testReplicaName: {
				First:  bson.MongoTimestamp(10 << 32),
				Second: bson.MongoTimestamp(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()
		// assert.Equal(t, nil, err, "should be equal")

		// insert
		ckptManager := ckpt.NewCheckpointManager(testReplicaName, 0)
		assert.Equal(t, true, ckptManager != nil, "should be equal")

		ckptManager.Get()

		err = ckptManager.Update(bson.MongoTimestamp(5 << 32))
		assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			MongoD: []*utils.MongoSource{
				{
					URL:         "1.1.1.1",
					ReplicaName: testReplicaName,
				},
			},
		}

		// run
		_, _, _, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, true, err != nil, "should be equal")

		err = ckptManager.Update(bson.MongoTimestamp(15 << 32))
		assert.Equal(t, nil, err, "should be equal")
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(15 << 32)), startTsMap[testReplicaName], "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
	}

	conf.Options.Tunnel = utils.VarTunnelDirect

	// sync_mode != "all"
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		coordinator := &ReplicationCoordinator{}
		syncMode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeFull)
		assert.Equal(t, utils.VarSyncModeFull, syncMode, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
		assert.Equal(t, true, err == nil, "should be equal")
	}

	// test replica set with fetch_method = "change_stream"
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase

		testReplicaName := "mockReplicaSet"
		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			testReplicaName: {
				First:  bson.MongoTimestamp(10 << 32),
				Second: bson.MongoTimestamp(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()
		// assert.Equal(t, nil, err, "should be equal")

		// insert
		ckptManager := ckpt.NewCheckpointManager(testReplicaName, 0)
		assert.Equal(t, true, ckptManager != nil, "should be equal")

		ckptManager.Get()

		// full sync
		err = ckptManager.Update(bson.MongoTimestamp(5 << 32))
		assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			MongoD: []*utils.MongoSource{
				{
					URL:         "1.1.1.1",
					ReplicaName: testReplicaName,
				},
			},
		}

		// run
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run sync mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		// incr sync
		err = ckptManager.Update(bson.MongoTimestamp(50 << 32))
		assert.Equal(t, nil, err, "should be equal")

		checkpoint, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, bson.MongoTimestamp(50<<32), checkpoint.Timestamp, "should be equal")

		// run
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			testReplicaName: int64(50) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// run sync mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			testReplicaName: int64(50) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// run with no checkpoint
		conn.Session.DB(testDb).C(testCollection).DropCollection()

		conf.Options.CheckpointStartPosition = 3
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		conf.Options.CheckpointStartPosition = 20
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")
	}
	return

	// test replica set with fetch_method = "oplog" and no checkpoint exists
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStartPosition = 5

		testReplicaName := "mockReplicaSet"
		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			testReplicaName: {
				First:  bson.MongoTimestamp(10 << 32),
				Second: bson.MongoTimestamp(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()
		// assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			MongoD: []*utils.MongoSource{
				{
					URL:         "1.1.1.1",
					ReplicaName: testReplicaName,
				},
			},
		}

		// run
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run sync_mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		conf.Options.CheckpointStartPosition = 50

		// run
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			testReplicaName: int64(50) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// run sync_mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			testReplicaName: int64(50) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		syncMode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeFull)
		assert.Equal(t, utils.VarSyncModeFull, syncMode, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
		assert.Equal(t, true, err == nil, "should be equal")
	}

	// test sharding with fetch_method = "oplog"
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase

		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			"mockReplicaSet1": {
				First:  bson.MongoTimestamp(10 << 32),
				Second: bson.MongoTimestamp(100 << 32),
			},
			"mockReplicaSet2": {
				First:  bson.MongoTimestamp(20 << 32),
				Second: bson.MongoTimestamp(101 << 32),
			},
			"mockReplicaSet3": {
				First:  bson.MongoTimestamp(30 << 32),
				Second: bson.MongoTimestamp(102 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()
		// assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet1
		ckptManager1 := ckpt.NewCheckpointManager("mockReplicaSet1", 0)
		assert.Equal(t, true, ckptManager1 != nil, "should be equal")

		ckptManager1.Get()
		err = ckptManager1.Update(bson.MongoTimestamp(20 << 32))
		assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet2
		ckptManager2 := ckpt.NewCheckpointManager("mockReplicaSet2", 0)
		assert.Equal(t, true, ckptManager2 != nil, "should be equal")

		ckptManager2.Get()
		err = ckptManager2.Update(bson.MongoTimestamp(25 << 32))
		assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet3
		ckptManager3 := ckpt.NewCheckpointManager("mockReplicaSet3", 0)
		assert.Equal(t, true, ckptManager3 != nil, "should be equal")

		ckptManager3.Get()
		err = ckptManager3.Update(bson.MongoTimestamp(20 << 32))
		assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			MongoD: []*utils.MongoSource{
				{
					URL:         "1.1.1.1",
					ReplicaName: "mockReplicaSet1",
				},
				{
					URL:         "2.2.2.2",
					ReplicaName: "mockReplicaSet2",
				},
				{
					URL:         "3.3.3.3",
					ReplicaName: "mockReplicaSet3",
				},
			},
		}

		// run, return all
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run, return all
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		// run, return incr
		err = ckptManager3.Update(bson.MongoTimestamp(35 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			"mockReplicaSet1": int64(20) << 32,
			"mockReplicaSet2": int64(25) << 32,
			"mockReplicaSet3": int64(35) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// test on checkpoint set

		// drop old table
		conn, err = utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()

		conf.Options.CheckpointStartPosition = 45
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			"mockReplicaSet1": int64(45) << 32,
			"mockReplicaSet2": int64(45) << 32,
			"mockReplicaSet3": int64(45) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// run sync_mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			"mockReplicaSet1": int64(45) << 32,
			"mockReplicaSet2": int64(45) << 32,
			"mockReplicaSet3": int64(45) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
	}

	// test sharding with fetch_method = "change_stream"
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase

		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			"mockReplicaSet1": {
				First:  bson.MongoTimestamp(10 << 32),
				Second: bson.MongoTimestamp(100 << 32),
			},
			"mockReplicaSet2": {
				First:  bson.MongoTimestamp(20 << 32),
				Second: bson.MongoTimestamp(101 << 32),
			},
			"mockReplicaSet3": {
				First:  bson.MongoTimestamp(30 << 32),
				Second: bson.MongoTimestamp(102 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()
		// assert.Equal(t, nil, err, "should be equal")

		// insert mockMongoS
		ckptManager4 := ckpt.NewCheckpointManager("mockMongoS", 0)
		assert.Equal(t, true, ckptManager4 != nil, "should be equal")

		ckptManager4.Get()
		err = ckptManager4.Update(bson.MongoTimestamp(10 << 32))
		assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			MongoD: []*utils.MongoSource{
				{
					URL:         "1.1.1.1",
					ReplicaName: "mockReplicaSet1",
				},
				{
					URL:         "2.2.2.2",
					ReplicaName: "mockReplicaSet2",
				},
				{
					URL:         "3.3.3.3",
					ReplicaName: "mockReplicaSet3",
				},
			},
			MongoS: &utils.MongoSource{
				URL:         "100.100.100.100",
				ReplicaName: "mockMongoS",
			},
		}

		// run, return all
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run, return all
		err = ckptManager4.Update(bson.MongoTimestamp(20 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run, return all
		err = ckptManager4.Update(bson.MongoTimestamp(30 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(bson.MongoTimestamp(100<<32)), ts, "should be equal")

		// run, return incr
		err = ckptManager4.Update(bson.MongoTimestamp(35 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			"mockMongoS":      int64(35) << 32,
			"mockReplicaSet1": int64(35) << 32,
			"mockReplicaSet2": int64(35) << 32,
			"mockReplicaSet3": int64(35) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")

		// test on checkpoint set

		// drop old table
		conn, err = utils.NewMongoConn(testUrl, "primary", true, "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB(testDb).C(testCollection).DropCollection()

		conf.Options.CheckpointStartPosition = 55
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[string]int64{
			"mockMongoS":      int64(55) << 32,
			"mockReplicaSet1": int64(55) << 32,
			"mockReplicaSet2": int64(55) << 32,
			"mockReplicaSet3": int64(55) << 32,
		}, startTsMap, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
	}
}
