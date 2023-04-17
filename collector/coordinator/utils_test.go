package coordinator

import (
	"fmt"
	"testing"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"

	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

const (
	testUrl           = unit_test_common.TestUrl
	testUrlServerless = unit_test_common.TestUrlServerlessTenant
	testDb            = "test_db3"
	testCollection    = "test_ut"
)

func TestSelectSyncMode(t *testing.T) {
	// only test selectSyncMode

	testSelectSyncMode = true

	utils.InitialLogger("", "", "info", true, 1)

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
				First:  int64(10 << 32),
				Second: int64(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// insert
		ckptManager := ckpt.NewCheckpointManager(testReplicaName, 0)
		assert.Equal(t, true, ckptManager != nil, "should be equal")

		ckptManager.Get()

		err = ckptManager.Update(int64(5 << 32))
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

		err = ckptManager.Update(int64(15 << 32))
		assert.Equal(t, nil, err, "should be equal")
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(15<<32), startTsMap[testReplicaName], "should be equal")
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
				First:  int64(10 << 32),
				Second: int64(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// insert
		ckptManager := ckpt.NewCheckpointManager(testReplicaName, 0)
		assert.Equal(t, true, ckptManager != nil, "should be equal")

		ckptManager.Get()

		// full sync
		err = ckptManager.Update(int64(5 << 32))
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
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run sync mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		// incr sync
		err = ckptManager.Update(int64(50 << 32))
		assert.Equal(t, nil, err, "should be equal")

		checkpoint, exist, err := ckptManager.Get()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, exist, "should be equal")
		assert.Equal(t, int64(50<<32), checkpoint.Timestamp, "should be equal")

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
		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)

		conf.Options.CheckpointStartPosition = 3
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		conf.Options.CheckpointStartPosition = 20
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Printf("startTsMap: %v\n", startTsMap)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")
	}

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
				First:  int64(10 << 32),
				Second: int64(100 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
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
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run sync_mode incr
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		conf.Options.CheckpointStartPosition = 50

		// run
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")

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
				First:  int64(10 << 32),
				Second: int64(100 << 32),
			},
			"mockReplicaSet2": {
				First:  int64(20 << 32),
				Second: int64(101 << 32),
			},
			"mockReplicaSet3": {
				First:  int64(30 << 32),
				Second: int64(102 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet1
		ckptManager1 := ckpt.NewCheckpointManager("mockReplicaSet1", 0)
		assert.Equal(t, true, ckptManager1 != nil, "should be equal")

		ckptManager1.Get()
		err = ckptManager1.Update(int64(20 << 32))
		assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet2
		ckptManager2 := ckpt.NewCheckpointManager("mockReplicaSet2", 0)
		assert.Equal(t, true, ckptManager2 != nil, "should be equal")

		ckptManager2.Get()
		err = ckptManager2.Update(int64(25 << 32))
		assert.Equal(t, nil, err, "should be equal")

		// insert mockReplicaSet3
		ckptManager3 := ckpt.NewCheckpointManager("mockReplicaSet3", 0)
		assert.Equal(t, true, ckptManager3 != nil, "should be equal")

		ckptManager3.Get()
		err = ckptManager3.Update(int64(20 << 32))
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
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run, return all
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, true, err != nil, "should be equal")

		// run, return incr
		err = ckptManager3.Update(int64(35 << 32))
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
		conn, err = utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)

		conf.Options.CheckpointStartPosition = 45
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")

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
				First:  int64(10 << 32),
				Second: int64(100 << 32),
			},
			"mockReplicaSet2": {
				First:  int64(20 << 32),
				Second: int64(101 << 32),
			},
			"mockReplicaSet3": {
				First:  int64(30 << 32),
				Second: int64(102 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// insert mockMongoS
		ckptManager4 := ckpt.NewCheckpointManager("mockMongoS", 0)
		assert.Equal(t, true, ckptManager4 != nil, "should be equal")

		ckptManager4.Get()
		err = ckptManager4.Update(int64(10 << 32))
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
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run, return all
		err = ckptManager4.Update(int64(20 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run, return all
		err = ckptManager4.Update(int64(30 << 32))
		assert.Equal(t, nil, err, "should be equal")

		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")

		// run, return incr
		err = ckptManager4.Update(int64(35 << 32))
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
		conn, err = utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		ckptManager5 := ckpt.NewCheckpointManager("mockMongoS", 0)
		assert.Equal(t, true, ckptManager5 != nil, "should be equal")

		ckptManager5.Get()

		conf.Options.CheckpointStartPosition = 55
		mode, startTsMap, ts, err = coordinator.selectSyncMode(utils.VarSyncModeAll)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, startTsMap == nil, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, mode, "should be equal")
		assert.Equal(t, int64(100<<32), ts, "should be equal")
	}

	// test sharding with fetch_method = "change_stream" with no checkpoint exist and kafka tunnel
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrl
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.CheckpointStartPosition = 1

		// mock GetAllTimestampInUT input map
		utils.GetAllTimestampInUTInput = map[string]utils.Pair{
			"mockReplicaSet1": {
				First:  int64(10 << 32),
				Second: int64(80 << 32),
			},
			"mockReplicaSet2": {
				First:  int64(20 << 32),
				Second: int64(101 << 32),
			},
			"mockReplicaSet3": {
				First:  int64(30 << 32),
				Second: int64(102 << 32),
			},
		}

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrl, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// no checkpoint pre-exist
		ckptManager4 := ckpt.NewCheckpointManager("mockMongoS", 0)
		assert.Equal(t, true, ckptManager4 != nil, "should be equal")

		/*ckptManager4.Get()
		err = ckptManager4.Update(int64(10 << 32))
		assert.Equal(t, nil, err, "should be equal")*/

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
		mode, startTsMap, ts, err := coordinator.selectSyncMode(utils.VarSyncModeIncr)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, len(startTsMap) >= 1, "should be equal")
		assert.Equal(t, int64(1)<<32, startTsMap["mockMongoS"], "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, mode, "should be equal")
		assert.Equal(t, int64(0), ts, "should be equal")
	}

	// aliyun_serverless, no-checkpoint
	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.Tunnel = utils.VarTunnelKafka

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrlServerless
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.SpecialSourceDBFlag = utils.VarSpecialSourceDBFlagAliyunServerless

		testReplicaName := "mockReplicaSet"

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrlServerless, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			RealSourceFullSync: []*utils.MongoSource{
				{
					URL:         testUrlServerless,
					ReplicaName: testReplicaName,
				},
			},
		}

		// run
		syncMode, _, fullBeginTs, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Println(syncMode, fullBeginTs)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, utils.VarSyncModeAll, syncMode, "should be equal")
		assert.Equal(t, true, len(fullBeginTs.(bson.Raw)) > 0, "should be equal")
	}

	{
		fmt.Printf("TestSelectSyncMode case %d.\n", nr)
		nr++

		conf.Options.Tunnel = utils.VarTunnelKafka

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.CheckpointStorageUrl = testUrlServerless
		conf.Options.CheckpointStorageDb = testDb
		conf.Options.CheckpointStorageCollection = testCollection
		conf.Options.CheckpointStorage = utils.VarCheckpointStorageDatabase
		conf.Options.SpecialSourceDBFlag = utils.VarSpecialSourceDBFlagAliyunServerless

		testReplicaName := "mockReplicaSet"

		// drop old table
		conn, err := utils.NewMongoCommunityConn(testUrlServerless, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")

		conn.Client.Database(testDb).Collection(testCollection).Drop(nil)
		// assert.Equal(t, nil, err, "should be equal")

		// insert
		ckptManager := ckpt.NewCheckpointManager(testReplicaName, 0)
		assert.Equal(t, true, ckptManager != nil, "should be equal")

		ckptManager.Get()

		err = ckptManager.Update(int64(5 << 32))
		assert.Equal(t, nil, err, "should be equal")

		coordinator := &ReplicationCoordinator{
			RealSourceIncrSync: []*utils.MongoSource{
				{
					URL:         testUrlServerless,
					ReplicaName: testReplicaName,
				},
			},
			RealSourceFullSync: []*utils.MongoSource{
				{
					URL:         testUrlServerless,
					ReplicaName: testReplicaName,
				},
			},
		}

		// run
		syncMode, _, fullBeginTs, err := coordinator.selectSyncMode(utils.VarSyncModeAll)
		fmt.Println(syncMode, fullBeginTs)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, utils.VarSyncModeIncr, syncMode, "should be equal")
		assert.Equal(t, int64(5<<32), fullBeginTs, "should be equal")
	}
}

func TestFetchIndexes(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	var nr int

	{
		fmt.Printf("TestFetchIndexes case %d.\n", nr)
		nr++

		url := testUrlServerless
		sourceList := []*utils.MongoSource{
			{
				ReplicaName: "test",
				URL:         url,
			},
		}

		// drop all old table
		conn, err := utils.NewMongoCommunityConn(url, "primary", true, "", "", "")
		assert.Equal(t, nil, err, "should be equal")
		conn.Client.Database(testDb).Drop(nil)

		// create index
		index1, err := conn.Client.Database(testDb).Collection("c1").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"x", 1}, {"y", 1}},
				Options: &options.IndexOptions{},
			})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index2, err := conn.Client.Database(testDb).Collection("c1").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"wwwww", 1}},
				Options: &options.IndexOptions{},
			})
		assert.Equal(t, nil, err, "should be equal")

		// create index
		index3, err := conn.Client.Database(testDb).Collection("c2").Indexes().CreateOne(context.Background(),
			mongo.IndexModel{
				Keys:    bson.D{{"hello", "hashed"}},
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

		out, err := fetchIndexes(sourceList, filterFunc)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(out), "should be equal")
		assert.Equal(t, 3, len(out[utils.NS{Database: testDb, Collection: "c1"}]), "should be equal")
		assert.Equal(t, 2, len(out[utils.NS{Database: testDb, Collection: "c2"}]), "should be equal")
		assert.Equal(t, 1, len(out[utils.NS{Database: testDb, Collection: "c3"}]), "should be equal")
		fmt.Println(out)
	}
}
