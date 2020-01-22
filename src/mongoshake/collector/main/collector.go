// +build darwin linux windows

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/executor"
	"mongoshake/modules"
	"mongoshake/oplog"
	"mongoshake/quorum"
)

type Exit struct{ Code int }

func main() {
	var err error
	defer handleExit()
	defer LOG.Close()
	defer utils.Goodbye(func() bool {
		return conf.Options.SyncMode != collector.SyncModeDocument
	})

	// argument options
	configuration := flag.String("conf", "", "configure file absolute path")
	verbose := flag.Bool("verbose", false, "show logs on console")
	version := flag.Bool("version", false, "show version")
	flag.Parse()

	if *configuration == "" || *version == true {
		fmt.Println(utils.BRANCH)
		panic(Exit{0})
	}

	var file *os.File
	if file, err = os.Open(*configuration); err != nil {
		crash(fmt.Sprintf("Configure file open failed. %v", err), -1)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		crash(fmt.Sprintf("Configure file %s parse failed. %v", *configuration, err), -2)
	}

	// verify collector options and revise
	if err = sanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	if err := utils.InitialLogger(conf.Options.LogDirectory, conf.Options.LogFileName, conf.Options.LogLevel, conf.Options.LogBuffer, *verbose); err != nil {
		crash(fmt.Sprintf("initial log.dir[%v] log.name[%v] failed[%v].", conf.Options.LogDirectory,
			conf.Options.LogFileName, err), -2)
	}

	conf.Options.Version = utils.BRANCH

	nimo.Profiling(conf.Options.SystemProfile)
	signalProfile, _ := strconv.Atoi(utils.SIGNALPROFILE)
	signalStack, _ := strconv.Atoi(utils.SIGNALSTACK)
	if signalProfile > 0 {
		nimo.RegisterSignalForProfiling(syscall.Signal(signalProfile)) // syscall.SIGUSR2
		nimo.RegisterSignalForPrintStack(syscall.Signal(signalStack), func(bytes []byte) { // syscall.SIGUSR1
			LOG.Info(string(bytes))
		})
	}

	utils.Welcome()

	// get exclusive process lock and write pid
	if utils.WritePidById("", conf.Options.CollectorId) {
		startup()
	}
}

func startup() {
	// leader election at the beginning
	selectLeader()

	// initialize http api
	utils.InitHttpApi(conf.Options.HTTPListenPort)

	utils.HttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return &conf.Options
	})

	coordinator := &collector.ReplicationCoordinator{
		Sources: make([]*utils.MongoSource, len(conf.Options.MongoUrls)),
	}
	// start mongodb replication
	if err := coordinator.Run(); err != nil {
		// initial or connection established failed
		crash(fmt.Sprintf("Oplog Tailer initialize failed: %v", err), -6)
	}

	// if the sync mode is "document", mongoshake should exit here.
	if conf.Options.SyncMode != collector.SyncModeDocument {
		if err := utils.HttpApi.Listen(); err != nil {
			LOG.Critical("Coordinator http api listen failed. %v", err)
		}
	}
}

func selectLeader() {
	// first of all. ensure we are the Master
	if conf.Options.MasterQuorum && conf.Options.ContextStorage == collector.StorageTypeDB {
		// election become to Master. keep waiting if we are the candidate. election id is must fixed
		quorum.UseElectionObjectId(bson.ObjectIdHex("5204af979955496907000001"))
		go quorum.BecomeMaster(conf.Options.ContextStorageUrl, utils.AppDatabase())

		// wait until become to a real master
		<-quorum.MasterPromotionNotifier
	} else {
		quorum.AlwaysMaster()
	}
}

func sanitizeOptions() error {
	// compatible with old version
	if len(conf.Options.LogFileNameOld) != 0 {
		conf.Options.LogFileName = conf.Options.LogFileNameOld
	}
	if len(conf.Options.LogLevelOld) != 0 {
		conf.Options.LogLevel = conf.Options.LogLevelOld
	}
	if conf.Options.LogBufferOld == true {
		conf.Options.LogBuffer = conf.Options.LogBufferOld
	}
	if len(conf.Options.LogFileName) == 0 {
		return fmt.Errorf("log.name[%v] shouldn't be empty", conf.Options.LogFileName)
	}

	if len(conf.Options.MongoUrls) == 0 {
		return errors.New("mongo_urls were empty")
	} else if len(conf.Options.MongoUrls) > 1 {
		if conf.Options.WorkerNum != len(conf.Options.MongoUrls) {
			//LOG.Warn("replication worker should be equal to count of mongo_urls while multi sources (shard), set worker = %v",
			//	len(conf.Options.MongoUrls))
			conf.Options.WorkerNum = len(conf.Options.MongoUrls)
		}
		if conf.Options.MongoCsUrl == "" {
			return errors.New("config server url should be configured when transfer from mongo sharding")
		}
	} else {
		if conf.Options.MongoCsUrl != "" {
			return errors.New("config server url should not be configured when transfer from mongo replica set")
		}
	}
	if conf.Options.ContextStorageUrl == "" {
		if len(conf.Options.MongoUrls) == 1 {
			conf.Options.ContextStorageUrl = conf.Options.MongoUrls[0]
		} else if len(conf.Options.MongoUrls) > 1 {
			return errors.New("checkpoint url should be configured when transfer from mongo sharding")
		}
	}

	if conf.Options.ReplayerExecutorUpsert == true {
		if len(conf.Options.MongoUrls) > 1 {
			return errors.New("replayer.executor.upsert should be set false when transfer from mongo sharding")
		}
	}

	if conf.Options.ReplayerExecutorInsertOnDupUpdate == true {
		if len(conf.Options.MongoUrls) > 1 {
			return errors.New("replayer.executor.insert_on_dup_update should be set false when transfer from mongo sharding")
		}
	}

	// avoid the typo of mongo urls
	if utils.HasDuplicated(conf.Options.MongoUrls) {
		return errors.New("mongo urls were duplicated")
	}
	if conf.Options.CollectorId == "" {
		return errors.New("collector id should not be empty")
	}
	if conf.Options.HTTPListenPort <= 1024 && conf.Options.HTTPListenPort > 0 {
		return errors.New("http listen port too low numeric")
	}
	if conf.Options.CheckpointInterval < 0 {
		return errors.New("checkpoint interval is negative")
	} else if conf.Options.CheckpointInterval  == 0 {
		conf.Options.CheckpointInterval = 10 // set default to 10 seconds
	}
	if conf.Options.MoveChunkInterval < 0 {
		return errors.New("move chunk interval is negative")
	} else if conf.Options.MoveChunkInterval  == 0 {
		conf.Options.MoveChunkInterval = 10 // set default to 10 seconds
	}
	if conf.Options.ShardKey != oplog.ShardByNamespace &&
		conf.Options.ShardKey != oplog.ShardByID &&
		conf.Options.ShardKey != oplog.ShardAutomatic {
		return errors.New("shard key type is unknown")
	}
	if conf.Options.SyncerReaderBufferTime == 0 {
		conf.Options.SyncerReaderBufferTime = 1
	}
	if conf.Options.WorkerNum <= 0 || conf.Options.WorkerNum > 256 {
		return errors.New("worker numeric is not valid")
	}
	if conf.Options.WorkerBatchQueueSize <= 0 {
		return errors.New("worker queue numeric is negative")
	}
	if conf.Options.ContextStorage == "" || conf.Options.ContextStorageDB == "" ||
		conf.Options.ContextStorageCollection == "" ||
		(conf.Options.ContextStorage != collector.StorageTypeAPI &&
			conf.Options.ContextStorage != collector.StorageTypeDB) {
		return errors.New("context storage type or address is invalid")
	}
	if conf.Options.WorkerOplogCompressor != module.CompressionNone &&
		conf.Options.WorkerOplogCompressor != module.CompressionGzip &&
		conf.Options.WorkerOplogCompressor != module.CompressionZlib &&
		conf.Options.WorkerOplogCompressor != module.CompressionDeflate {
		return errors.New("compressor is not supported")
	}
	if conf.Options.MasterQuorum && conf.Options.ContextStorage != collector.StorageTypeDB {
		return errors.New("context storage should set to 'database' while master election enabled")
	}
	if len(conf.Options.FilterNamespaceBlack) != 0 &&
		len(conf.Options.FilterNamespaceWhite) != 0 {
		return errors.New("at most one of black lists and white lists option can be given")
	}

	conf.Options.HTTPListenPort = utils.MayBeRandom(conf.Options.HTTPListenPort)
	conf.Options.SystemProfile = utils.MayBeRandom(conf.Options.SystemProfile)

	if conf.Options.Tunnel == "" {
		return errors.New("tunnel is empty")
	}
	if len(conf.Options.TunnelAddress) == 0 && conf.Options.Tunnel != "mock" {
		return errors.New("tunnel address is illegal")
	}
	if conf.Options.SyncMode == "" {
		conf.Options.SyncMode = "oplog" // default
	}

	// judge the replayer configuration when tunnel type is "direct"
	if conf.Options.Tunnel == "direct" {
		if len(conf.Options.TunnelAddress) > conf.Options.WorkerNum {
			return errors.New("then length of tunnel_address with type 'direct' shouldn't bigger than worker number")
		}
		if conf.Options.ReplayerExecutor <= 0 {
			conf.Options.ReplayerExecutor = 1
			// return errors.New("executor number should be large than 1")
		}
		if conf.Options.ReplayerConflictWriteTo != executor.DumpConflictToDB &&
			conf.Options.ReplayerConflictWriteTo != executor.DumpConflictToSDK &&
			conf.Options.ReplayerConflictWriteTo != executor.NoDumpConflict {
			return errors.New("collision write strategy is neither db nor sdk nor none")
		}
		conf.Options.ReplayerCollisionEnable = conf.Options.ReplayerExecutor != 1
	} else {
		if conf.Options.SyncMode != "oplog" {
			return errors.New("document replication only support direct tunnel type")
		}
	}

	if (conf.Options.Tunnel != "file" && conf.Options.Tunnel != "kafka") &&
		conf.Options.TunnelMessage != utils.TunnelMessageRaw {
		return fmt.Errorf("tunnel.message should be 'raw' if tunnel type is not 'kafka' or 'file'")
	}

	if conf.Options.SyncMode != "oplog" && conf.Options.SyncMode != "document" && conf.Options.SyncMode != "all" {
		return fmt.Errorf("unknown sync_mode[%v]", conf.Options.SyncMode)
	}

	if conf.Options.MongoConnectMode != utils.ConnectModePrimary &&
		conf.Options.MongoConnectMode != utils.ConnectModeSecondaryPreferred &&
		conf.Options.MongoConnectMode != utils.ConnectModeStandalone {
		return fmt.Errorf("unknown mongo_connect_mode[%v]", conf.Options.MongoConnectMode)
	}

	if conf.Options.LogDirectory == "" {
		conf.Options.LogDirectory = "logs"
	}
	return nil
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			os.Exit(exit.Code)
		}
		panic(e)
	}
}
