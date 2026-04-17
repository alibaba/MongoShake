//go:build darwin || linux || windows
// +build darwin linux windows

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"syscall"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/coordinator"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/quorum"
)

type Exit struct{ Code int }

func main() {
	var err error
	defer handleExit()
	defer func() {
		if l.Logger != nil {
			_ = l.Logger.Sync()
		}
	}()

	// argument options
	configuration := flag.String("conf", "", "configure file absolute path")
	verbose := flag.Int("verbose", 0, "where log goes to: 0 - file，1 - file+stdout，2 - stdout")
	GCPercent := flag.Int("GCPercent", 100, "golang GC percent")
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
	defer func() {
		_ = file.Close()
	}()

	// read fcv and do comparison
	if _, err := conf.CheckFcv(*configuration, utils.FcvConfiguration.FeatureCompatibleVersion); err != nil {
		crash(err.Error(), -5)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		crash(fmt.Sprintf("Configure file %s parse failed. %v", *configuration, err), -2)
	}

	// verify collector options and revise
	if err = SanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	if err := utils.InitialLoggerWithRotation(conf.Options.LogDirectory,
		conf.Options.LogFileName, conf.Options.LogLevel, conf.Options.LogFlush,
		*verbose, conf.Options.LogMaxSizeMb, conf.Options.LogMaxAge); err != nil {
		crash(fmt.Sprintf("initial log.dir[%v] log.name[%v] failed[%v].", conf.Options.LogDirectory,
			conf.Options.LogFileName, err), -2)
	}
	defer utils.Goodbye()
	l.Logger.Infof("log init succeed. log.dir[%v] log.name[%v] log.level[%v]",
		conf.Options.LogDirectory, conf.Options.LogFileName, conf.Options.LogLevel)
	l.Logger.Infof("MongoDB Version Source[%v] Target[%v]", conf.Options.SourceDBVersion, conf.Options.TargetDBVersion)

	conf.Options.Version = utils.BRANCH

	nimo.Profiling(conf.Options.SystemProfilePort)
	signalProfile, _ := strconv.Atoi(utils.SIGNALPROFILE)
	signalStack, _ := strconv.Atoi(utils.SIGNALSTACK)
	if signalProfile > 0 {
		nimo.RegisterSignalForProfiling(syscall.Signal(signalProfile))                     // syscall.SIGUSR2
		nimo.RegisterSignalForPrintStack(syscall.Signal(signalStack), func(bytes []byte) { // syscall.SIGUSR1
			l.Logger.Infof("%s", string(bytes))
		})
	}

	utils.Welcome()

	// get exclusive process lock and write pid
	if utils.WritePidById(conf.Options.LogDirectory, conf.Options.Id) {
		if *GCPercent > 0 && *GCPercent <= 100 {
			debug.SetGCPercent(*GCPercent)
		}
		startup()
	}
}

func startup() {
	// leader election at the beginning
	selectLeader()

	// initialize http api
	utils.FullSyncInitHttpApi(conf.Options.FullSyncHTTPListenPort)
	utils.IncrSyncInitHttpApi(conf.Options.IncrSyncHTTPListenPort)
	utils.PrometheusInitHttpApi(conf.Options.PromHTTPListenPort)
	ReplCord := &coordinator.ReplicationCoordinator{
		MongoD: make([]*utils.MongoSource, len(conf.Options.MongoUrls)),
	}

	// register conf
	utils.FullSyncHttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return conf.GetSafeOptions()
	})
	utils.IncrSyncHttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return conf.GetSafeOptions()
	})

	if utils.IsHTTPPortEnabled(conf.Options.PromHTTPListenPort) {
		nimo.GoRoutine(func() {
			if err := utils.PrometheusHttpApi.Listen(); err != nil {
				l.Logger.Errorf("start prometheus server with port[%v] failed: %v",
					conf.Options.PromHTTPListenPort, err)
			}
		})
	} else {
		l.Logger.Infof("prometheus http api disabled. port[%v]", conf.Options.PromHTTPListenPort)
	}

	// init
	for i, src := range conf.Options.MongoUrls {
		ReplCord.MongoD[i] = new(utils.MongoSource)
		ReplCord.MongoD[i].URL = src
		if len(conf.Options.IncrSyncOplogGIDS) != 0 {
			ReplCord.MongoD[i].Gids = conf.Options.IncrSyncOplogGIDS
		}
	}
	if conf.Options.MongoSUrl != "" {
		ReplCord.MongoS = &utils.MongoSource{
			URL:         conf.Options.MongoSUrl,
			ReplicaName: utils.ReplicaNameMongos,
		}
		ReplCord.RealSourceFullSync = []*utils.MongoSource{ReplCord.MongoS}
		ReplCord.RealSourceIncrSync = []*utils.MongoSource{ReplCord.MongoS}
		if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
			ReplCord.RealSourceIncrSync = ReplCord.MongoD
		}
	} else {
		ReplCord.RealSourceFullSync = ReplCord.MongoD
		ReplCord.RealSourceIncrSync = ReplCord.MongoD
	}

	if conf.Options.MongoCsUrl != "" {
		ReplCord.MongoCS = &utils.MongoSource{
			URL: conf.Options.MongoCsUrl,
		}
	}

	// start mongodb replication
	if err := ReplCord.Run(); err != nil {
		// initial or connection established failed
		l.Logger.Errorf("run replication failed: %v", err)
		crash(err.Error(), -6)
	}

	// if the sync mode is "document", mongoshake should exit here.
	if conf.Options.SyncMode == utils.VarSyncModeFull {
		return
	}

	// do not exit
	select {}
}

func selectLeader() {
	// first of all. ensure we are the Master
	if conf.Options.MasterQuorum && conf.Options.CheckpointStorage == utils.VarCheckpointStorageDatabase {
		// election become to Master. keep waiting if we are the candidate. election id must be fixed
		objectId, _ := primitive.ObjectIDFromHex("5204af979955496907000001")
		quorum.UseElectionObjectId(objectId)
		go func() {
			_ = quorum.BecomeMaster(conf.Options.CheckpointStorageUrl, utils.VarCheckpointStorageDbReplicaDefault)
		}()

		// wait until become to a real master
		<-quorum.MasterPromotionNotifier
	} else {
		quorum.AlwaysMaster()
	}
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
