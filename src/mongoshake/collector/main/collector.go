// +build darwin linux windows

package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"
	"strconv"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/quorum"
	"mongoshake/collector/coordinator"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

type Exit struct{ Code int }

func main() {
	var err error
	defer handleExit()
	defer LOG.Close()
	defer utils.Goodbye()

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
	if err = SanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	if err := utils.InitialLogger(conf.Options.LogDirectory, conf.Options.LogFileName, conf.Options.LogLevel, conf.Options.LogFlush, *verbose); err != nil {
		crash(fmt.Sprintf("initial log.dir[%v] log.name[%v] failed[%v].", conf.Options.LogDirectory,
			conf.Options.LogFileName, err), -2)
	}

	conf.Options.Version = utils.BRANCH

	nimo.Profiling(int(conf.Options.SystemProfile))
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
	if utils.WritePidById(conf.Options.LogDirectory, conf.Options.Id) {
		startup()
	}
}

func startup() {
	// leader election at the beginning
	selectLeader()

	// initialize http api
	utils.InitHttpApi(conf.Options.HTTPListenPort)
	coordinator := &coordinator.ReplicationCoordinator{
		Sources: make([]*utils.MongoSource, len(conf.Options.MongoUrls)),
	}

	utils.HttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return conf.GetSafeOptions()
	})

	for i, src := range conf.Options.MongoUrls {
		coordinator.Sources[i] = new(utils.MongoSource)
		coordinator.Sources[i].URL = src
		if len(conf.Options.IncrSyncOplogGIDS) != 0 {
			coordinator.Sources[i].Gids = conf.Options.IncrSyncOplogGIDS
		}
	}

	// start mongodb replication
	if err := coordinator.Run(); err != nil {
		// initial or connection established failed
		crash(fmt.Sprintf("run replication failed: %v", err), -6)
	}

	// if the sync mode is "document", mongoshake should exit here.
	if conf.Options.SyncMode != utils.VarSyncModeFull {
		if err := utils.HttpApi.Listen(); err != nil {
			LOG.Critical("Coordinator http api listen failed. %v", err)
		}
	}
}

func selectLeader() {
	// first of all. ensure we are the Master
	if conf.Options.MasterQuorum && conf.Options.CheckpointStorage == utils.VarCheckpointStorageDatabase {
		// election become to Master. keep waiting if we are the candidate. election id is must fixed
		quorum.UseElectionObjectId(bson.ObjectIdHex("5204af979955496907000001"))
		go quorum.BecomeMaster(conf.Options.CheckpointStorageUrl, utils.VarCheckpointStorageDbReplicaDefault)

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
