package main

import (
	"os"

	"mongoshake/collector/mongo_oplog_replay"

	LOG "github.com/vinllen/log4go"
	"github.com/mongodb/mongo-tools-common/signals"
	"github.com/mongodb/mongo-tools-common/util"
	"mongoshake/common"
	"fmt"
)

var (
	VersionStr = "built-without-version-string"
	GitCommit  = "build-without-git-commit"
)

type Exit struct{ Code int }

func main() {
	defer LOG.Close()

	opts, err := mongo_oplog_replay.ParseOptions(os.Args[1:], VersionStr, GitCommit)
	if err != nil {
		LOG.Error(util.ShortUsage("mongooplogreplay"))
		crash(fmt.Sprintf("parse options failed[%v].", err), -1)
	}

	if err := utils.InitialLogger("", "log.out", utils.VarLogLevelInfo, true, true); err != nil {
		crash(fmt.Sprintf("initial log failed[%v].", err), -2)
	}

	// print help or version info, if specified
	if opts.PrintHelp(false) {
		return
	}

	if opts.PrintVersion() {
		return
	}

	oplogreplay, err := mongo_oplog_replay.New(opts)
	if err != nil {
		crash(fmt.Sprintf("create opts failed[%v].", err), -1)
	}
	defer oplogreplay.Close()

	finishedChan := signals.HandleWithInterrupt(oplogreplay.HandleInterrupt)
	defer close(finishedChan)

	if err = oplogreplay.Replay(); err != nil {
		// log.LogTagvf(log.Always, log.ERROR, "Failed: %v", err)
		LOG.Critical("start replay failed: %v", err)
		if err == util.ErrTerminated {
			os.Exit(util.ExitFailure)
		}
		os.Exit(util.ExitFailure)
	}

	os.Exit(util.ExitSuccess)
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
