// this is an receiver example connect to different tunnels
package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"
	"errors"
	"strconv"

	"mongoshake/common"
	"mongoshake/receiver/configure"
	"mongoshake/tunnel"
	"mongoshake/receiver"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

type Exit struct{ Code int }

func main() {
	var err error
	defer handleExit()
	defer LOG.Close()

	// argument options
	configuration := flag.String("conf", "", "configure file absolute path")
	verbose := flag.Bool("verbose", false, "show logs on console")
	flag.Parse()

	if *configuration == "" {
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

	// verify receiver options and revise
	if err = sanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	if err := utils.InitialLogger(conf.Options.LogDirectory, conf.Options.LogFileName, conf.Options.LogLevel, conf.Options.LogFlush, *verbose); err != nil {
		crash(fmt.Sprintf("initial log.dir[%v] log.name[%v] failed[%v].", conf.Options.LogDirectory,
			conf.Options.LogFileName, err), -2)
	}
	nimo.Profiling(int(conf.Options.SystemProfilePort))
	signalProfile, _ := strconv.Atoi(utils.SIGNALPROFILE)
	signalStack, _ := strconv.Atoi(utils.SIGNALSTACK)
	if signalProfile > 0 {
		nimo.RegisterSignalForProfiling(syscall.Signal(signalProfile)) // syscall.SIGUSR2
		nimo.RegisterSignalForPrintStack(syscall.Signal(signalStack), func(bytes []byte) { // syscall.SIGUSR1
			LOG.Info(string(bytes))
		})
	}


	startup()

	select {}
}

func sanitizeOptions() error {
	if conf.Options.Tunnel == "" {
		return errors.New("tunnel is empty")
	}
	if len(conf.Options.TunnelAddress) == 0 {
		return errors.New("tunnel address is illegal")
	}
	return nil
}

// this is the main connector function
func startup() {
	factory := tunnel.ReaderFactory{Name: conf.Options.Tunnel}
	reader := factory.Create(conf.Options.TunnelAddress)
	if reader == nil {
		return
	}

	/*
	 * create re-players, the number of re-players number is equal to the
	 * collector worker number to fulfill load balance. The tunnel that message
	 * sent to is determined in the collector side: `TMessage.Shard`.
	 */
	repList := make([]tunnel.Replayer, conf.Options.ReplayerNum)
	for i := range repList {
		repList[i] = replayer.NewExampleReplayer(i)
	}

	LOG.Info("receiver is starting...")
	if err := reader.Link(repList); err != nil {
		LOG.Critical("Replayer link to tunnel error %v", err)
		return
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
