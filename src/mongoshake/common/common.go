package utils

import (
	"fmt"
	"os"
	"strings"

	LOG "github.com/vinllen/log4go"
	"github.com/nightlyone/lockfile"
)

const (
	WorkGood       uint64 = 0
	GetReady       uint64 = 1
	FetchBad       uint64 = 2
	TunnelSendBad  uint64 = 4
	TunnelSyncBad  uint64 = 8
	ReplicaExecBad uint64 = 16
)

// Build info
var BRANCH = "$"

const (
	APPNAME = "mongoshake"

	AppDatabase          = APPNAME
	APPConflictDatabase  = APPNAME + "_conflict"
	GlobalDiagnosticPath = "diagnostic"

	// This is the time of golang was born to the world
	GolangSecurityTime = "2006-01-02T15:04:05Z"
)

func init() {
	// prepare global folders
	Mkdirs(GlobalDiagnosticPath /*, GlobalStoragePath*/)
}

func RunStatusMessage(status uint64) string {
	switch status {
	case WorkGood:
		return "Good"
	case GetReady:
		return "prepare for ready"
	case FetchBad:
		return "can't fetch oplog from source MongoDB"
	case TunnelSendBad:
		return "collector send oplog to tunnel failed"
	case TunnelSyncBad:
		return "receiver fetch from tunnel failed"
	case ReplicaExecBad:
		return "receiver replica executed failed"
	default:
		return "unknown"
	}
}

func InitialLogger(logFile string, level string, logBuffer bool, verbose bool) bool {
	logLevel := parseLogLevel(level)
	if verbose {
		LOG.AddFilter("console", logLevel, LOG.NewConsoleLogWriter())
	}
	if len(logFile) != 0 {
		// create logs folder for log4go. because of its mistake that doesn't create !
		if err := os.MkdirAll("logs", os.ModeDir|os.ModePerm); err != nil {
			return false
		}
		if logBuffer {
			LOG.LogBufferLength = 32
		} else {
			LOG.LogBufferLength = 0
		}
		fileLogger := LOG.NewFileLogWriter(fmt.Sprintf("logs/%s", logFile), true)
		fileLogger.SetRotateDaily(true)
		fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		fileLogger.SetRotateMaxBackup(7)
		LOG.AddFilter("file", logLevel, fileLogger)
	}
	return true
}

func parseLogLevel(level string) LOG.Level {
	switch strings.ToLower(level) {
	case "debug":
		return LOG.DEBUG
	case "info":
		return LOG.INFO
	case "warning":
		return LOG.WARNING
	case "error":
		return LOG.ERROR
	default:
		return LOG.DEBUG
	}
}

func WritePid(id string) (err error) {
	var lock lockfile.Lockfile
	lock, err = lockfile.New(id)
	if err != nil {
		return err
	}
	if err = lock.TryLock(); err != nil {
		return err
	}

	return nil
}

func DelayFor(ms int64) {
	YieldInMs(ms)
}
