package utils

import (
	"fmt"
	"os"
	"strings"
	"encoding/json"

	"github.com/nightlyone/lockfile"
	LOG "github.com/vinllen/log4go"

)

// Build info
var BRANCH = "$"
var SIGNALPROFILE = "$"
var SIGNALSTACK = "$"

const (
	// APPNAME = "mongoshake"
	// AppDatabase          = APPNAME
	// APPConflictDatabase  = APPNAME + "_conflict"

	GlobalDiagnosticPath = "diagnostic"
	// This is the time of golang was born to the world
	GolangSecurityTime = "2006-01-02T15:04:05Z"

	WorkGood       uint64 = 0
	GetReady       uint64 = 1
	FetchBad       uint64 = 2
	TunnelSendBad  uint64 = 4
	TunnelSyncBad  uint64 = 8
	ReplicaExecBad uint64 = 16

	MajorityWriteConcern = "majority"

	Int32max = (int64(1) << 32) - 1
)

var (
	AppDatabase         = VarCheckpointStorageDbReplicaDefault
	APPConflictDatabase = VarCheckpointStorageDbReplicaDefault + "_conflict"
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
func InitialLogger(logDir, logFile, level string, logFlush bool, verbose bool) error {
	logLevel := parseLogLevel(level)
	if verbose {
		writer := LOG.NewConsoleLogWriter()
		writer.SetFormat("[%D %T] [%L] %M")
		LOG.AddFilter("console", logLevel, writer)
	}

	if len(logDir) == 0 {
		logDir = "logs"
	}
	// check directory exists
	if _, err := os.Stat(logDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
			return fmt.Errorf("create log.dir[%v] failed[%v]", logDir, err)
		}
	}

	if len(logFile) != 0 {
		if !logFlush {
			LOG.LogBufferLength = 32
		} else {
			LOG.LogBufferLength = 0
		}
		fileLogger := LOG.NewFileLogWriter(fmt.Sprintf("%s/%s", logDir, logFile), true)
		fileLogger.SetRotateDaily(true)
		// fileLogger.SetFormat("[%D %T] [%L] [%s] %M") // print function
		fileLogger.SetFormat("[%D %T] [%L] %M")
		fileLogger.SetRotateMaxBackup(7)
		LOG.AddFilter("file", logLevel, fileLogger)
	} else {
		return fmt.Errorf("log.file[%v] shouldn't be empty", logFile)
	}

	return nil
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

/**
 * block password in mongo_urls:
 * two kind mongo_urls:
 * 1. mongodb://username:password@address
 * 2. username:password@address
 */
func BlockMongoUrlPassword(url, replace string) string {
	colon := strings.Index(url, ":")
	if colon == -1 || colon == len(url)-1 {
		return url
	} else if url[colon+1] == '/' {
		// find the second '/'
		for colon++; colon < len(url); colon++ {
			if url[colon] == ':' {
				break
			}
		}

		if colon == len(url) {
			return url
		}
	}

	at := strings.Index(url, "@")
	if at == -1 || at == len(url)-1 || at <= colon {
		return url
	}

	newUrl := make([]byte, 0, len(url))
	for i := 0; i < len(url); i++ {
		if i <= colon || i > at {
			newUrl = append(newUrl, byte(url[i]))
		} else if i == at {
			newUrl = append(newUrl, []byte(replace)...)
			newUrl = append(newUrl, byte(url[i]))
		}
	}
	return string(newUrl)
}

// marshal given strcut by json
func MarshalStruct(input interface{}) string {
	ret, err := json.Marshal(input)
	if err != nil {
		return fmt.Sprintf("marshal struct failed[%v]", err)
	}
	return string(ret)
}