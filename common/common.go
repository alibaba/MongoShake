package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/nightlyone/lockfile"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// Build info
var BRANCH = "$"
var SIGNALPROFILE = "$"
var SIGNALSTACK = "$"

const (
	GlobalDiagnosticPath = "diagnostic"
	GolangSecurityTime   = "2006-01-02T15:04:05Z"
	defaultLogDir        = "logs"
	defaultLogFile       = "mongoshake.log"

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

// InitialLogger initialize logger
//
// verbose: where log goes to: 0 - file，1 - file+stdout，2 - stdout
func InitialLogger(logDir, logFile, level string, logFlush bool, verbose int) error {
	return InitialLoggerWithRotation(logDir, logFile, level, logFlush, verbose, 20, 7)
}

func InitialLoggerWithRotation(logDir, logFile, level string,
	logFlush bool, verbose, maxSizeMB, maxAge int) error {
	if logDir == "" {
		logDir = defaultLogDir
	}
	if logFile == "" {
		logFile = defaultLogFile
	}

	if verbose != 2 {
		if err := os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
			return fmt.Errorf("create log.dir[%v] failed[%v]", logDir, err)
		}
	}

	return l.New(level, logDir, logFile, logFlush, maxSizeMB, maxAge, verbose)
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

// BlockMongoUrlPassword block password in two kinds of mongo_urls:
// 1) "mongodb://username:password@address"
// 2) "username:password@address"
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

	at := strings.LastIndex(url, "@")
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
func DuplicateKey(err error) bool {
	return mongo.IsDuplicateKeyError(err)
}

// HaveIdIndexKey return true if index key is just '_id'
func HaveIdIndexKey(obj bson.D) bool {
	for _, ele := range obj {
		if ele.Key != "key" {
			continue
		}

		keyValue, ok := ele.Value.(bson.D)
		if !ok {
			continue
		}
		if len(keyValue) > 1 {
			continue
		}

		for _, fieldEle := range keyValue {
			if fieldEle.Key == "_id" {
				return true
			}
		}
	}

	return false
}
