package mongo_oplog_replay

import (
	"io"
	"fmt"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"github.com/mongodb/mongo-tools-common/json"
	"github.com/mongodb/mongo-tools-common/util"
	LOG "github.com/vinllen/log4go"
	"os"
	"strings"
	"strconv"
	"mongoshake/collector/coordinator"
	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/quorum"
	"time"
	"mongoshake/oplog"
)

const (
	ParserNum = 8
	WorkerNum = 16
)

type MongoOplogReplay struct {
	ToolOptions   *options.ToolOptions
	InputOptions  *InputOptions
	OutputOptions *OutputOptions
	NSOptions     *NSOptions

	SessionProvider *db.SessionProvider

	TargetFiles string

	// other internal state
	oplogGte primitive.Timestamp
	oplogLt  primitive.Timestamp

	// channel on which to notify if/when a termination signal is received
	termChan chan struct{}

	// for testing. If set, this value will be used instead of os.Stdin
	stdin io.Reader

	// Server version for version-specific behavior
	serverVersion db.Version

	// only replays oplog belogs to dbs
	dbNames []string
}

// New initializes an instance of MongoOplogReplay according to the provided options.
func New(opts Options) (*MongoOplogReplay, error) {
	provider, err := db.NewSessionProvider(*opts.ToolOptions)
	if err != nil {
		return nil, fmt.Errorf("error connecting to host: %v", err)
	}

	serverVersion, err := provider.ServerVersionArray()
	if err != nil {
		return nil, fmt.Errorf("error getting server version: %v", err)
	}

	oplogreplay := &MongoOplogReplay{
		ToolOptions:     opts.ToolOptions,
		OutputOptions:   opts.OutputOptions,
		InputOptions:    opts.InputOptions,
		NSOptions:       opts.NSOptions,
		TargetFiles:     opts.TargetFiles,
		SessionProvider: provider,
		serverVersion:   serverVersion,
	}

	return oplogreplay, nil
}

func (oplogreplay *MongoOplogReplay) Close() {
	oplogreplay.SessionProvider.Close()
}

func (oplogreplay *MongoOplogReplay) HandleInterrupt() {
	if oplogreplay.termChan != nil {
		close(oplogreplay.termChan)
	}
}

// ParseTimestampFlag takes in a string the form of <time_t>,<ordinal>,
// where <time_t> is the seconds since the UNIX epoch, and <ordinal> represents
// a counter of operations in the oplog that occurred in the specified second.
// It parses this timestamp string and returns a bson.MongoTimestamp type.
func ParseTimestampFlag(ts string) (primitive.Timestamp, error) {
	var seconds, increment int
	timestampFields := strings.Split(ts, ",")
	if len(timestampFields) > 2 {
		return primitive.Timestamp{}, fmt.Errorf("too many : characters")
	}

	seconds, err := strconv.Atoi(timestampFields[0])
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("error parsing timestamp seconds: %v", err)
	}

	// parse the increment field if it exists
	if len(timestampFields) == 2 {
		if len(timestampFields[1]) > 0 {
			increment, err = strconv.Atoi(timestampFields[1])
			if err != nil {
				return primitive.Timestamp{}, fmt.Errorf("error parsing timestamp increment: %v", err)
			}
		} else {
			// handle the case where the user writes "<time_t>," with no ordinal
			increment = 0
		}
	}

	return primitive.Timestamp{T: uint32(seconds), I: uint32(increment)}, nil
}

func (oplogreplay *MongoOplogReplay) ParseAndValidateOptions() error {
	LOG.Info("checking options")
	var err error
	if oplogreplay.InputOptions.OplogGte != "" {
		oplogreplay.oplogGte, err = ParseTimestampFlag(oplogreplay.InputOptions.OplogGte)
		if err != nil {
			return fmt.Errorf("error parsing timestamp argument to --oplogGte: %v", err)
		}
	}
	if oplogreplay.InputOptions.OplogLt != "" {
		oplogreplay.oplogLt, err = ParseTimestampFlag(oplogreplay.InputOptions.OplogLt)
		if err != nil {
			return fmt.Errorf("error parsing timestamp argument to --oplogLt: %v", err)
		}
	}
	if oplogreplay.NSOptions.DBS != "" {
		json.Unmarshal([]byte(oplogreplay.NSOptions.DBS), &oplogreplay.dbNames)
		if err != nil {
			return fmt.Errorf("error parsing namesoace options to --dbs: %v", err)
		}
		LOG.Info("replay oplog for databases: %v", oplogreplay.dbNames)
		for _, dbName := range oplogreplay.dbNames {
			if err := util.ValidateDBName(dbName); err != nil {
				return fmt.Errorf("invalid db name: %v", err)
			}
		}
	}
	if oplogreplay.stdin == nil {
		oplogreplay.stdin = os.Stdin
	}

	return nil
}

func (oplogreplay *MongoOplogReplay) Replay() error {
	err := oplogreplay.ParseAndValidateOptions()
	if err != nil {
		LOG.Info("got error from options parsing: %v", err)
		return err
	}

	oplogreplay.termChan = make(chan struct{})

	coordinator := &coordinator.ReplicationCoordinator{}

	// init extra conf
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodFile
	conf.Options.FilterNamespaceWhite = oplogreplay.dbNames
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncWorker = WorkerNum
	conf.Options.Tunnel = "direct"
	conf.Options.TunnelAddress = []string{oplogreplay.ToolOptions.ConnectionString}
	conf.Options.CheckpointStorageUrl = "/tmp/mongo-oplog-replay-checkpoint.txt"
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageFile
	conf.Options.IncrSyncReaderBufferTime = 1
	conf.Options.SkipFailure = oplogreplay.OutputOptions.SkipFailure
	conf.Options.IncrSyncExecutorInsertOnDupUpdate = true
	conf.Options.IncrSyncExecutorUpsert = true
	conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
	conf.Options.IncrSyncWorkerBatchQueueSize = 64
	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 1024
	conf.Options.IncrSyncFetcherBufferCapacity = 256
	conf.Options.IncrSyncExecutor = 1
	conf.Options.CheckpointInterval = 5000
	conf.Options.CheckpointStartPosition = 1
	conf.Options.LogLevel = utils.VarLogLevelDebug
	conf.Options.LogFlush = true
	conf.Options.Id = "mongoshake-mongooplogreplay"
	conf.Options.IncrSyncShardKey = oplog.ShardByID

	quorum.AlwaysMaster()

	// start mongodb replication
	gte := (int64(oplogreplay.oplogGte.T) << 32) | int64(oplogreplay.oplogGte.I)
	lt := (int64(oplogreplay.oplogLt.T) << 32) | int64(oplogreplay.oplogLt.I)
	LOG.Info("start %v with InputOptions", conf.Options.Id, *oplogreplay.InputOptions)

	if err := coordinator.StartOplogReplay(gte, lt, ParserNum, WorkerNum,
			strings.Split(oplogreplay.TargetFiles, ","), oplogreplay.InputOptions.Directory); err != nil {
		// initial or connection established failed
		return err
	}

	for range time.NewTicker(3 * time.Second).C {
		if utils.Exit {
			break
		}
	}

	LOG.Info("bye")
	return nil
}