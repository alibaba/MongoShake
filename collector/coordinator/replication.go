package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	nimo "github.com/gugemichael/nimo4go"

	"github.com/alibaba/MongoShake/v2/collector"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

var (
	testSelectSyncMode = false
)

// ReplicationCoordinator global coordinator instance. consist of
// one syncerGroup and a number of workers
type ReplicationCoordinator struct {
	MongoD             []*utils.MongoSource // the source mongod
	MongoS             *utils.MongoSource   // the source mongos
	MongoCS            *utils.MongoSource   // the source mongo cs
	RealSourceFullSync []*utils.MongoSource // point to MongoD if source is mongod, otherwise MongoS
	RealSourceIncrSync []*utils.MongoSource // point to MongoD if source is mongod, otherwise MongoS

	// Sentinel listener
	fullSentinel *utils.Sentinel
	incrSentinel *utils.Sentinel

	// syncerGroup and workerGroup number is 1:N in ReplicaSet.
	// 1:1 while replicated in shard cluster
	syncerGroup []*collector.OplogSyncer

	// namespaces that carry a unique index other than _id. shard_key=auto
	// replays them in order on a single worker
	orderedNamespaces []string
}

func (coordinator *ReplicationCoordinator) Run() error {
	// check all mongodb deployment and fetch the instance info
	if err := coordinator.sanitizeMongoDB(); err != nil {
		return err
	}
	l.Logger.Infof("Collector startup. shard_by[%s] gids[%s]", conf.Options.IncrSyncShardKey, conf.Options.IncrSyncOplogGIDS)

	// run extra job if we need
	if err := RunExtraJob(coordinator.RealSourceIncrSync); err != nil {
		return err
	}

	// all configurations have changed to immutable
	// opts, _ := json.Marshal(conf.Options)
	opts, _ := json.Marshal(conf.GetSafeOptions())
	l.Logger.Infof("Collector configuration %s", string(opts))

	// sentinel: full and incr
	coordinator.fullSentinel = utils.NewSentinel(utils.TypeFull)
	coordinator.fullSentinel.Register()
	coordinator.incrSentinel = utils.NewSentinel(utils.TypeIncr)
	coordinator.incrSentinel.Register()

	syncMode, startTsMap, fullBeginTs, err := coordinator.selectSyncMode(conf.Options.SyncMode)
	if err != nil {
		return fmt.Errorf("select sync mode failed: %v", err)
	}

	/*
	 * Generally speaking, it's better to use several bridge timestamp so that
	 * each shard match one in sharding mode.
	 * TODO
	 */
	fullBegin := fullBeginTs
	if val, ok := fullBegin.(int64); ok {
		fullBegin = utils.ExtractTimestampForLog(val)
	}
	l.Logger.Infof("start running with mode[%v], fullBeginTs[%v]", syncMode, fullBegin)

	switch syncMode {
	case utils.VarSyncModeAll:
		if conf.Options.FullSyncReaderOplogStoreDisk {
			l.Logger.Infof("run parallel document oplog")
			if err := coordinator.parallelDocumentOplog(fullBeginTs); err != nil {
				return err
			}
		} else {
			l.Logger.Infof("run serialize document oplog")
			if err := coordinator.serializeDocumentOplog(fullBeginTs); err != nil {
				return err
			}
		}
	case utils.VarSyncModeFull:
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
	case utils.VarSyncModeIncr:
		if err := coordinator.startOplogReplication(int64(0), int64(0), startTsMap); err != nil {
			return err
		}
	default:
		l.Logger.Criticalf("unknown sync mode %v", conf.Options.SyncMode)
		return errors.New("unknown sync mode " + conf.Options.SyncMode)
	}

	return nil
}

func (coordinator *ReplicationCoordinator) sanitizeMongoDB() error {
	var conn *utils.MongoCommunityConn
	var err error
	rs := map[string]int{}

	// the index metadata decides the routing for shard_key=auto and feeds the
	// warning for shard_key=id, so it is scanned once before the syncers start
	scanUniqueIndex := conf.Options.SyncMode != utils.VarSyncModeFull &&
		(conf.Options.IncrSyncShardKey == oplog.ShardAutomatic ||
			conf.Options.IncrSyncShardKey == oplog.ShardByID)
	uniqueIndexNamespaces := make(map[string]struct{})
	var scanUniqueIndexErr error

	// try to connect CheckpointStorage
	checkpointStorageUrl := conf.Options.CheckpointStorageUrl
	if conn, err = utils.NewMongoCommunityConn(checkpointStorageUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault,
		conf.Options.CheckpointStorageUrlMongoSslRootCaFile); conn == nil || !conn.IsGood() || err != nil {
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			err = fmt.Errorf("checkpoint storage[%v] is not reachable", utils.BlockMongoUrlPassword(checkpointStorageUrl, "***"))
		}
		l.Logger.Criticalf("Connect checkpointStorageUrl[%v] error[%v]. Please add primary node into 'mongo_urls' "+
			"if 'context.storage.url' is empty", checkpointStorageUrl, err)
		return err
	}
	conn.Close()

	for i, src := range coordinator.MongoD {
		if conn, err = utils.NewMongoCommunityConn(src.URL, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault,
			conf.Options.MongoSslRootCaFile); conn == nil || !conn.IsGood() || err != nil {
			if conn != nil {
				conn.Close()
			}
			if err == nil {
				err = fmt.Errorf("mongo server[%v] is not reachable", utils.BlockMongoUrlPassword(src.URL, "***"))
			}

			l.Logger.Criticalf("Connect mongo server error. %v, url : %s. "+
				"See https://github.com/alibaba/MongoShake/wiki/FAQ"+
				"#q-how-to-solve-the-oplog-tailer-initialize-failed-no-reachable-servers-error", err, src.URL)
			return err
		}

		// a conventional ReplicaSet should have local.oplog.rs collection
		if conf.Options.SyncMode != utils.VarSyncModeFull &&
			// conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog &&
			conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog &&
			!conn.HasOplogNs(utils.GetListCollectionQueryCondition(conn)) {

			l.Logger.Criticalf("There has no oplog collection in mongo db server")
			conn.Close()
			return errors.New("no oplog ns in mongo. " +
				"See https://github.com/alibaba/MongoShake/wiki/FAQ" +
				"#q-how-to-solve-the-oplog-tailer-initialize-failed-no-oplog-ns-in-mongo-error")
		}

		// check if there has dup server every replica set in RS or Shard
		rsName := conn.AcquireReplicaSetName()
		// rsName will be set to default if empty
		if rsName == "" {
			rsName = fmt.Sprintf("default-%d", i)
			l.Logger.Warnf("Source mongodb have empty replica set name, url[%s], change to default[%s]",
				utils.BlockMongoUrlPassword(src.URL, "***"), rsName)
		}

		if _, exist := rs[rsName]; exist {
			l.Logger.Criticalf("There has duplicate replica set name : %s", rsName)
			conn.Close()
			return errors.New("duplicated replica set source")
		}
		rs[rsName] = 1
		src.ReplicaName = rsName

		// look around if there has unique index
		if scanUniqueIndex && scanUniqueIndexErr == nil {
			nsList, scanErr := conn.ListUniqueIndexNamespaces(utils.GetListCollectionQueryCondition(conn))
			if scanErr != nil {
				scanUniqueIndexErr = scanErr
			}
			for _, ns := range nsList {
				uniqueIndexNamespaces[ns] = struct{}{}
			}
		}
		// doesn't reuse current connection
		conn.Close()
	}

	// a mongos source (change stream on a sharded cluster) has no entry in
	// MongoD, but its collection metadata is what decides the routing
	if scanUniqueIndex && scanUniqueIndexErr == nil && coordinator.MongoS != nil &&
		conf.Options.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodOplog {
		conn, err = utils.NewMongoCommunityConn(coordinator.MongoS.URL, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil || conn == nil || !conn.IsGood() {
			if conn != nil {
				conn.Close()
			}
			scanUniqueIndexErr = fmt.Errorf("connect mongos source[%v] failed: %v",
				utils.BlockMongoUrlPassword(coordinator.MongoS.URL, "***"), err)
		} else {
			nsList, scanErr := conn.ListUniqueIndexNamespaces(utils.GetListCollectionQueryCondition(conn))
			conn.Close()
			if scanErr != nil {
				scanUniqueIndexErr = scanErr
			}
			for _, ns := range nsList {
				uniqueIndexNamespaces[ns] = struct{}{}
			}
		}
	}

	switch conf.Options.IncrSyncShardKey {
	case oplog.ShardAutomatic:
		if scanUniqueIndexErr != nil {
			// an unreadable namespace may carry a unique index, so replay
			// everything in order instead of guessing
			conf.Options.IncrSyncShardKey = oplog.ShardByNamespace
			l.Logger.Criticalf("shard_key=auto: read source index metadata failed[%v], "+
				"fall back to shard_key=collection so that every namespace replays in order", scanUniqueIndexErr)
			break
		}
		coordinator.orderedNamespaces = sortedNamespaces(uniqueIndexNamespaces)
		l.Logger.Infof("shard_key=auto: %d namespace(s) carry a unique index besides _id and replay in order: %v. "+
			"Other namespaces hash by _id", len(coordinator.orderedNamespaces), coordinator.orderedNamespaces)
		// a worker hands its batch to incr_sync.executor executors split by _id,
		// which breaks the order across documents that ordered replay relies on
		if len(coordinator.orderedNamespaces) != 0 && conf.Options.IncrSyncExecutor > 1 {
			l.Logger.Criticalf("shard_key=auto: incr_sync.executor=%d splits the batch of one worker "+
				"across executors by _id, which breaks the ordered replay these namespaces need: %v. "+
				"Set incr_sync.executor=1 to keep the guarantee",
				conf.Options.IncrSyncExecutor, coordinator.orderedNamespaces)
		}
	case oplog.ShardByID:
		// shard_key=id keeps its meaning, but a silent data loss is not an option
		if scanUniqueIndexErr != nil {
			l.Logger.Errorf("shard_key=id: read source index metadata failed[%v], "+
				"cannot tell whether a unique index makes the routing unsafe", scanUniqueIndexErr)
			break
		}
		if nsList := sortedNamespaces(uniqueIndexNamespaces); len(nsList) != 0 {
			l.Logger.Criticalf("shard_key=id: these namespaces carry a unique index besides _id and may lose "+
				"documents when the replay order changes: %v. Use shard_key=auto or shard_key=collection, "+
				"or drop the unique index", nsList)
		}
	}

	// TODO, check unique exist on given namespace
	if len(conf.Options.IncrSyncShardByObjectIdWhiteList) != 0 {

	}

	return nil
}

func sortedNamespaces(namespaces map[string]struct{}) []string {
	nsList := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		nsList = append(nsList, ns)
	}
	sort.Strings(nsList)
	return nsList
}

// run incr-sync after full-sync
func (coordinator *ReplicationCoordinator) serializeDocumentOplog(fullBeginTs interface{}) error {
	var err error
	if err = coordinator.startDocumentReplication(); err != nil {
		return fmt.Errorf("start document replication failed: %v", err)
	}

	// get current newest timestamp
	var fullFinishTs, oldestTs int64
	if conf.Options.SpecialSourceDBFlag != utils.VarSpecialSourceDBFlagAliyunServerless && len(coordinator.MongoD) > 0 {
		_, fullFinishTs, _, oldestTs, _, err = utils.GetAllTimestamp(coordinator.MongoD, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return fmt.Errorf("get full sync finish timestamp failed[%v]", err)
		}
	} else {
		fullFinishTs = int64(math.MaxInt64)
	}

	l.Logger.Infof("------------------------full sync done!------------------------")

	fullBegin := fullBeginTs
	if val, ok := fullBeginTs.(int64); ok {
		fullBegin = utils.ExtractTimestampForLog(val)

		// the oldest oplog is lost
		if oldestTs >= val {
			err = fmt.Errorf("incr sync ts[%v] is less than current oldest ts[%v], this error means user's "+
				"oplog collection size is too small or full sync continues too long",
				fullBegin, utils.ExtractTimestampForLog(oldestTs))
			l.Logger.Errorf("%v", err)
			return err
		}

		l.Logger.Infof("oldestTs[%v] fullBeginTs[%v] fullFinishTs[%v]", utils.ExtractTimestampForLog(oldestTs),
			fullBegin, utils.ExtractTimestampForLog(fullFinishTs))
	}

	l.Logger.Infof("finish full sync, start incr sync with timestamp: fullBeginTs[%v], fullFinishTs[%v]",
		fullBegin, utils.ExtractTimestampForLog(fullFinishTs))

	return coordinator.startOplogReplication(fullBeginTs, fullFinishTs, nil)
}

// TODO, set initSyncFinishTs into worker
// run full-sync and incr-sync in parallel
func (coordinator *ReplicationCoordinator) parallelDocumentOplog(fullBeginTs interface{}) error {
	var docError error
	var docWg sync.WaitGroup
	docWg.Add(1)
	nimo.GoRoutine(func() {
		defer docWg.Done()
		if err := coordinator.startDocumentReplication(); err != nil {
			l.Logger.Criticalf("document Replication error. %v", err)
			docError = err
			return
		}
		l.Logger.Infof("------------------------full sync done!------------------------")
	})
	// during document replication, oplog syncer fetch oplog and store on disk, in order to avoid oplog roll up
	// fullSyncFinishPosition means no need to check the end time to disable DDL
	if err := coordinator.startOplogReplication(fullBeginTs, int64(0), nil); err != nil {
		l.Logger.Criticalf("start oplog replication failed: %v", err)
		return err
	}
	// wait for document replication to finish, set docEndTs to oplog syncer, start oplog replication
	docWg.Wait()
	if docError != nil {
		return docError
	}
	l.Logger.Infof("finish document replication, change oplog replication to %v",
		utils.LogFetchStage(utils.FetchStageStoreDiskApply))
	for _, syncer := range coordinator.syncerGroup {
		syncer.StartDiskApply()
	}
	return nil
}
