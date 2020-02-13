package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/collector/docsyncer"
	"mongoshake/collector/transform"
	"mongoshake/common"
	"mongoshake/oplog"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"mongoshake/sharding"
	"mongoshake/collector/filter"
)

const (
	SYNCMODE_ALL      = "all"
	SYNCMODE_DOCUMENT = "document"
	SYNCMODE_OPLOG    = "oplog"
)

// ReplicationCoordinator global coordinator instance. consist of
// one syncerGroup and a number of workers
type ReplicationCoordinator struct {
	Sources []*utils.MongoSource
	// Sentinel listener
	sentinel *utils.Sentinel

	// syncerGroup and workerGroup number is 1:N in ReplicaSet.
	// 1:1 while replicated in shard cluster
	syncerGroup []*OplogSyncer

	rateController *nimo.SimpleRateController
}

func (coordinator *ReplicationCoordinator) Run() error {
	// check all mongodb deployment and fetch the instance info
	if err := coordinator.sanitizeMongoDB(); err != nil {
		return err
	}
	LOG.Info("Collector startup. shard_by[%s] gids[%s]", conf.Options.ShardKey, conf.Options.OplogGIDS)

	// all configurations has changed to immutable
	// opts, _ := json.Marshal(conf.Options)
	opts, _ := json.Marshal(conf.GetSafeOptions())
	LOG.Info("Collector configuration %s", string(opts))

	coordinator.sentinel = &utils.Sentinel{}
	coordinator.sentinel.Register()

	syncMode, fullBeginTs, err := coordinator.selectSyncMode(conf.Options.SyncMode)
	if err != nil {
		return err
	}
	fullBeginTs32 := utils.ExtractMongoTimestamp(fullBeginTs)

	/*
	 * Generally speaking, it's better to use several bridge timestamp so that
	 * each shard match one in sharding mode.
	 * TODO
	 */
	LOG.Info("start running with mode[%v], fullBeginTs[%v]", syncMode, fullBeginTs32)

	switch syncMode {
	case SYNCMODE_ALL:
		if conf.Options.FullSyncOplogStoreDisk {
			LOG.Info("run parallel document oplog")
			if err := coordinator.parallelDocumentOplog(fullBeginTs32); err != nil {
				return err
			}
		} else {
			LOG.Info("run serialize document oplog")
			if err := coordinator.serializeDocumentOplog(fullBeginTs32); err != nil {
				return err
			}
		}
	case SYNCMODE_DOCUMENT:
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
	case SYNCMODE_OPLOG:
		if err := coordinator.startOplogReplication(conf.Options.ContextStartPosition,
			conf.Options.ContextStartPosition); err != nil {
			return err
		}
	default:
		LOG.Critical("unknown sync mode %v", conf.Options.SyncMode)
		return errors.New("unknown sync mode " + conf.Options.SyncMode)
	}

	return nil
}

func (coordinator *ReplicationCoordinator) sanitizeMongoDB() error {
	var conn *utils.MongoConn
	var err error
	var hasUniqIndex = false
	rs := map[string]int{}

	// try to connect ContextStorageUrl
	storageUrl := conf.Options.ContextStorageUrl
	if conn, err = utils.NewMongoConn(storageUrl, utils.ConnectModePrimary, true); conn == nil || !conn.IsGood() || err != nil {
		LOG.Critical("Connect storageUrl[%v] error[%v]. Please add primary node into 'mongo_urls' " +
			"if 'context.storage.url' is empty", storageUrl, err)
		return err
	}
	conn.Close()

	for i, src := range coordinator.Sources {
		if conn, err = utils.NewMongoConn(src.URL, conf.Options.MongoConnectMode, true); conn == nil || !conn.IsGood() || err != nil {
			LOG.Critical("Connect mongo server error. %v, url : %s. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-reachable-servers-error", err, src.URL)
			return err
		}

		// a conventional ReplicaSet should have local.oplog.rs collection
		if conf.Options.SyncMode != SYNCMODE_DOCUMENT && !conn.HasOplogNs() {
			LOG.Critical("There has no oplog collection in mongo db server")
			conn.Close()
			return errors.New("no oplog ns in mongo. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-oplog-ns-in-mongo-error")
		}

		// check if there has dup server every replica set in RS or Shard
		rsName := conn.AcquireReplicaSetName()
		// rsName will be set to default if empty
		if rsName == "" {
			rsName = fmt.Sprintf("default-%d", i)
			LOG.Warn("Source mongodb have empty replica set name, url[%s], change to default[%s]", src.URL, rsName)
		}

		if _, exist := rs[rsName]; exist {
			LOG.Critical("There has duplicate replica set name : %s", rsName)
			conn.Close()
			return errors.New("duplicated replica set source")
		}
		rs[rsName] = 1
		src.ReplicaName = rsName

		// look around if there has uniq index
		if !hasUniqIndex {
			hasUniqIndex = conn.HasUniqueIndex()
		}
		// doesn't reuse current connection
		conn.Close()
	}

	// we choose sharding by collection if there are unique index
	// existing in collections
	if conf.Options.ShardKey == oplog.ShardAutomatic {
		if hasUniqIndex {
			conf.Options.ShardKey = oplog.ShardByNamespace
		} else {
			conf.Options.ShardKey = oplog.ShardByID
		}
	}

	return nil
}

// TODO, add UT
// if the oplog of checkpoint timestamp exist in all source db, then only do oplog replication instead of document replication
func (coordinator *ReplicationCoordinator) selectSyncMode(syncMode string) (string, int64, error) {
	if syncMode != SYNCMODE_ALL {
		return syncMode, 0, nil
	}

	// oldestTs is the smallest of the all newest timestamp
	tsMap, _, oldestTs, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return syncMode, 0, nil
	}

	needFull := false
	for replName, ts := range tsMap {
		ckptManager := ckpt.NewCheckpointManager(replName, 0)
		ckpt, _, err := ckptManager.Get()
		if err != nil {
			return "", 0, err
		}

		// checkpoint less than the oldest timestamp, ckpt.OplogDiskQueue == "" means not enable
		// disk persist
		if ts.Oldest >= ckpt.Timestamp && ckpt.OplogDiskQueue == "" {
			// check if disk queue enable
			needFull = true
			break
		}
	}

	if needFull {
		return SYNCMODE_ALL, utils.TimestampToInt64(oldestTs), nil
	} else {
		LOG.Info("sync mode change from 'all' to 'oplog'")
		return SYNCMODE_OPLOG, 0, nil
	}
}

// run incr-sync after full-sync
func (coordinator *ReplicationCoordinator) serializeDocumentOplog(fullBeginTs32 int64) error {
	if err := coordinator.startDocumentReplication(); err != nil {
		return fmt.Errorf("start document replication failed: %v", err)
	}

	// get current newest timestamp
	_, fullFinishTs, _, oldestTs, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return fmt.Errorf("get full sync finish timestamp failed[%v]", err)
	}
	LOG.Info("------------------------full sync done!------------------------")
	oldestTs32 := utils.ExtractMongoTimestamp(oldestTs)
	LOG.Info("oldestTs[%v] fullBeginTs[%v] fullFinishTs[%v]", oldestTs32, fullBeginTs32,
		utils.ExtractMongoTimestamp(fullFinishTs))
	// the oldest oplog is lost
	if oldestTs32 >= fullBeginTs32 {
		err = fmt.Errorf("incr sync ts[%v] is less than current oldest ts[%v], this error means user's " +
			"oplog collection size is too small or full sync continues too long", fullBeginTs32, oldestTs32)
		LOG.Error(err)
		return err
	}

	LOG.Info("finish full sync, start incr sync with timestamp: fullBeginTs[%v], fullFinishTs[%v]",
		fullBeginTs32, utils.ExtractMongoTimestamp(fullFinishTs))

	return coordinator.startOplogReplication(fullBeginTs32, utils.TimestampToInt64(fullFinishTs))
}

// run full-sync and incr-sync in parallel
func (coordinator *ReplicationCoordinator) parallelDocumentOplog(fullBeginTs32 int64) error {
	var docError error
	var docWg sync.WaitGroup
	docWg.Add(1)
	nimo.GoRoutine(func() {
		defer docWg.Done()
		if err := coordinator.startDocumentReplication(); err != nil {
			docError = LOG.Critical("document Replication error. %v", err)
			return
		}
		LOG.Info("------------------------document replication done!------------------------")

		/*
		// get current newest timestamp
		endAllTsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			docError = LOG.Critical("document replication get end timestamp failed[%v]", err)
			return
		}

		for replset, endTs := range endAllTsMap {
			beginTs := beginTsMap[replset]
			LOG.Info("document replication replset %v beginTs[%v] endTs[%v]",
				replset, utils.ExtractTs32(beginTs), utils.ExtractTs32(endTs.Newest))
			docEndTsMap[replset] = endTs.Newest
		}*/
	})
	// during document replication, oplog syncer fetch oplog and store on disk, in order to avoid oplog roll up
	// fullSyncFinishPosition means no need to check the end time to disable DDL
	if err := coordinator.startOplogReplication(fullBeginTs32, 0); err != nil {
		return LOG.Critical("start oplog replication failed: %v", err)
	}
	// wait for document replication to finish, set docEndTs to oplog syncer, start oplog replication
	docWg.Wait()
	if docError != nil {
		return docError
	}
	LOG.Info("finish document replication, change oplog replication to %v", utils.FetchStageStoreDiskApply)
	for _, syncer := range coordinator.syncerGroup {
		syncer.startDiskApply()
	}

	return nil
}

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	// init orphan sharding chunk map if source is sharding
	shardingChunkMap := make(sharding.ShardingChunkMap)
	if len(coordinator.Sources) > 1 {
		ok, err := sharding.GetBalancerStatusByUrl(conf.Options.MongoCsUrl)
		if ok {
			if err != nil {
				return LOG.Critical("obtain balance status from mongo_cs_url=%s error. %v",
					conf.Options.MongoCsUrl, err)
			}
			return LOG.Critical("source mongodb sharding need to stop balancer when document replication occur.")
		}

		if conf.Options.FullSyncExecutorFilterOrphanDocument {
			LOG.Info("begin to get chunk map from config.chunks of source mongodb sharding")
			if shardingChunkMap, err = sharding.GetChunkMapByUrl(conf.Options.MongoCsUrl); err != nil {
				return err
			}
		}
	}

	// get all namespace need to sync
	nsSet, err := docsyncer.GetAllNamespace(coordinator.Sources)
	if err != nil {
		return err
	}

	var ckptMap map[string]utils.TimestampNode
	// get all newest timestamp for each mongodb if sync mode isn't "document"
	if conf.Options.SyncMode != SYNCMODE_DOCUMENT {
		ckptMap, _, _, _, _, err = utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			return err
		}
	}

	fromIsSharding := len(coordinator.Sources) > 1
	toUrl := conf.Options.TunnelAddress[0]
	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(toUrl, utils.ConnectModePrimary, true); err != nil {
		return err
	}
	defer toConn.Close()

	trans := transform.NewNamespaceTransform(conf.Options.TransformNamespace)

	shardingSync := docsyncer.IsShardingToSharding(fromIsSharding, toConn)
	if err := docsyncer.StartDropDestCollection(nsSet, toConn, trans); err != nil {
		return err
	}
	if shardingSync {
		if err := docsyncer.StartNamespaceSpecSyncForSharding(conf.Options.ContextStorageUrl, toConn, trans); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	var replError error
	var mutex sync.Mutex
	indexMap := make(map[utils.NS][]mgo.Index)

	for i, src := range coordinator.Sources {
		var orphanFilter *filter.OrphanFilter
		if conf.Options.FullSyncExecutorFilterOrphanDocument {
			dbChunkMap := make(sharding.DBChunkMap)
			if chunkMap, ok := shardingChunkMap[src.ReplicaName]; ok {
				dbChunkMap = chunkMap
			} else {
				LOG.Warn("document syncer %v has no chunk map", src.ReplicaName)
			}
			orphanFilter = filter.NewOrphanFilter(src.ReplicaName, dbChunkMap)
		}

		dbSyncer := docsyncer.NewDBSyncer(i, src.URL, toUrl, trans, orphanFilter)
		LOG.Info("document syncer-%d do replication for url=%v", i, src.URL)
		wg.Add(1)
		nimo.GoRoutine(func() {
			defer wg.Done()
			if err := dbSyncer.Start(); err != nil {
				LOG.Critical("document replication for url=%v failed. %v", src.URL, err)
				replError = err
			}
			mutex.Lock()
			defer mutex.Unlock()
			for ns, indexList := range dbSyncer.GetIndexMap() {
				indexMap[ns] = indexList
			}
		})
	}
	wg.Wait()
	if replError != nil {
		return replError
	}

	if conf.Options.FullSyncCreateIndex {
		if err := docsyncer.StartIndexSync(indexMap, toUrl, trans); err != nil {
			return fmt.Errorf("create index failed[%v]", err)
		}
	}

	if conf.Options.SyncMode != SYNCMODE_DOCUMENT {
		LOG.Info("try to set checkpoint with map[%v]", ckptMap)
		if err := docsyncer.Checkpoint(ckptMap); err != nil {
			return err
		}
	}
	LOG.Info("document syncer sync end")
	return nil
}

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition, fullSyncFinishPosition int64) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	for _, src := range coordinator.Sources {
		// oplogStartPosition is 32 bits
		syncer := NewOplogSyncer(coordinator, src.ReplicaName, int32(oplogStartPosition), fullSyncFinishPosition, src.URL,
			src.Gids)
		// syncerGroup http api registry
		syncer.init()
		coordinator.syncerGroup = append(coordinator.syncerGroup, syncer)
	}

	// prepare worker routine and bind it to syncer
	for i := 0; i != conf.Options.WorkerNum; i++ {
		syncer := coordinator.syncerGroup[i%len(coordinator.syncerGroup)]
		w := NewWorker(coordinator, syncer, uint32(i))
		if !w.init() {
			return errors.New("worker initialize error")
		}

		// syncer and worker are independent. the relationship between
		// them needs binding here. one worker definitely belongs to a specific
		// syncer. However individual syncer could bind multi workers (if source
		// of overall replication is single mongodb replica)
		syncer.bind(w)
		go w.startWorker()
	}

	for _, syncer := range coordinator.syncerGroup {
		go syncer.start()
	}
	return nil
}
