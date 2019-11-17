package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/filter"
	"sync"

	"mongoshake/collector/configure"
	"mongoshake/collector/docsyncer"
	"mongoshake/collector/transform"
	"mongoshake/common"
	"mongoshake/oplog"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
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
	opts, _ := json.Marshal(conf.Options)
	LOG.Info("Collector configuration %s", string(opts))

	coordinator.sentinel = &utils.Sentinel{}
	coordinator.sentinel.Register()

	syncMode, fullBeginTs, err := coordinator.selectSyncMode(conf.Options.SyncMode)
	if err != nil {
		return err
	}

	/*
	 * Generally speaking, it's better to use several bridge timestamp so that
	 * each shard match one in sharding mode.
	 * TODO
	 */
	LOG.Info("start running with mode[%v], fullBeginTs[%v]", syncMode, fullBeginTs)

	switch syncMode {
	case SYNCMODE_ALL:
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}

		// get current newest timestamp
		_, fullFinishTs, _, bigOldTs, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			return LOG.Critical("get full sync finish timestamp failed[%v]", err)
		}
		LOG.Info("------------------------full sync done!------------------------")

		LOG.Info("bigOldTs[%v] fullBeginTs[%v] fullFinishTs[%v]", utils.ExtractTimestamp(bigOldTs),
			utils.ExtractTimestamp(fullBeginTs), utils.ExtractTimestamp(fullFinishTs))
		// the oldest oplog is lost
		if utils.ExtractTimestamp(bigOldTs) >= fullBeginTs {
			return LOG.Critical("incr sync fullBeginTs[%v] is less than current bigOldTs[%v], this error means user's "+
				"oplog collection size is too small or full sync continues too long", fullBeginTs, bigOldTs)
		}

		LOG.Info("finish full sync, start incr sync with timestamp: fullBeginTs[%v], fullFinishTs[%v]",
			utils.ExtractTimestamp(fullBeginTs), utils.ExtractTimestamp(fullFinishTs))

		if err := coordinator.startOplogReplication(fullBeginTs, utils.TimestampToInt64(fullFinishTs)); err != nil {
			return err
		}
	case SYNCMODE_DOCUMENT:
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
	case SYNCMODE_OPLOG:
		beginTs := conf.Options.ContextStartPosition
		if beginTs != 0 {
			// get current oldest timestamp
			_, _, _, bigOldTs, _, err := utils.GetAllTimestamp(coordinator.Sources)
			if err != nil {
				return LOG.Critical("get oldest timestamp failed[%v]", err)
			}
			// the oldest oplog is lost
			if utils.ExtractTimestamp(bigOldTs) >= beginTs {
				return LOG.Critical("incr sync beginTs[%v] is less than current bigOldTs[%v], this error means user's "+
					"oplog collection size is too small or full sync continues too long", beginTs, bigOldTs)
			}
		}
		if err := coordinator.startOplogReplication(beginTs, beginTs); err != nil {
			return err
		}
	default:
		return LOG.Critical("unknown sync mode %v", conf.Options.SyncMode)
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
		LOG.Critical("Connect storageUrl[%v] error[%v]. Please add primary node into 'mongo_urls' "+
			"if 'context.storage.url' is empty", storageUrl, err)
		return err
	}
	conn.Close()

	csUrl := conf.Options.MongoCsUrl
	if csUrl != "" {
		// try to connect MongoCsUrl
		if conn, err = utils.NewMongoConn(csUrl, utils.ConnectModePrimary, true); conn == nil || !conn.IsGood() || err != nil {
			LOG.Critical("Connect MongoCsUrl[%v] error[%v].", csUrl, err)
			return err
		}
		conn.Close()
	}

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
		src.Replset = rsName

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
	tsMap, _, smallNewTs, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return syncMode, 0, err
	}

	needFull := false
	ckptMap, err := docsyncer.LoadCheckpoint()
	if err != nil {
		return syncMode, 0, err
	}

	for replset, ts := range tsMap {
		if _, ok := ckptMap[replset]; !ok {
			continue
		}
		if ts.Oldest >= ckptMap[replset] {
			// checkpoint less than the oldest timestamp
			needFull = true
			break
		}
	}

	if needFull {
		return SYNCMODE_ALL, utils.TimestampToInt64(smallNewTs), nil
	} else {
		LOG.Info("sync mode change from 'all' to 'oplog'")
		return SYNCMODE_OPLOG, 0, nil
	}
}

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	shardingChunkMap := make(utils.ShardingChunkMap)
	fromIsSharding := len(coordinator.Sources) > 1
	if fromIsSharding {
		ok, _ := utils.GetBalancerStatusByUrl(conf.Options.MongoCsUrl)
		if ok {
			LOG.Critical("source mongodb sharding need to stop balancer when document replication occur")
			return errors.New("source mongodb sharding need to stop balancer when document replication occur")
		}
		var err error
		if shardingChunkMap, err = utils.GetChunkMapByUrl(conf.Options.MongoCsUrl); err != nil {
			return err
		}
	}

	// get all namespace need to sync
	nsSet, err := docsyncer.GetAllNamespace(coordinator.Sources)
	if err != nil {
		return err
	}

	ckptMap := make(map[string]bson.MongoTimestamp)
	// get all newest timestamp for each mongodb if sync mode isn't "document"
	if conf.Options.SyncMode != SYNCMODE_DOCUMENT {
		tsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			return err
		}
		for replset, tsNode := range tsMap {
			ckptMap[replset] = tsNode.Newest
		}
	}

	toUrl := conf.Options.TunnelAddress[0]
	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(toUrl, utils.ConnectModePrimary, true); err != nil {
		return err
	}
	defer toConn.Close()

	trans := transform.NewNamespaceTransform(conf.Options.TransformNamespace)

	shardingSync := docsyncer.IsShardingToSharding(fromIsSharding, toConn)
	nsExistedSet, err := docsyncer.StartDropDestCollection(nsSet, toConn, trans)
	if err != nil {
		return err
	}
	if shardingSync {
		if err := docsyncer.StartNamespaceSpecSyncForSharding(conf.Options.MongoCsUrl, toConn, nsExistedSet, trans); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	var replError error
	var mutex sync.Mutex
	indexMap := make(map[utils.NS][]mgo.Index)

	for _, src := range coordinator.Sources {
		dbChunkMap := make(utils.DBChunkMap)
		if chunkMap, ok := shardingChunkMap[src.Replset]; ok {
			dbChunkMap = chunkMap
		} else {
			LOG.Warn("document syncer %v has no chunk map", src.Replset)
		}
		orphanFilter := filter.NewOrphanFilter(src.Replset, dbChunkMap)

		dbSyncer := docsyncer.NewDBSyncer(src.Replset, src.URL, toUrl, trans, orphanFilter)
		LOG.Info("document syncer %v begin replication for url=%v", src.Replset, src.URL)
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

	if err := docsyncer.StartIndexSync(indexMap, toUrl, nsExistedSet, trans); err != nil {
		return err
	}

	// checkpoint after document syncer
	if conf.Options.SyncMode != SYNCMODE_DOCUMENT {
		LOG.Info("try to set checkpoint with map[%v]", ckptMap)
		if err := docsyncer.FlushCheckpoint(ckptMap); err != nil {
			LOG.Error("document syncer flush checkpoint failed. %v", err)
			return err
		}
	}
	LOG.Info("document syncer sync end")
	return nil
}

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition, fullSyncFinishPosition int64) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	ckptManager := NewCheckpointManager(oplogStartPosition)
	mvckManager := NewMoveChunkManager(ckptManager)
	ddlManager := NewDDLManager(ckptManager)

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	for _, src := range coordinator.Sources {
		syncer := NewOplogSyncer(coordinator, src.Replset, fullSyncFinishPosition,
			src.URL, src.Gids, ckptManager, mvckManager, ddlManager)
		// syncerGroup http api registry
		syncer.init()
		ckptManager.addOplogSyncer(syncer)
		if conf.Options.MoveChunkEnable {
			mvckManager.addOplogSyncer(syncer)
		}
		if DDLSupportForSharding() {
			ddlManager.addOplogSyncer(syncer)
		}
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

	// initialize checkpoint timestamp of oplog syncer
	if err := ckptManager.Load(); err != nil {
		return err
	}
	ckptManager.start()
	if conf.Options.MoveChunkEnable {
		mvckManager.start()
	}
	if DDLSupportForSharding() {
		ddlManager.start()
	}

	for _, syncer := range coordinator.syncerGroup {
		go syncer.start()
	}
	return nil
}

func DDLSupportForSharding() bool {
	return !conf.Options.ReplayerDMLOnly && conf.Options.MongoCsUrl != ""
}
