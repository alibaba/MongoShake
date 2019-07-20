package collector

import (
	"encoding/json"
	"errors"
	"fmt"
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
		_, fullFinishTs, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			return fmt.Errorf("get full sync finish timestamp failed[%v]", err)
		}
		LOG.Info("finish full sync, start incr sync with timestamp: fullBeginTs[%v], fullFinishTs[%v]",
			utils.ExtractMongoTimestamp(fullBeginTs), utils.ExtractMongoTimestamp(fullFinishTs))

		if err := coordinator.startOplogReplication(fullBeginTs, utils.TimestampToInt64(fullFinishTs)); err != nil {
			return err
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
		LOG.Critical("Connect storageUrl[%v] error[%v].", storageUrl, err)
		return err
	}
	conn.Close()

	for i, src := range coordinator.Sources {
		if conn, err = utils.NewMongoConn(src.URL, conf.Options.MongoConnectMode, true); conn == nil || !conn.IsGood() || err != nil {
			LOG.Critical("Connect mongo server error. %v, url : %s", err, src.URL)
			return err
		}
		// a conventional ReplicaSet should have local.oplog.rs collection
		if !conn.HasOplogNs() {
			LOG.Critical("There has no oplog collection in mongo db server")
			conn.Close()
			return errors.New("no oplog ns in mongo")
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
	tsMap, _, oldestTs, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return syncMode, 0, nil
	}

	needFull := false
	ckptManager := NewCheckpointManager(0)
	if err := ckptManager.Load(); err != nil {
		return "", 0, err
	}
	for replset, ts := range tsMap {
		if ts.Oldest >= ckptManager.Get(replset) {
			// checkpoint less than the oldest timestamp
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

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	// get all namespace need to sync
	nsSet, err := docsyncer.GetAllNamespace(coordinator.Sources)
	if err != nil {
		return err
	}
	// get all newest timestamp for each mongodb
	ckptMap, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return err
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
		dbSyncer := docsyncer.NewDBSyncer(i, src.URL, toUrl, trans)
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

	if err := docsyncer.StartIndexSync(indexMap, toUrl, trans); err != nil {
		return err
	}

	// checkpoint after document syncer
	ckptManager := NewCheckpointManager(0)
	for replset, ts := range ckptMap {
		ckptManager.Update(replset, ts.Newest)
	}
	if err := ckptManager.Flush(); err != nil {
		LOG.Error("document syncer flush checkpoint failed. %v", err)
		return err
	}

	LOG.Info("document syncer sync end")
	return nil
}

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition, fullSyncFinishPosition int64) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	ckptManager := NewCheckpointManager(oplogStartPosition)
	mvckManager := NewMoveChunkManager()

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	for _, src := range coordinator.Sources {
		syncer := NewOplogSyncer(coordinator, src.ReplicaName, fullSyncFinishPosition,
			src.URL, src.Gid, ckptManager, mvckManager)
		// syncerGroup http api registry
		syncer.init()
		ckptManager.addOplogSyncer(syncer)
		mvckManager.addOplogSyncer(syncer)
		coordinator.syncerGroup = append(coordinator.syncerGroup, syncer)
	}

	if conf.Options.MoveChunkEnable {
		ckptManager.registerPersis(mvckManager)
	}
	// initialize checkpoint timestamp of oplog syncer
	if err := ckptManager.Load(); err != nil {
		return err
	}
	ckptManager.start()

	if conf.Options.MoveChunkEnable {
		mvckManager.start()
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
