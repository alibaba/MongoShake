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
	SyncModeAll      = "all"
	SyncModeDocument = "document"
	SyncModeOplog    = "oplog"
)

// ReplicationCoordinator global coordinator instance. consist of
// one oplogSyncerGroup and a number of workers
type ReplicationCoordinator struct {
	Sources []*utils.MongoSource
	// Sentinel listener
	sentinel *utils.Sentinel

	// oplogSyncerGroup and workerGroup number is 1:N in ReplicaSet.
	// 1:1 while replicated in shard cluster
	oplogSyncerGroup []*OplogSyncer

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

	syncMode, beginAllTsMap, err := coordinator.selectSyncMode(conf.Options.SyncMode)
	if err != nil {
		return err
	}
	beginTsMap := make(map[string]bson.MongoTimestamp)
	for replset, ts := range beginAllTsMap {
		beginTsMap[replset] = ts.Newest
	}

	LOG.Info("start running with mode[%v]", syncMode)
	switch syncMode {
	case SyncModeAll:
		if err := docsyncer.CleanCheckpoint(); err != nil {
			return LOG.Critical("document replication clean checkpoint error. %v", err)
		}

		for replset, beginTs := range beginTsMap {
			LOG.Info("replset %v replication start with beginTs[%v]",
				replset, utils.ExtractTs32(beginTs))
		}

		if conf.Options.ReplayerOplogStoreDisk {
			if err := coordinator.parallelDocumentOplog(beginTsMap, beginAllTsMap); err != nil {
				return err
			}
		} else {
			if err := coordinator.serializeDocumentOplog(beginTsMap, beginAllTsMap); err != nil {
				return err
			}
		}
	case SyncModeDocument:
		if err := coordinator.startDocumentReplication(beginTsMap); err != nil {
			return err
		}
	case SyncModeOplog:
		beginTs32 := conf.Options.ContextStartPosition
		if beginTs32 != 0 {
			// get current oldest timestamp, the oldest oplog is lost
			for replset, beginTs := range beginAllTsMap {
				if beginTs32 >= utils.ExtractTs32(beginTs.Oldest) {
					return LOG.Critical("oplog replication replset %v startPosition[%v] is less than current beginTs.Oldest[%v], "+
						"this error means user's oplog collection size is too small or document replication continues too long",
						replset, beginTs32, utils.ExtractTs32(beginTs.Oldest))
				}
			}
		} else {
			// we can't insert Timestamp(0, 0) that will be treat as Now(), so we use Timestamp(1, 0)
			beginTs32 = 1
		}
		for replset := range beginTsMap {
			beginTsMap[replset] = bson.MongoTimestamp(beginTs32 << 32)
		}
		if err := coordinator.startOplogReplication(beginTsMap, beginTsMap); err != nil {
			return err
		}
	default:
		return LOG.Critical("unknown sync mode %v", conf.Options.SyncMode)
	}

	return nil
}

func (coordinator *ReplicationCoordinator) parallelDocumentOplog(
		beginTsMap map[string]bson.MongoTimestamp, beginAllTsMap map[string]utils.TimestampNode) error {
	LOG.Info("parallel run document and oplog replication")
	var docError, oplogError error
	docEndTsMap := make(map[string]bson.MongoTimestamp)
	var docWg, oplogWg sync.WaitGroup
	docWg.Add(1)
	oplogWg.Add(1)

	nimo.GoRoutine(func() {
		defer docWg.Done()
		if err := coordinator.startDocumentReplication(beginTsMap); err != nil {
			docError = LOG.Critical("Document Replication error. %v", err)
			return
		}
		// get current newest timestamp
		endAllTsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
		if err != nil {
			docError = LOG.Critical("document replication get end timestamp failed[%v]", err)
			return
		}
		LOG.Info("------------------------document replication done!------------------------")
		for replset, endTs := range endAllTsMap {
			beginTs := beginAllTsMap[replset]
			LOG.Info("document replication replset %v beginTs[%v] endTs[%v]",
				replset, utils.ExtractTs32(beginTs.Newest), utils.ExtractTs32(endTs.Newest))
			// the oldest oplog is lost
			if utils.ExtractTs32(endTs.Oldest) >= utils.ExtractTs32(beginTs.Newest) {
				docError = LOG.Critical("oplog replication replset %v beginTs.Newest[%v] is less than endTs.Oldest[%v], "+
					"this error means user's oplog collection size is too small or document replication continues too long",
					replset, utils.ExtractTs32(beginTs.Newest), utils.ExtractTs32(endTs.Oldest))
				return
			}
			docEndTsMap[replset] = endTs.Newest
		}
	})
	// during document replication, oplog syncer fetch oplog and store on disk, in order to avoid oplog roll up
	nimo.GoRoutine(func() {
		defer oplogWg.Done()
		if err := coordinator.startOplogReplication(beginTsMap, nil); err != nil {
			oplogError = LOG.Critical("Oplog Replication error. %v", err)
			return
		}
	})
	// wait for document replication to finish, set docEndTs to oplog syncer, start oplog replication
	docWg.Wait()
	if docError != nil {
		return docError
	}
	LOG.Info("finish document replication, start oplog replication")
	for _, oplogSyncer := range coordinator.oplogSyncerGroup {
		oplogSyncer.startDiskApply(docEndTsMap[oplogSyncer.replset])
	}
	// wait for oplog replication to finish
	oplogWg.Wait()
	if oplogError != nil {
		return oplogError
	}
	return nil
}

func (coordinator *ReplicationCoordinator) serializeDocumentOplog(
		beginTsMap map[string]bson.MongoTimestamp, beginAllTsMap map[string]utils.TimestampNode) error {
	LOG.Info("serially run document and oplog replication")
	if err := coordinator.startDocumentReplication(beginTsMap); err != nil {
		return LOG.Critical("Document Replication error. %v", err)
	}

	// get current newest timestamp
	endAllTsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return LOG.Critical("document replication get end timestamp failed[%v]", err)
	}
	LOG.Info("------------------------document replication done!------------------------")
	docEndTsMap := make(map[string]bson.MongoTimestamp)
	for replset, endTs := range endAllTsMap {
		beginTs := beginAllTsMap[replset]
		LOG.Info("document replication replset %v beginTs[%v] endTs[%v]",
			replset, utils.ExtractTs32(beginTs.Newest), utils.ExtractTs32(endTs.Newest))
		// the oldest oplog is lost
		if utils.ExtractTs32(endTs.Oldest) >= utils.ExtractTs32(beginTs.Newest) {
			return LOG.Critical("oplog replication replset %v beginTs.Newest[%v] is less than endTs.Oldest[%v], "+
				"this error means user's oplog collection size is too small or document replication continues too long",
				replset, utils.ExtractTs32(beginTs.Newest), utils.ExtractTs32(endTs.Oldest))
		}
		docEndTsMap[replset] = endTs.Newest
	}

	if err := coordinator.startOplogReplication(beginTsMap, docEndTsMap); err != nil {
		return LOG.Critical("Oplog Replication error. %v", err)
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
		return LOG.Critical("Connect storageUrl[%v] error[%v]. Please check context.storage.url connection using primary mode", storageUrl, err)
	}
	conn.Close()

	csUrl := conf.Options.MongoCsUrl
	if csUrl != "" {
		// try to connect MongoCsUrl
		if conn, err = utils.NewMongoConn(csUrl, utils.ConnectModePrimary, true); conn == nil || !conn.IsGood() || err != nil {
			return LOG.Critical("Connect MongoCsUrl[%v] error[%v]. Please check mongo_cs_url connection using primary mode", csUrl, err)
		}
		conn.Close()
	}

	for i, rawurl := range conf.Options.MongoUrls {
		url, params := utils.ParseMongoUrl(rawurl)
		coordinator.Sources[i] = new(utils.MongoSource)
		coordinator.Sources[i].URL = url
		if len(conf.Options.OplogGIDS) != 0 {
			coordinator.Sources[i].Gids = conf.Options.OplogGIDS
		}

		if conn, err = utils.NewMongoConn(url, conf.Options.MongoConnectMode, true); conn == nil || !conn.IsGood() || err != nil {
			return LOG.Critical("Connect mongo server from url[%v] error. %v. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-reachable-servers-error", url, err)
		}

		// a conventional ReplicaSet should have local.oplog.rs collection
		if conf.Options.SyncMode != SyncModeDocument && !conn.HasOplogNs() {
			conn.Close()
			return LOG.Critical("no oplog ns in mongo. See https://github.com/alibaba/MongoShake/wiki/FAQ#q-how-to-solve-the-oplog-tailer-initialize-failed-no-oplog-ns-in-mongo-error")
		}

		// check if there has dup server every replica set in RS or Shard
		rsName, err := conn.AcquireReplicaSetName()
		// rsName will be set to default if empty
		if err != nil {
			if conf.Options.FilterOrphanDocument {
				var ok bool
				if rsName, ok = params["replicaSet"]; !ok {
					conn.Close()
					return LOG.Critical("acquire replica set name from url[%v] by replSetGetStatus failed. %v", url, err)
				}
			} else {
				rsName = fmt.Sprintf("default-%d", i)
				LOG.Warn("Source mongodb have empty replica set name, url[%s], change to default[%s]", url, rsName)
			}
		}

		if _, exist := rs[rsName]; exist {
			conn.Close()
			return LOG.Critical("There has duplicate replica set name : %s", rsName)
		}
		rs[rsName] = 1
		coordinator.Sources[i].Replset = rsName

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
func (coordinator *ReplicationCoordinator) selectSyncMode(syncMode string) (string, map[string]utils.TimestampNode, error) {
	// oldestTs is the smallest of the all newest timestamp
	tsMap, _, _, _, _, err := utils.GetAllTimestamp(coordinator.Sources)
	if err != nil {
		return syncMode, nil, err
	}

	if syncMode != SyncModeAll {
		return syncMode, tsMap, nil
	}

	needFull := false
	ckptMap, err := docsyncer.LoadCheckpoint()
	if err != nil {
		return syncMode, nil, err
	}

	for replset, ts := range tsMap {
		if _, ok := ckptMap[replset]; !ok {
			needFull = true
			break
		}
		if ts.Oldest >= ckptMap[replset] {
			// checkpoint less than the oldest timestamp
			needFull = true
			break
		}
	}

	if needFull {
		return SyncModeAll, tsMap, nil
	} else {
		LOG.Info("sync mode change from 'all' to 'oplog'")
		return SyncModeOplog, tsMap, nil
	}
}

func (coordinator *ReplicationCoordinator) startDocumentReplication(beginTsMap map[string]bson.MongoTimestamp) error {
	shardingChunkMap := make(utils.ShardingChunkMap)
	fromIsSharding := len(coordinator.Sources) > 1
	if fromIsSharding {
		ok, err := utils.GetBalancerStatusByUrl(conf.Options.MongoCsUrl)
		if ok {
			if err != nil {
				return LOG.Critical("obtain balance status from mongo_cs_url=%s error. %v", err)
			}
			return LOG.Critical("source mongodb sharding need to stop balancer when document replication occur.")
		}
		if conf.Options.FilterOrphanDocument {
			if shardingChunkMap, err = utils.GetChunkMapByUrl(conf.Options.MongoCsUrl); err != nil {
				return err
			}
		}
	}

	// get all namespace need to sync
	nsSet, err := docsyncer.GetAllNamespace(coordinator.Sources)
	if err != nil {
		return err
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
		var orphanFilter *filter.OrphanFilter
		if conf.Options.FilterOrphanDocument {
			dbChunkMap := make(utils.DBChunkMap)
			if chunkMap, ok := shardingChunkMap[src.Replset]; ok {
				dbChunkMap = chunkMap
			} else {
				LOG.Warn("document syncer %v has no chunk map", src.Replset)
			}
			orphanFilter = filter.NewOrphanFilter(src.Replset, dbChunkMap)
		}

		dbSyncer := docsyncer.NewDBSyncer(src.Replset, src.URL, toUrl, trans, orphanFilter)
		LOG.Info("document syncer %v begin replication for url=%v", src.Replset, src.URL)
		wg.Add(1)
		nimo.GoRoutine(func() {
			defer wg.Done()
			if err := dbSyncer.Start(); err != nil {
				replError = LOG.Critical("document replication for url=%v failed. %v", src.URL, err)
				return
			}
			mutex.Lock()
			defer mutex.Unlock()
			for ns, indexList := range dbSyncer.GetIndexMap() {
				indexMap[ns] = indexList
			}
		})
	}
	wg.Wait()
	if err := docsyncer.StartIndexSync(indexMap, toUrl, nsExistedSet, trans); err != nil {
		return err
	}
	if replError != nil {
		return replError
	}

	// checkpoint after document syncer
	LOG.Info("document syncer do checkpoint with map[%v]", beginTsMap)
	if err := docsyncer.FlushCheckpoint(beginTsMap); err != nil {
		return LOG.Error("document syncer flush checkpoint failed. %v", err)
	}
	LOG.Info("document syncer sync end")
	return nil
}

func (coordinator *ReplicationCoordinator) startOplogReplication(beginTsMap, docEndTsMap map[string]bson.MongoTimestamp) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	ckptManager := NewCheckpointManager(beginTsMap)
	mvckManager := NewMoveChunkManager(ckptManager)
	ddlManager := NewDDLManager(ckptManager)

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	for _, src := range coordinator.Sources {
		syncer := NewOplogSyncer(coordinator, src.Replset, docEndTsMap,
			src.URL, src.Gids, ckptManager, mvckManager, ddlManager)
		// oplogSyncerGroup http api registry
		syncer.init()
		ckptManager.addOplogSyncer(syncer)
		if conf.Options.MoveChunkEnable {
			mvckManager.addOplogSyncer(syncer)
		}
		if DDLSupportForSharding() {
			ddlManager.addOplogSyncer(syncer)
		}
		coordinator.oplogSyncerGroup = append(coordinator.oplogSyncerGroup, syncer)
	}

	// prepare worker routine and bind it to syncer
	for i := 0; i != conf.Options.WorkerNum; i++ {
		syncer := coordinator.oplogSyncerGroup[i%len(coordinator.oplogSyncerGroup)]
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
	if docEndTsMap != nil {
		// oplog start applying oplog
		ckptManager.start()
	}
	if conf.Options.MoveChunkEnable {
		mvckManager.start()
	}
	if DDLSupportForSharding() {
		ddlManager.start()
	}

	for _, syncer := range coordinator.oplogSyncerGroup {
		go syncer.start()
	}
	return nil
}

func DDLSupportForSharding() bool {
	return !conf.Options.ReplayerDMLOnly && conf.Options.MongoCsUrl != ""
}
