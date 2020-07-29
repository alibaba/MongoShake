package coordinator

import (
	"fmt"
	"sync"
	"math"

	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/sharding"
	"mongoshake/collector/filter"
	"mongoshake/collector/docsyncer"
	"mongoshake/collector/transform"

	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

func fetchChunkMap(isSharding bool) (sharding.ShardingChunkMap, error) {
	// return directly if source is replica set or fetch method is change stream
	if !isSharding || conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		return nil, nil
	}

	ok, err := sharding.GetBalancerStatusByUrl(conf.Options.MongoCsUrl)
	if err != nil {
		return nil, fmt.Errorf("obtain balance status from mongo_cs_url=%s error. %v",
			conf.Options.MongoCsUrl, err)
	}
	if ok {
		return nil, fmt.Errorf("source mongodb sharding need to stop balancer when document replication occur")
	}

	// enable filter orphan document
	if conf.Options.FullSyncExecutorFilterOrphanDocument {
		LOG.Info("begin to get chunk map from config.chunks of source mongodb sharding")
		return sharding.GetChunkMapByUrl(conf.Options.MongoCsUrl)
	}

	return nil, nil
}

func getTimestampMap(sources []*utils.MongoSource) (map[string]utils.TimestampNode, error) {
	// no need to fetch if sync mode is full only
	if conf.Options.SyncMode == utils.VarSyncModeFull {
		return nil, nil
	}

	var ckptMap map[string]utils.TimestampNode
	var err error

	ckptMap, _, _, _, _, err = utils.GetAllTimestamp(sources)
	if err != nil {
		return nil, fmt.Errorf("fetch source all timestamp failed: %v", err)
	}

	return ckptMap, nil
}

/*
 * fetch all indexes.
 * the cost is low so that no need to run in parallel.
 */
func fetchIndexes(sourceList []*utils.MongoSource) (map[utils.NS][]mgo.Index, error) {
	var mutex sync.Mutex
	indexMap := make(map[utils.NS][]mgo.Index)
	for _, src := range sourceList {
		LOG.Info("source[%v %v] start fetching index", src.ReplicaName, src.URL)
		// 1. fetch namespace
		nsList, _, err := docsyncer.GetDbNamespace(src.URL)
		if err != nil {
			return nil, fmt.Errorf("source[%v %v] get namespace failed: %v", src.ReplicaName, src.URL, err)
		}

		// 2. build connection
		conn, err := utils.NewMongoConn(src.URL, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault)
		if err != nil {
			return nil, fmt.Errorf("source[%v %v] build connection failed: %v", src.ReplicaName, src.URL, err)
		}
		defer conn.Close() // it's acceptable to call defer here

		// 3. fetch all indexes
		for _, ns := range nsList {
			indexes, err := conn.Session.DB(ns.Database).C(ns.Collection).Indexes()
			if err != nil {
				return nil, fmt.Errorf("source[%v %v] fetch index failed: %v", src.ReplicaName, src.URL, err)
			}

			mutex.Lock()
			indexMap[ns] = indexes
			mutex.Unlock()
		}

		LOG.Info("source[%v %v] finish fetching index", src.ReplicaName, src.URL)
	}
	return indexMap, nil
}

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	// for change stream, we need to fetch current timestamp
	/*fromConn0, err := utils.NewMongoConn(coordinator.Sources[0].URL, utils.VarMongoConnectModePrimary, true)
	if err != nil {
		return fmt.Errorf("connect soruce[%v] failed[%v]", coordinator.Sources[0].URL, err)
	}
	defer fromConn0.Close()*/

	// the source is sharding or replica-set
	// fromIsSharding := len(coordinator.Sources) > 1 || fromConn0.IsMongos()

	fromIsSharding := coordinator.SourceIsSharding()

	var shardingChunkMap sharding.ShardingChunkMap
	var err error
	// init orphan sharding chunk map if source is mongod
	if fromIsSharding && coordinator.MongoS == nil {
		LOG.Info("source is mongod, need to fetching chunk map")
		shardingChunkMap, err = fetchChunkMap(fromIsSharding)
		if err != nil {
			LOG.Critical("fetch chunk map failed[%v]", err)
			return err
		}
	} else {
		LOG.Info("source is replica or mongos, no need to fetching chunk map")
	}

	// get all namespace need to sync
	nsSet, _, err := docsyncer.GetAllNamespace(coordinator.RealSourceFullSync)
	if err != nil {
		return err
	}
	LOG.Info("all namespace: %v", nsSet)

	// get current newest timestamp
	ckptMap, err := getTimestampMap(coordinator.MongoD)
	if err != nil {
		return err
	}

	// create target client
	toUrl := conf.Options.TunnelAddress[0]
	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(toUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault); err != nil {
		return err
	}
	defer toConn.Close()

	// create namespace transform
	trans := transform.NewNamespaceTransform(conf.Options.TransformNamespace)

	// drop target collection if possible
	if err := docsyncer.StartDropDestCollection(nsSet, toConn, trans); err != nil {
		return err
	}

	// enable shard if sharding -> sharding
	shardingSync := docsyncer.IsShardingToSharding(fromIsSharding, toConn)
	if shardingSync {
		if err := docsyncer.StartNamespaceSpecSyncForSharding(conf.Options.MongoCsUrl, toConn, trans); err != nil {
			return err
		}
	}

	// fetch all indexes
	var indexMap map[utils.NS][]mgo.Index
	if conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexNone {
		if indexMap, err = fetchIndexes(coordinator.RealSourceFullSync); err != nil {
			return fmt.Errorf("fetch index failed[%v]", err)
		}

		// print
		LOG.Info("index list below: ----------")
		for ns, index := range indexMap {
			LOG.Info("collection[%v] -> %s", ns, utils.MarshalStruct(index))
		}
		LOG.Info("index list above: ----------")

		if conf.Options.FullSyncCreateIndex == utils.VarFullSyncCreateIndexBackground {
			if err := docsyncer.StartIndexSync(indexMap, toUrl, trans, true); err != nil {
				return fmt.Errorf("create background index failed[%v]", err)
			}
		}
	}

	// global qps limit, all dbsyncer share only 1 Qos
	qos := utils.StartQoS(0, int64(conf.Options.FullSyncReaderDocumentBatchSize), &utils.FullSentinelOptions.TPS)

	// start sync each db
	var wg sync.WaitGroup
	var replError error
	for i, src := range coordinator.RealSourceFullSync {
		var orphanFilter *filter.OrphanFilter
		if conf.Options.FullSyncExecutorFilterOrphanDocument && shardingChunkMap != nil {
			dbChunkMap := make(sharding.DBChunkMap)
			if chunkMap, ok := shardingChunkMap[src.ReplicaName]; ok {
				dbChunkMap = chunkMap
			} else {
				LOG.Warn("document syncer %v has no chunk map", src.ReplicaName)
			}
			orphanFilter = filter.NewOrphanFilter(src.ReplicaName, dbChunkMap)
		}

		dbSyncer := docsyncer.NewDBSyncer(i, src.URL, src.ReplicaName, toUrl, trans, orphanFilter, qos, fromIsSharding)
		dbSyncer.Init()
		LOG.Info("document syncer-%d do replication for url=%v", i, src.URL)

		wg.Add(1)
		nimo.GoRoutine(func() {
			defer wg.Done()
			if err := dbSyncer.Start(); err != nil {
				LOG.Critical("document replication for url=%v failed. %v", src.URL, err)
				replError = err
			}
			dbSyncer.Close()
		})
	}

	// start http server.
	nimo.GoRoutine(func() {
		// before starting, we must register all interface
		if err := utils.FullSyncHttpApi.Listen(); err != nil {
			LOG.Critical("start full sync server with port[%v] failed: %v", conf.Options.FullSyncHTTPListenPort,
				err)
		}
	})

	// wait all db finished
	wg.Wait()
	if replError != nil {
		return replError
	}

	// create index if == foreground
	if conf.Options.FullSyncCreateIndex == utils.VarFullSyncCreateIndexForeground {
		if err := docsyncer.StartIndexSync(indexMap, toUrl, trans, false); err != nil {
			return fmt.Errorf("create forground index failed[%v]", err)
		}
	}

	// update checkpoint after full sync
	if conf.Options.SyncMode != utils.VarSyncModeFull {
		// need merge to one when from mongos and fetch_mothod=="change_stream"
		if coordinator.MongoS != nil && conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
			var smallestNew bson.MongoTimestamp = math.MaxInt64
			for _, val := range ckptMap {
				if smallestNew > val.Newest {
					smallestNew = val.Newest
				}
			}
			ckptMap = map[string]utils.TimestampNode {
				coordinator.MongoS.ReplicaName: {
					Newest: smallestNew,
				},
			}
		}

		LOG.Info("try to set checkpoint with map[%v]", ckptMap)
		if err := docsyncer.Checkpoint(ckptMap); err != nil {
			return err
		}
	}

	LOG.Info("document syncer sync end")
	return nil
}

func (coordinator *ReplicationCoordinator) SourceIsSharding() bool {
	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
		return coordinator.MongoS != nil
	} else {
		return len(conf.Options.MongoUrls) > 1
	}
}