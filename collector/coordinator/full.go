package coordinator

import (
	"fmt"
	"math"
	"sync"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/docsyncer"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	"github.com/alibaba/MongoShake/v2/collector/transform"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/sharding"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
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

func getTimestampMap(sources []*utils.MongoSource, sslRootFile string) (map[string]utils.TimestampNode, error) {
	// no need to fetch if sync mode is full only
	if conf.Options.SyncMode == utils.VarSyncModeFull {
		return nil, nil
	}

	var ckptMap map[string]utils.TimestampNode
	var err error

	ckptMap, _, _, _, _, err = utils.GetAllTimestamp(sources, sslRootFile)
	if err != nil {
		return nil, fmt.Errorf("fetch source all timestamp failed: %v", err)
	}

	return ckptMap, nil
}

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {

	fromIsSharding := coordinator.SourceIsSharding()

	var shardingChunkMap sharding.ShardingChunkMap
	var err error
	// init orphan sharding chunk map if source is mongod(get data directly from mongod)
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

	filterList := filter.NewDocFilterList()
	// get all namespace need to sync
	nsSet, _, err := utils.GetAllNamespace(coordinator.RealSourceFullSync, filterList.IterateFilter,
		conf.Options.MongoSslRootCaFile)
	if err != nil {
		return err
	}
	LOG.Info("all namespace: %v", nsSet)

	var ckptMap map[string]utils.TimestampNode
	if conf.Options.SpecialSourceDBFlag != utils.VarSpecialSourceDBFlagAliyunServerless && len(coordinator.MongoD) > 0 {
		// get current newest timestamp
		ckptMap, err = getTimestampMap(coordinator.MongoD, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return err
		}
	}

	// create target client
	toUrl := conf.Options.TunnelAddress[0]
	var toConn *utils.MongoCommunityConn
	if !conf.Options.FullSyncExecutorDebug {
		if toConn, err = utils.NewMongoCommunityConn(toUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, conf.Options.TunnelMongoSslRootCaFile); err != nil {
			return err
		}
		defer toConn.Close()
	}

	// create namespace transform
	trans := transform.NewNamespaceTransform(conf.Options.TransformNamespace)

	// drop target collection if possible
	if err := docsyncer.StartDropDestCollection(nsSet, toConn, trans); err != nil {
		return err
	}

	// enable shard if sharding -> sharding
	shardingSync := docsyncer.IsShardingToSharding(fromIsSharding, toConn)
	if shardingSync {
		var connString string
		if len(conf.Options.MongoSUrl) > 0 {
			connString = conf.Options.MongoSUrl
		} else {
			connString = conf.Options.MongoCsUrl
		}
		if err := docsyncer.StartNamespaceSpecSyncForSharding(connString, toConn, trans); err != nil {
			return err
		}
	}

	// fetch all indexes
	var indexMap map[utils.NS][]bson.D
	if conf.Options.FullSyncCreateIndex != utils.VarFullSyncCreateIndexNone {
		if indexMap, err = fetchIndexes(coordinator.RealSourceFullSync, filterList.IterateFilter); err != nil {
			return fmt.Errorf("fetch index failed[%v]", err)
		}

		// print
		LOG.Info("index list below: ----------")
		for ns, index := range indexMap {
			// LOG.Info("collection[%v] -> %s", ns, utils.MarshalStruct(index))
			LOG.Info("collection[%v] -> %v", ns, index)
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
				LOG.Critical("document replication for url=%v failed. %v",
					utils.BlockMongoUrlPassword(src.URL, "***"), err)
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
	// do not update checkpoint when source is "aliyun_serverless"
	if conf.Options.SyncMode != utils.VarSyncModeFull && conf.Options.SpecialSourceDBFlag != utils.VarSpecialSourceDBFlagAliyunServerless {
		// need merge to one when from mongos and fetch_mothod=="change_stream"
		if coordinator.MongoS != nil && conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodChangeStream {
			var smallestNew int64 = math.MaxInt64
			for _, val := range ckptMap {
				if smallestNew > val.Newest {
					smallestNew = val.Newest
				}
			}
			ckptMap = map[string]utils.TimestampNode{
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
