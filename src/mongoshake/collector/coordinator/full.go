package coordinator

import (
	"fmt"
	"sync"

	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/sharding"
	"mongoshake/collector/filter"
	"mongoshake/collector/docsyncer"
	"mongoshake/collector/transform"

	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo"
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

	ckptMap, _, _, _, _, _, err = utils.GetAllTimestamp(sources)
	if err != nil {
		return nil, fmt.Errorf("fetch source all timestamp failed: %v", err)
	}
	return ckptMap, nil
}

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	// for change stream, we need to fetch current timestamp
	fromConn0, err := utils.NewMongoConn(coordinator.Sources[0].URL, utils.VarMongoConnectModePrimary, true)
	if err != nil {
		return fmt.Errorf("connect soruce[%v] failed[%v]", coordinator.Sources[0].URL, err)
	}
	defer fromConn0.Close()

	// the source is sharding or replica-set
	fromIsSharding := len(coordinator.Sources) > 1 || fromConn0.IsMongos()

	// init orphan sharding chunk map if source is sharding
	shardingChunkMap, err := fetchChunkMap(fromIsSharding)
	if err != nil {
		LOG.Critical("fetch chunk map failed[%v]", err)
		return err
	}

	// get all namespace need to sync
	nsSet, err := docsyncer.GetAllNamespace(coordinator.Sources)
	if err != nil {
		return err
	}

	// get current newest timestamp
	ckptMap, err := getTimestampMap(coordinator.Sources)
	if err != nil {
		return err
	}

	// create target client
	toUrl := conf.Options.IncrSyncTunnelAddress[0]
	var toConn *utils.MongoConn
	if toConn, err = utils.NewMongoConn(toUrl, utils.VarMongoConnectModePrimary, true); err != nil {
		return err
	}
	defer toConn.Close()

	// crate namespace transform
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

	// create index if enable, current we only support foreground index
	if conf.Options.FullSyncCreateIndex == "foreground" {
		if err := docsyncer.StartIndexSync(indexMap, toUrl, trans); err != nil {
			return fmt.Errorf("create index failed[%v]", err)
		}
	}

	// update checkpoint after full sync
	if conf.Options.SyncMode != utils.VarSyncModeFull {
		LOG.Info("try to set checkpoint with map[%v]", ckptMap)
		if err := docsyncer.Checkpoint(ckptMap); err != nil {
			return err
		}
	}

	LOG.Info("document syncer sync end")
	return nil
}
