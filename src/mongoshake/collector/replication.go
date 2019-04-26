package collector

import (
	"encoding/json"
	"errors"
	"github.com/vinllen/mgo"
	"mongoshake/collector/docsyncer"
	"sync"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/dbpool"
	"mongoshake/oplog"

	"fmt"
	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

// ReplicationCoordinator global coordinator instance. consist of
// one syncerGroup and a number of workers
type ReplicationCoordinator struct {
	Sources []*MongoSource
	// Sentinel listener
	sentinel *utils.Sentinel

	// syncerGroup and workerGroup number is 1:N in ReplicaSet.
	// 1:1 while replicated in shard cluster
	syncerGroup []*OplogSyncer

	rateController *nimo.SimpleRateController
}

type MongoSource struct {
	URL         string
	ReplicaName string
	Gid         string
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

	switch conf.Options.SyncMode {
	case "all":
		oplogStartPosition := time.Now().Unix()
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
		if err := coordinator.startOplogReplication(oplogStartPosition); err != nil {
			return err
		}
	case "document":
		if err := coordinator.startDocumentReplication(); err != nil {
			return err
		}
	case "oplog":
		if err := coordinator.startOplogReplication(conf.Options.ContextStartPosition); err != nil {
			return err
		}
	default:
		LOG.Critical("unknown sync mode %v", conf.Options.SyncMode)
		return errors.New("unknown sync mode " + conf.Options.SyncMode)
	}

	return nil
}

func (coordinator *ReplicationCoordinator) sanitizeMongoDB() error {
	var conn *dbpool.MongoConn
	var err error
	var hasUniqIndex = false
	rs := map[string]int{}
	if len(coordinator.Sources) > 1 {
		csUrl := conf.Options.ContextStorageUrl
		if conn, err = dbpool.NewMongoConn(csUrl, false); conn == nil || !conn.IsGood() || err != nil {
			LOG.Critical("Connect mongo server error. %v, url : %s", err, csUrl)
			return err
		}
		conn.Close()
	}
	for i, src := range coordinator.Sources {
		if conn, err = dbpool.NewMongoConn(src.URL, false); conn == nil || !conn.IsGood() || err != nil {
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

func (coordinator *ReplicationCoordinator) startDocumentReplication() error {
	if conf.Options.Tunnel != "direct" {
		return errors.New("document replication only support direct tunnel type")
	}

	// get all namespace need to sync
	nsSet := make(map[dbpool.NS]bool)
	for _, src := range coordinator.Sources {
		nsList, err := docsyncer.GetAllNamespace(src.URL)
		if err != nil {
			return err
		}
		for _, ns := range nsList {
			nsSet[ns] = true
		}
	}

	shardingSync := len(coordinator.Sources) > 1
	if err := docsyncer.StartDropDestCollection(nsSet, shardingSync); err != nil {
		return err
	}
	if shardingSync {
		if err := docsyncer.StartNamespaceSpecSyncForSharding(conf.Options.ContextStorageUrl); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	var replError error
	var mutex sync.Mutex
	indexMap := make(map[dbpool.NS][]mgo.Index)

	for i, src := range coordinator.Sources {
		dbSyncer := docsyncer.NewDBSyncer(i, src.URL, conf.Options.TunnelAddress[0], shardingSync)
		LOG.Info("document syncer-%d do replication for url=%v", i, src.URL)
		wg.Add(1)
		nimo.GoRoutine(func() {
			if err := dbSyncer.Start(); err != nil {
				LOG.Critical("document replication for url=%v failed. %v", src.URL, err)
				replError = err
			}
			mutex.Lock()
			for ns, indexList := range dbSyncer.GetIndexMap() {
				indexMap[ns] = indexList
			}
			mutex.Unlock()
			wg.Done()
		})
	}
	wg.Wait()

	if err := docsyncer.StartIndexSync(indexMap, shardingSync); err != nil {
		return err
	}

	return replError
}

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition int64) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	for _, src := range coordinator.Sources {
		syncer := NewOplogSyncer(coordinator, src.ReplicaName, oplogStartPosition, src.URL, src.Gid)
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
