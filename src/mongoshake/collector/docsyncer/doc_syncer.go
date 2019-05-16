package docsyncer

import (
	"errors"
	"fmt"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/dbpool"
	"sync"
	"time"

	LOG "github.com/vinllen/log4go"
)

func IsShardingToSharding(fromIsSharding bool, toConn *dbpool.MongoConn) (bool, error) {
	var toIsSharding bool
	var result interface{}
	err := toConn.Session.DB("config").C("version").Find(bson.M{}).One(&result)
	if err != nil {
		toIsSharding = false
	} else {
		toIsSharding = true
	}

	if fromIsSharding && toIsSharding {
		LOG.Warn("replication from sharding to sharding")
		return true, nil
	} else if fromIsSharding && !toIsSharding {
		LOG.Warn("replication from sharding to replica")
		return false, nil
	} else if !fromIsSharding && toIsSharding {
		LOG.Warn("replication from replica to sharding")
		return false, nil
	} else {
		LOG.Warn("replication from replica to replica")
		return false, nil
	}
}

func StartDropDestCollection(nsSet map[dbpool.NS]bool, toConn *dbpool.MongoConn, shardingSync bool) error {
	for ns := range nsSet {
		toNS := getToNs(ns, shardingSync)
		err := toConn.Session.DB(toNS.Database).C(toNS.Collection).DropCollection()
		if err != nil && err.Error() != "ns not found"{
			LOG.Critical("Drop Collection ns %v of dest mongodb failed. %v", toNS, err)
			return errors.New(fmt.Sprintf("Drop Collection ns %v of dest mongodb failed. %v", toNS, err))
		}
	}

	return nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *dbpool.MongoConn) error {
	LOG.Info("document syncer namespace spec for sharding begin")

	var fromConn *dbpool.MongoConn
	var err error
	if fromConn, err = dbpool.NewMongoConn(csUrl, true); err != nil {
		return err
	}
	defer fromConn.Close()

	type dbConfig struct {
		Db          string `bson:"_id"`
		Partitioned bool   `bson:"partitioned"`
	}
	var dbDoc dbConfig

	dbIter := fromConn.Session.DB("config").C("databases").Find(bson.M{}).Iter()
	for dbIter.Next(&dbDoc) {
		if dbDoc.Partitioned {
			var todbDoc dbConfig
			err = toConn.Session.DB("config").C("databases").
				Find(bson.D{{"_id", dbDoc.Db}}).One(&todbDoc)
			if err == nil && todbDoc.Partitioned {
				continue
			}
			err = toConn.Session.DB("admin").Run(bson.D{{"enablesharding", dbDoc.Db}}, nil)
			if err != nil {
				LOG.Critical("Enable sharding for db %v of dest mongodb failed. %v", dbDoc.Db, err)
				return errors.New(fmt.Sprintf("Enable sharding for db %v of dest mongodb failed. %v",
					dbDoc.Db, err))
			}
		}
	}

	if err := dbIter.Close(); err != nil {
		LOG.Critical("Close iterator of config.database failed. %v", err)
	}

	type colConfig struct {
		Ns      string    `bson:"_id"`
		Key     *bson.Raw `bson:"key"`
		Unique  bool      `bson:"unique"`
		Dropped bool      `bson:"dropped"`
	}
	var colDoc colConfig
	colIter := fromConn.Session.DB("config").C("collections").Find(bson.M{}).Iter()
	for colIter.Next(&colDoc) {
		if !colDoc.Dropped {
			err = toConn.Session.DB("admin").Run(bson.D{{"shardCollection", colDoc.Ns},
				{"key", bson.M{"a":1}}, {"unique", colDoc.Unique}}, nil)
			if err != nil {
				LOG.Critical("Shard collection for ns %v of dest mongodb failed. %v", colDoc.Ns, err)
				return errors.New(fmt.Sprintf("Shard collection for ns %v of dest mongodb failed. %v",
					colDoc.Ns, err))
			}
		}
	}

	if err = colIter.Close(); err != nil {
		LOG.Critical("Close iterator of config.collections failed. %v", err)
	}

	LOG.Info("document syncer namespace spec for sharding successful")
	return nil
}

func StartIndexSync(indexMap map[dbpool.NS][]mgo.Index, toUrl string, shardingSync bool) (syncError error) {
	type IndexNS struct {
		ns        dbpool.NS
		indexList []mgo.Index
	}

	LOG.Info("document syncer sync index begin")
	var wg sync.WaitGroup

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan *IndexNS, collExecutorParallel)

	wg.Add(len(indexMap))

	nimo.GoRoutine(func() {
		for ns, indexList := range indexMap {
			namespaces <- &IndexNS{ns: ns, indexList: indexList}
		}
	})

	var conn *dbpool.MongoConn
	var err error
	if conn, err = dbpool.NewMongoConn(toUrl, true); err != nil {
		return err
	}
	defer conn.Close()

	for i := 0; i < collExecutorParallel; i++ {
		nimo.GoRoutine(func() {
			session := conn.Session.Clone()
			defer session.Close()

			for {
				indexNs, ok := <-namespaces
				if !ok {
					break
				}
				ns := indexNs.ns
				toNS := getToNs(ns, shardingSync)

				if !shardingSync && conf.Options.ReplayerCollectionRename {
					// rename collection before create index, because the collection name of dest mongodb maybe too long
					// namespace.indexName cannot exceed 127 byte
					err := session.DB("admin").
						Run(bson.D{{"renameCollection", toNS.Str()}, {"to", ns.Str()}}, nil)
					if err != nil && err.Error() != "source namespace does not exist" {
						LOG.Critical("Rename Collection ns %v of dest mongodb to ns %v failed. %v", toNS, ns, err)
						syncError = errors.New(fmt.Sprintf("Rename Collection ns %v of dest mongodb to ns %v failed. %v",
							toNS, ns, err))
					} else {
						LOG.Info("Rename collection ns %v of dest mongodb to ns %v successful", toNS, ns)
					}
					toNS = ns
				}

				for _, index := range indexNs.indexList {
					index.Background = false
					if err = session.DB(toNS.Database).C(toNS.Collection).EnsureIndex(index); err != nil {
						LOG.Warn("Create indexes for ns %v of dest mongodb failed. %v", ns, err)
					}
				}
				LOG.Info("Create indexes for ns %v of dest mongodb finish", toNS)

				wg.Done()
			}
		})
	}

	wg.Wait()
	close(namespaces)
	LOG.Info("document syncer sync index finish")
	return syncError
}

func Checkpoint(ckptMap map[string]bson.MongoTimestamp) error {
	for name, ts := range ckptMap {
		ckptManager := ckpt.NewCheckpointManager(name, 0)
		ckptManager.Get()
		if err := ckptManager.Update(ts); err != nil {
			return err
		}
	}
	return nil
}

type DBSyncer struct {
	// syncer id
	id int
	// source mongodb url
	FromMongoUrl string
	// destination mongodb url
	ToMongoUrl string
	// index of namespace
	indexMap map[dbpool.NS][]mgo.Index
	// start time of sync
	startTime time.Time
	// is sharding sync
	shardingSync bool

	mutex sync.Mutex

	replMetric *utils.ReplicationMetric
}

func NewDBSyncer(
	id int,
	fromMongoUrl string,
	toMongoUrl string,
	shardingSync bool) *DBSyncer {

	syncer := &DBSyncer{
		id:           id,
		FromMongoUrl: fromMongoUrl,
		ToMongoUrl:   toMongoUrl,
		indexMap:     make(map[dbpool.NS][]mgo.Index),
		shardingSync: shardingSync,
	}

	return syncer
}

func (syncer *DBSyncer) Start() (syncError error) {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	nsList, err := getDbNamespace(syncer.FromMongoUrl)
	if err != nil {
		return err
	}

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan dbpool.NS, collExecutorParallel)

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			namespaces <- ns
		}
	})

	for i := 0; i < collExecutorParallel; i++ {
		collExecutorId := GenerateCollExecutorId()
		nimo.GoRoutine(func() {
			for {
				ns, ok := <-namespaces
				if !ok {
					break
				}

				LOG.Info("document syncer-%d collExecutor-%d sync ns %v begin", syncer.id, collExecutorId, ns)
				if err := syncer.collectionSync(collExecutorId, ns); err != nil {
					LOG.Critical("document syncer-%d collExecutor-%d sync ns %v failed. %v", syncer.id, collExecutorId, ns, err)
					syncError = errors.New(fmt.Sprintf("document syncer sync ns %v failed. %v", ns, err))
				} else {
					LOG.Info("document syncer-%d collExecutor-%d sync ns %v successful", syncer.id, collExecutorId, ns)
				}

				wg.Done()
			}
			LOG.Info("document syncer-%d collExecutor-%d finish", syncer.id, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)
	return nil
}


func (syncer *DBSyncer) collectionSync(collExecutorId int, ns dbpool.NS) error {
	reader := NewDocumentReader(syncer.FromMongoUrl, ns)

	toNS := getToNs(ns, syncer.shardingSync)
	colExecutor := NewCollectionExecutor(collExecutorId, syncer.ToMongoUrl, toNS)
	if err := colExecutor.Start(); err != nil {
		return err
	}

	bufferSize := conf.Options.ReplayerDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)

	for {
		var doc *bson.Raw
		var err error
		if doc, err = reader.NextDoc(); err != nil {
			return errors.New(fmt.Sprintf("Get next document from ns %v of src mongodb failed. %v", ns, err))
		} else if doc == nil {
			colExecutor.Sync(buffer)
			if err := colExecutor.Wait(); err != nil {
				return err
			}
			break
		}
		buffer = append(buffer, doc)
		if len(buffer) >= bufferSize {
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
		}
	}

	if indexes, err := reader.GetIndexes(); err != nil {
		return errors.New(fmt.Sprintf("Get indexes from ns %v of src mongodb failed. %v", ns, err))
	} else {
		syncer.mutex.Lock()
		defer syncer.mutex.Unlock()
		syncer.indexMap[ns] = indexes
	}

	reader.Close()
	return nil
}

func (syncer *DBSyncer) GetIndexMap() map[dbpool.NS][]mgo.Index {
	return syncer.indexMap
}

func getToNs(ns dbpool.NS, shardingSync bool) dbpool.NS {
	if shardingSync {
		return ns
	} else {
		toCollection := fmt.Sprintf("%s_%s", ns.Collection, utils.APPNAME)
		return dbpool.NS{Database:ns.Database, Collection:toCollection}
	}
}
