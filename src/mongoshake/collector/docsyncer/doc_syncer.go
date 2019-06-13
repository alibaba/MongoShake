package docsyncer

import (
	"errors"
	"fmt"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/collector/transform"
	"mongoshake/common"
	"mongoshake/dbpool"
	"sync"
	"sync/atomic"
	"time"

	LOG "github.com/vinllen/log4go"
)

const (
	MAX_BUFFER_BYTE_SIZE = 16*1024*1024
)

func IsShardingToSharding(fromIsSharding bool, toConn *dbpool.MongoConn) bool {
	var toIsSharding bool
	var result interface{}
	err := toConn.Session.DB("config").C("version").Find(bson.M{}).One(&result)
	if err != nil {
		toIsSharding = false
	} else {
		toIsSharding = true
	}

	if fromIsSharding && toIsSharding {
		LOG.Info("replication from sharding to sharding")
		return true
	} else if fromIsSharding && !toIsSharding {
		LOG.Info("replication from sharding to replica")
		return false
	} else if !fromIsSharding && toIsSharding {
		LOG.Info("replication from replica to sharding")
		return false
	} else {
		LOG.Info("replication from replica to replica")
		return false
	}
}

func StartDropDestCollection(nsSet map[dbpool.NS]bool, toConn *dbpool.MongoConn,
	nsTrans *transform.NamespaceTransform) error {
	for ns := range nsSet {
		toNS := dbpool.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.ReplayerCollectionDrop {
			colNames, err := toConn.Session.DB(toNS.Database).CollectionNames()
			if err != nil {
				LOG.Critical("Get collection names of db %v of dest mongodb failed. %v", toNS.Database, err)
				return err
			}
			for _, colName := range colNames {
				if colName == ns.Collection {
					LOG.Critical("ns %v to be synced already exists in dest mongodb", toNS)
					return errors.New(fmt.Sprintf("ns %v to be synced already exists in dest mongodb", toNS))
				}
			}
		}
		err := toConn.Session.DB(toNS.Database).C(toNS.Collection).DropCollection()
		if err != nil && err.Error() != "ns not found" {
			LOG.Critical("Drop collection ns %v of dest mongodb failed. %v", toNS, err)
			return errors.New(fmt.Sprintf("Drop collection ns %v of dest mongodb failed. %v", toNS, err))
		}
	}
	return nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *dbpool.MongoConn,
	nsTrans *transform.NamespaceTransform) error {
	LOG.Info("document syncer namespace spec for sharding begin")

	var fromConn *dbpool.MongoConn
	var err error
	if fromConn, err = dbpool.NewMongoConn(csUrl, true, true); err != nil {
		return err
	}
	defer fromConn.Close()

	filterList := filter.NewDocFilterList()
	dbTrans := transform.NewDBTransform(conf.Options.TransformNamespace)

	type dbSpec struct {
		Db          string `bson:"_id"`
		Partitioned bool   `bson:"partitioned"`
	}
	var dbSpecDoc dbSpec
	// enable sharding for db
	dbSpecIter := fromConn.Session.DB("config").C("databases").Find(bson.M{}).Iter()
	for dbSpecIter.Next(&dbSpecDoc) {
		if dbSpecDoc.Partitioned {
			if filterList.IterateFilter(dbSpecDoc.Db) {
				LOG.Debug("DB is filtered. %v", dbSpecDoc.Db)
				continue
			}
			var todbSpecDoc dbSpec
			todbList := dbTrans.Transform(dbSpecDoc.Db)
			for _, todb := range todbList {
				err = toConn.Session.DB("config").C("databases").
					Find(bson.D{{"_id", todb}}).One(&todbSpecDoc)
				if err == nil && todbSpecDoc.Partitioned {
					continue
				}
				err = toConn.Session.DB("admin").Run(bson.D{{"enablesharding", todb}}, nil)
				if err != nil {
					LOG.Critical("Enable sharding for db %v of dest mongodb failed. %v", todb, err)
					return errors.New(fmt.Sprintf("Enable sharding for db %v of dest mongodb failed. %v",
						todb, err))
				}
				LOG.Info("Enable sharding for db %v of dest mongodb successful", todb)
			}
		}
	}
	if err := dbSpecIter.Close(); err != nil {
		LOG.Critical("Close iterator of config.database failed. %v", err)
	}

	type colSpec struct {
		Ns      string    `bson:"_id"`
		Key     *bson.Raw `bson:"key"`
		Unique  bool      `bson:"unique"`
		Dropped bool      `bson:"dropped"`
	}
	var colSpecDoc colSpec
	// enable sharding for db
	colSpecIter := fromConn.Session.DB("config").C("collections").Find(bson.M{}).Iter()
	for colSpecIter.Next(&colSpecDoc) {
		if !colSpecDoc.Dropped {
			if filterList.IterateFilter(colSpecDoc.Ns) {
				LOG.Debug("Namespace is filtered. %v", colSpecDoc.Ns)
				continue
			}
			toNs := nsTrans.Transform(colSpecDoc.Ns)
			err = toConn.Session.DB("admin").Run(bson.D{{"shardCollection", toNs},
				{"key", colSpecDoc.Key}, {"unique", colSpecDoc.Unique}}, nil)
			if err != nil {
				LOG.Critical("Shard collection for ns %v of dest mongodb failed. %v", toNs, err)
				return errors.New(fmt.Sprintf("Shard collection for ns %v of dest mongodb failed. %v",
					toNs, err))
			}
			LOG.Info("Shard collection for ns %v of dest mongodb successful", toNs)
		}
	}
	if err = colSpecIter.Close(); err != nil {
		LOG.Critical("Close iterator of config.collections failed. %v", err)
	}

	LOG.Info("document syncer namespace spec for sharding successful")
	return nil
}

func StartIndexSync(indexMap map[dbpool.NS][]mgo.Index, toUrl string,
	nsTrans *transform.NamespaceTransform) (syncError error) {
	type IndexNS struct {
		ns        dbpool.NS
		indexList []mgo.Index
	}

	LOG.Info("document syncer sync index begin")
	if len(indexMap) == 0 {
		LOG.Info("document syncer sync index finish, but no data")
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(indexMap))

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan *IndexNS, collExecutorParallel)
	nimo.GoRoutine(func() {
		for ns, indexList := range indexMap {
			namespaces <- &IndexNS{ns: ns, indexList: indexList}
		}
	})

	var conn *dbpool.MongoConn
	var err error
	if conn, err = dbpool.NewMongoConn(toUrl, true, false); err != nil {
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
				toNS := dbpool.NewNS(nsTrans.Transform(ns.Str()))

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

	nsTrans *transform.NamespaceTransform

	mutex sync.Mutex

	replMetric *utils.ReplicationMetric
}

func NewDBSyncer(
	id int,
	fromMongoUrl string,
	toMongoUrl string,
	nsTrans *transform.NamespaceTransform) *DBSyncer {

	syncer := &DBSyncer{
		id:           id,
		FromMongoUrl: fromMongoUrl,
		ToMongoUrl:   toMongoUrl,
		indexMap:     make(map[dbpool.NS][]mgo.Index),
		nsTrans:      nsTrans,
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

	if len(nsList) == 0 {
		LOG.Info("document syncer-%d finish, but no data", syncer.id)
	}

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan dbpool.NS, collExecutorParallel)

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			namespaces <- ns
		}
	})

	var nsDoneCount int32 = 0
	for i := 0; i < collExecutorParallel; i++ {
		collExecutorId := GenerateCollExecutorId()
		nimo.GoRoutine(func() {
			for {
				ns, ok := <-namespaces
				if !ok {
					break
				}

				toNS := dbpool.NewNS(syncer.nsTrans.Transform(ns.Str()))

				LOG.Info("document syncer-%d collExecutor-%d sync ns %v to %v begin",
					syncer.id, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					LOG.Critical("document syncer-%d collExecutor-%d sync ns %v to %v failed. %v",
						syncer.id, collExecutorId, ns, toNS, err)
					syncError = errors.New(fmt.Sprintf("document syncer sync ns %v to %vfailed. %v",
						ns, toNS, err))
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					LOG.Info("document syncer-%d collExecutor-%d sync ns %v to %v successful. db syncer-%d progress %v%%",
						syncer.id, collExecutorId, ns, toNS, syncer.id, process)
				}
				wg.Done()
			}
			LOG.Info("document syncer-%d collExecutor-%d finish", syncer.id, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)
	return syncError
}

func (syncer *DBSyncer) collectionSync(collExecutorId int, ns dbpool.NS,
	toNS dbpool.NS) error {
	reader := NewDocumentReader(syncer.FromMongoUrl, ns)

	colExecutor := NewCollectionExecutor(collExecutorId, syncer.ToMongoUrl, toNS)
	if err := colExecutor.Start(); err != nil {
		return err
	}

	bufferSize := conf.Options.ReplayerDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)
	bufferByteSize := 0

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
		if bufferByteSize + len(doc.Data) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}
		// transform dbref for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.TransformDBRef {
			docData := bson.M{}
			if err := bson.Unmarshal(doc.Data, docData); err != nil {
				LOG.Warn("collectionSync do bson unmarshal %v failed. %v", doc.Data, err)
			} else {
				if ns.Collection == "tt" {
					LOG.Info("test docData %v", docData)
				}
				docData = transform.TransformDBRef(docData, ns.Database, syncer.nsTrans)
				if ns.Collection == "tt" {
					LOG.Info("test docData2 %v", docData)
				}
				if v, err := bson.Marshal(docData); err != nil {
					LOG.Warn("collectionSync do bson marshal %v failed. %v", docData, err)
				} else {
					doc.Data = v
				}
			}
		}
		buffer = append(buffer, doc)
		bufferByteSize += len(doc.Data)
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
