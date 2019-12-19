package docsyncer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/collector/transform"
	"mongoshake/common"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	MAX_BUFFER_BYTE_SIZE = 16 * 1024 * 1024
)

func IsShardingToSharding(fromIsSharding bool, toConn *utils.MongoConn) bool {
	toIsSharding := utils.IsSharding(toConn.Session)

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

func StartDropDestCollection(nsSet map[utils.NS]bool, toConn *utils.MongoConn,
	nsTrans *transform.NamespaceTransform) (map[string]bool, error) {
	nsExistedSet := make(map[string]bool)
	for ns := range nsSet {
		toNS := utils.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.ReplayerCollectionDrop {
			colNames, err := toConn.Session.DB(toNS.Database).CollectionNames()
			if err != nil {
				return nil, LOG.Critical("Get collection names of db %v of dest mongodb failed. %v", toNS.Database, err)
			}
			for _, colName := range colNames {
				if colName == ns.Collection {
					LOG.Warn("ns %v to be synced already exists in dest mongodb, collection and index info will not be synced", toNS)
					nsExistedSet[ns.Str()] = true
					break
				}
			}
		} else {
			err := toConn.Session.DB(toNS.Database).C(toNS.Collection).DropCollection()
			if err != nil && err.Error() != "ns not found" {
				return nil, LOG.Critical("Drop collection ns %v of dest mongodb failed. %v", toNS, err)
			}
		}
	}
	return nsExistedSet, nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *utils.MongoConn,
	nsExistedSet map[string]bool, nsTrans *transform.NamespaceTransform) error {
	LOG.Info("document syncer namespace spec for sharding begin")

	var fromConn *utils.MongoConn
	var err error
	if fromConn, err = utils.NewMongoConn(csUrl, utils.ConnectModePrimary, true); err != nil {
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
			if filterList.IterateFilter(dbSpecDoc.Db + ".$cmd") {
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
					return LOG.Critical("Enable sharding for db %v of dest mongodb failed. %v", todb, err)
				}
				LOG.Info("Enable sharding for db %v of dest mongodb successful", todb)
			}
		}
	}
	if err := dbSpecIter.Close(); err != nil {
		return LOG.Critical("Close iterator of config.database failed. %v", err)
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
		if _, ok := nsExistedSet[colSpecDoc.Ns]; ok {
			LOG.Debug("Namespace spec sync is skipped. %v", colSpecDoc.Ns)
			continue
		}
		if !colSpecDoc.Dropped {
			if filterList.IterateFilter(colSpecDoc.Ns) {
				LOG.Debug("Namespace spec sync is filtered. %v", colSpecDoc.Ns)
				continue
			}
			toNs := nsTrans.Transform(colSpecDoc.Ns)
			err = toConn.Session.DB("admin").Run(bson.D{{"shardCollection", toNs},
				{"key", colSpecDoc.Key}, {"unique", colSpecDoc.Unique}}, nil)
			if err != nil {
				return LOG.Critical("Shard collection for ns %v of dest mongodb failed. %v", toNs, err)
			}
			LOG.Info("Shard collection for ns %v of dest mongodb successful", toNs)
		}
	}
	if err = colSpecIter.Close(); err != nil {
		return LOG.Critical("Close iterator of config.collections failed. %v", err)
	}

	LOG.Info("document syncer namespace spec for sharding successful")
	return nil
}

func StartIndexSync(indexMap map[utils.NS][]mgo.Index, toUrl string,
	nsExistedSet map[string]bool, nsTrans *transform.NamespaceTransform) (syncError error) {
	type IndexNS struct {
		ns        utils.NS
		indexList []mgo.Index
	}

	LOG.Info("document syncer sync index begin")
	if len(indexMap) == 0 {
		LOG.Info("document syncer sync index finish, but no data")
		return nil
	}

	var indexNeedSync int
	for ns := range indexMap {
		if _, ok := nsExistedSet[ns.Str()]; ok {
			LOG.Info("document syncer index sync of ns[%v] is skipped", ns.Str())
			continue
		}
		indexNeedSync++
	}

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan *IndexNS, collExecutorParallel)
	nimo.GoRoutine(func() {
		for ns, indexList := range indexMap {
			if _, ok := nsExistedSet[ns.Str()]; ok {
				continue
			}
			namespaces <- &IndexNS{ns: ns, indexList: indexList}
		}
	})

	var conn *utils.MongoConn
	var err error
	if conn, err = utils.NewMongoConn(toUrl, utils.ConnectModePrimary, false); err != nil {
		return err
	}
	defer conn.Close()

	if indexNeedSync > 0 {
		var wg sync.WaitGroup
		wg.Add(indexNeedSync)
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
					toNS := utils.NewNS(nsTrans.Transform(ns.Str()))

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
	}

	close(namespaces)
	LOG.Info("document syncer sync index finish")
	return syncError
}

type DBSyncer struct {
	replset string
	// source mongodb url
	FromMongoUrl string
	// destination mongodb url
	ToMongoUrl string
	// index of namespace
	indexMap map[utils.NS][]mgo.Index
	// start time of sync
	startTime time.Time
	// namespace transform
	nsTrans *transform.NamespaceTransform
	// filter orphan duplicate record
	orphanFilter *filter.OrphanFilter

	mutex sync.Mutex

	replMetric *utils.ReplicationMetric
}

func NewDBSyncer(
	replset string,
	fromMongoUrl string,
	toMongoUrl string,
	nsTrans *transform.NamespaceTransform,
	orphanFilter *filter.OrphanFilter) *DBSyncer {

	syncer := &DBSyncer{
		replset:      replset,
		FromMongoUrl: fromMongoUrl,
		ToMongoUrl:   toMongoUrl,
		indexMap:     make(map[utils.NS][]mgo.Index),
		nsTrans:      nsTrans,
		orphanFilter: orphanFilter,
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
		LOG.Info("document syncer %v finish, but no data", syncer.replset)
	}

	collExecutorParallel := conf.Options.ReplayerCollectionParallel
	namespaces := make(chan utils.NS, collExecutorParallel)

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

				toNS := utils.NewNS(syncer.nsTrans.Transform(ns.Str()))

				LOG.Info("document syncer %v collExecutor-%d sync ns %v to %v begin",
					syncer.replset, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					syncError = LOG.Critical("document syncer %v collExecutor-%d sync ns %v to %v failed. %v",
						syncer.replset, collExecutorId, ns, toNS, err)
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					LOG.Info("document syncer %v collExecutor-%d sync ns %v to %v successful. db syncer %v progress %v%%",
						syncer.replset, collExecutorId, ns, toNS, syncer.replset, process)
				}
				wg.Done()
			}
			LOG.Info("document syncer %v collExecutor-%d finish", syncer.replset, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)
	return syncError
}

func (syncer *DBSyncer) collectionSync(collExecutorId int, ns utils.NS,
	toNS utils.NS) error {
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
		if bufferByteSize+len(doc.Data) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// filter orphan document of chunk
		if conf.Options.FilterOrphanDocument && syncer.orphanFilter.Filter(doc, ns.Str()) {
			continue
		}

		// transform dbref
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.DBRef {
			doc = transform.TransformDBRef(doc, ns.Database, syncer.nsTrans)
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

func (syncer *DBSyncer) GetIndexMap() map[utils.NS][]mgo.Index {
	return syncer.indexMap
}
