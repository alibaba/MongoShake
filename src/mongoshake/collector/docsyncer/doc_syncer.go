package docsyncer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mongoshake/collector/ckpt"
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
	SpliterReader        = 4
)

func IsShardingToSharding(fromIsSharding bool, toConn *utils.MongoConn) bool {
	var source, target string
	if fromIsSharding {
		source = "sharding"
	} else {
		source = "replica"
	}

	var result interface{}
	err := toConn.Session.DB("config").C("version").Find(bson.M{}).One(&result)
	if err != nil {
		target = "replica"
	} else {
		target = "sharding"
	}

	LOG.Info("replication from [%s] to [%s]", source, target)
	if source == "sharding" && target == "sharding" {
		return true
	}
	return false
}

func StartDropDestCollection(nsSet map[utils.NS]struct{}, toConn *utils.MongoConn,
	nsTrans *transform.NamespaceTransform) error {
	for ns := range nsSet {
		toNS := utils.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.FullSyncCollectionDrop {
			// do not drop
			colNames, err := toConn.Session.DB(toNS.Database).CollectionNames()
			if err != nil {
				LOG.Critical("Get collection names of db %v of dest mongodb failed. %v", toNS.Database, err)
				return err
			}

			// judge whether toNs exists
			for _, colName := range colNames {
				if colName == toNS.Collection {
					LOG.Warn("ns %v to be synced already exists in dest mongodb", toNS)
					break
				}
			}
		} else {
			// need drop
			err := toConn.Session.DB(toNS.Database).C(toNS.Collection).DropCollection()
			if err != nil && err.Error() != "ns not found" {
				LOG.Critical("Drop collection ns %v of dest mongodb failed. %v", toNS, err)
				return errors.New(fmt.Sprintf("Drop collection ns %v of dest mongodb failed. %v", toNS, err))
			}
		}
	}
	return nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *utils.MongoConn,
	nsTrans *transform.NamespaceTransform) error {
	LOG.Info("document syncer namespace spec for sharding begin")

	var fromConn *utils.MongoConn
	var err error
	if fromConn, err = utils.NewMongoConn(csUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault); err != nil {
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

func StartIndexSync(indexMap map[utils.NS][]mgo.Index, toUrl string,
		nsTrans *transform.NamespaceTransform, background bool) (syncError error) {
	type IndexNS struct {
		ns        utils.NS
		indexList []mgo.Index
	}

	LOG.Info("start writing index with background[%v], indexMap length[%v]", background, len(indexMap))
	if len(indexMap) == 0 {
		LOG.Info("finish writing index, but no data")
		return nil
	}

	collExecutorParallel := conf.Options.FullSyncReaderCollectionParallel
	namespaces := make(chan *IndexNS, collExecutorParallel)
	nimo.GoRoutine(func() {
		for ns, indexList := range indexMap {
			namespaces <- &IndexNS{ns: ns, indexList: indexList}
		}
		close(namespaces)
	})

	var conn *utils.MongoConn
	var err error
	if conn, err = utils.NewMongoConn(toUrl, utils.VarMongoConnectModePrimary, false,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernMajority); err != nil {
		return err
	}
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(collExecutorParallel)
	for i := 0; i < collExecutorParallel; i++ {
		nimo.GoRoutine(func() {
			session := conn.Session.Clone()
			defer session.Close()
			defer wg.Done()

			for {
				indexNs, ok := <-namespaces
				if !ok {
					break
				}
				ns := indexNs.ns
				toNS := utils.NewNS(nsTrans.Transform(ns.Str()))

				for _, index := range indexNs.indexList {
					// ignore _id
					if len(index.Key) == 1 && index.Key[0] == "_id" {
						continue
					}

					index.Background = background
					if err = session.DB(toNS.Database).C(toNS.Collection).EnsureIndex(index); err != nil {
						LOG.Warn("Create indexes for ns %v of dest mongodb failed. %v", ns, err)
					}
				}
				LOG.Info("Create indexes for ns %v of dest mongodb finish", toNS)
			}
		})
	}

	wg.Wait()
	LOG.Info("finish writing index")
	return syncError
}

func Checkpoint(ckptMap map[string]utils.TimestampNode) error {
	for name, ts := range ckptMap {
		ckptManager := ckpt.NewCheckpointManager(name, 0)
		ckptManager.Get() // load checkpoint in ckptManager
		if err := ckptManager.Update(ts.Newest); err != nil {
			return err
		}
	}
	return nil
}

/************************************************************************/
// 1 shard -> 1 DBSyncer
type DBSyncer struct {
	// syncer id
	id int
	// source mongodb url
	FromMongoUrl string
	fromReplset string
	// destination mongodb url
	ToMongoUrl string
	// index of namespace
	indexMap map[utils.NS][]mgo.Index
	// start time of sync
	startTime time.Time
	// source is sharding?
	FromIsSharding bool

	nsTrans *transform.NamespaceTransform
	// filter orphan duplicate record
	orphanFilter *filter.OrphanFilter

	mutex sync.Mutex

	qos *utils.Qos // not owned

	replMetric *utils.ReplicationMetric

	// below are metric info
	metricNsMapLock sync.Mutex
	metricNsMap     map[utils.NS]*CollectionMetric // namespace map: db.collection -> collection metric
}

func NewDBSyncer(
	id int,
	fromMongoUrl string,
	fromReplset string,
	toMongoUrl string,
	nsTrans *transform.NamespaceTransform,
	orphanFilter *filter.OrphanFilter,
	qos *utils.Qos,
	fromIsSharding bool) *DBSyncer {

	syncer := &DBSyncer{
		id:             id,
		FromMongoUrl:   fromMongoUrl,
		fromReplset:    fromReplset,
		ToMongoUrl:     toMongoUrl,
		nsTrans:        nsTrans,
		orphanFilter:   orphanFilter,
		qos:            qos,
		metricNsMap:    make(map[utils.NS]*CollectionMetric),
		replMetric:     utils.NewMetric(fromReplset, utils.TypeFull, utils.METRIC_TPS),
		FromIsSharding: fromIsSharding,
	}

	return syncer
}

func (syncer *DBSyncer) String() string {
	return fmt.Sprintf("DBSyncer id[%v] source[%v] target[%v] startTime[%v]",
		syncer.id, utils.BlockMongoUrlPassword(syncer.FromMongoUrl, "***"),
		utils.BlockMongoUrlPassword(syncer.ToMongoUrl, "***"), syncer.startTime)
}

func (syncer *DBSyncer) Init() {
	syncer.RestAPI()
}

func (syncer *DBSyncer) Close() {
	LOG.Info("syncer[%v] closed", syncer)
	syncer.replMetric.Close()
}

// @deprecated
func (syncer *DBSyncer) GetIndexMap() map[utils.NS][]mgo.Index {
	return syncer.indexMap
}

func (syncer *DBSyncer) Start() (syncError error) {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	// get all namespace
	nsList, _, err := GetDbNamespace(syncer.FromMongoUrl)
	if err != nil {
		return err
	}

	if len(nsList) == 0 {
		LOG.Info("%s finish, but no data", syncer)
		return
	}

	// create metric for each collection
	for _, ns := range nsList {
		syncer.metricNsMap[ns] = NewCollectionMetric()
	}

	collExecutorParallel := conf.Options.FullSyncReaderCollectionParallel
	namespaces := make(chan utils.NS, collExecutorParallel)

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			namespaces <- ns
		}
	})

	// run collection sync in parallel
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

				LOG.Info("%s collExecutor-%d sync ns %v to %v begin", syncer, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					LOG.Critical("%s collExecutor-%d sync ns %v to %v failed. %v",
						syncer, collExecutorId, ns, toNS, err)
					syncError = fmt.Errorf("document syncer sync ns %v to %v failed. %v", ns, toNS, err)
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					LOG.Info("%s collExecutor-%d sync ns %v to %v successful. db syncer-%d progress %v%%",
						syncer, collExecutorId, ns, toNS, syncer.id, process)
				}
				wg.Done()
			}
			LOG.Info("%s collExecutor-%d finish", syncer, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)

	return syncError
}

// start sync single collection
func (syncer *DBSyncer) collectionSync(collExecutorId int, ns utils.NS, toNS utils.NS) error {
	// writer
	colExecutor := NewCollectionExecutor(collExecutorId, syncer.ToMongoUrl, toNS, syncer)
	if err := colExecutor.Start(); err != nil {
		return err
	}

	// splitter reader
	splitter := NewDocumentSplitter(syncer.FromMongoUrl, ns)
	if splitter == nil {
		return fmt.Errorf("create splitter failed")
	}
	defer splitter.Close()

	// metric
	collectionMetric := syncer.metricNsMap[ns]
	collectionMetric.CollectionStatus = StatusProcessing
	collectionMetric.TotalCount = splitter.count

	// run in several pieces
	var wg sync.WaitGroup
	wg.Add(splitter.pieceNumber)
	for i := 0; i < SpliterReader; i++ {
		go func() {
			for {
				reader, ok := <-splitter.readerChan
				if !ok {
					break
				}

				if err := syncer.splitSync(reader, colExecutor, collectionMetric); err != nil {
					LOG.Crashf("%v", err)
				}

				wg.Done()
			}
		}()
	}
	wg.Wait()

	// close writer
	if err := colExecutor.Wait(); err != nil {
		return fmt.Errorf("close writer failed: %v", err)
	}

	/*
	 * in the former version, we fetch indexes after all data finished. However, it'll
	 * have problem if the index is build/delete/update in the full-sync stage, the oplog
	 * will be replayed again, e.g., build index, which must be wrong.
	 */
	// fetch index

	// set collection finish
	collectionMetric.CollectionStatus = StatusFinish

	return nil
}

func (syncer *DBSyncer) splitSync(reader *DocumentReader, colExecutor *CollectionExecutor, collectionMetric *CollectionMetric) error {
	bufferSize := conf.Options.FullSyncReaderDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)
	bufferByteSize := 0

	for {
		doc, err := reader.NextDoc()
		if err != nil {
			return fmt.Errorf("splitter reader[%v] get next document failed: %v", reader, err)
		} else if doc == nil {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			break
		}

		syncer.replMetric.AddGet(1)
		syncer.replMetric.AddSuccess(1) // only used to calculate the tps which is extract from "success"

		if bufferByteSize+len(doc.Data) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// transform dbref for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.IncrSyncDBRef {
			var docData bson.D
			if err := bson.Unmarshal(doc.Data, docData); err != nil {
				LOG.Error("splitter reader[%v] do bson unmarshal %v failed. %v", reader, doc.Data, err)
			} else {
				docData = transform.TransformDBRef(docData, reader.ns.Database, syncer.nsTrans)
				if v, err := bson.Marshal(docData); err != nil {
					LOG.Warn("splitter reader[%v] do bson marshal %v failed. %v", reader, docData, err)
				} else {
					doc.Data = v
				}
			}
		}
		buffer = append(buffer, doc)
		bufferByteSize += len(doc.Data)
	}

	LOG.Info("splitter reader finishes: %v", reader)
	reader.Close()
	return nil
}

/************************************************************************/
// restful api
func (syncer *DBSyncer) RestAPI() {
	// progress api
	type OverviewInfo struct {
		Progress             string            `json:"progress"`                     // synced_collection_number / total_collection_number
		TotalCollection      int               `json:"total_collection_number"`      // total collection
		FinishedCollection   int               `json:"finished_collection_number"`   // finished
		ProcessingCollection int               `json:"processing_collection_number"` // in processing
		WaitCollection       int               `json:"wait_collection_number"`       // wait start
		CollectionMetric     map[string]string `json:"collection_metric"`            // collection_name -> process
	}

	utils.FullSyncHttpApi.RegisterAPI("/progress", nimo.HttpGet, func([]byte) interface{} {
		ret := OverviewInfo{
			CollectionMetric: make(map[string]string),
		}

		syncer.metricNsMapLock.Lock()
		defer syncer.metricNsMapLock.Unlock()

		ret.TotalCollection = len(syncer.metricNsMap)
		for ns, collectionMetric := range syncer.metricNsMap {
			ret.CollectionMetric[ns.Str()] = collectionMetric.String()
			switch collectionMetric.CollectionStatus {
			case StatusWaitStart:
				ret.WaitCollection += 1
			case StatusProcessing:
				ret.ProcessingCollection += 1
			case StatusFinish:
				ret.FinishedCollection += 1
			}
		}

		if ret.TotalCollection == 0 {
			ret.Progress = "100%"
		} else {
			ret.Progress = fmt.Sprintf("%.2f%%", float64(ret.FinishedCollection) / float64(ret.TotalCollection) * 100)
		}

		return ret
	})

	/***************************************************/


}
