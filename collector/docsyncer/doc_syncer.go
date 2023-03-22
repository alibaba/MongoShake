package docsyncer

import (
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	"github.com/alibaba/MongoShake/v2/collector/transform"
	utils "github.com/alibaba/MongoShake/v2/common"

	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	MAX_BUFFER_BYTE_SIZE = 12 * 1024 * 1024
)

func IsShardingToSharding(fromIsSharding bool, toConn *utils.MongoCommunityConn) bool {
	if conf.Options.FullSyncExecutorDebug {
		LOG.Info("full_sync.executor.debug set, no need to check IsShardingToSharding")
		return false
	}

	var source, target string
	if fromIsSharding {
		source = "sharding"
	} else {
		source = "replica"
	}

	err := toConn.Client.Database("config").Collection("version").FindOne(nil, bson.M{}).Err()
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

func in(target string, str_array []string) bool {
	sort.Strings(str_array)
	index := sort.SearchStrings(str_array, target)
	if index < len(str_array) && str_array[index] == target {
		return true
	}
	return false
}

func StartDropDestCollection(nsSet map[utils.NS]struct{}, toConn *utils.MongoCommunityConn,
	nsTrans *transform.NamespaceTransform) error {
	if conf.Options.FullSyncExecutorDebug {
		LOG.Info("full_sync.executor.debug set, no need to drop collection")
		return nil
	}

	for ns := range nsSet {
		toNS := utils.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.FullSyncCollectionDrop {
			// do not drop
			colNames, err := toConn.Client.Database(toNS.Database).ListCollectionNames(nil,
				utils.GetListCollectionQueryCondition(toConn))
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
			err := toConn.Client.Database(toNS.Database).Collection(toNS.Collection).Drop(nil)
			if err != nil && err.Error() != "ns not found" {
				LOG.Critical("Drop collection ns %v of dest mongodb failed. %v", toNS, err)
				return errors.New(fmt.Sprintf("Drop collection ns %v of dest mongodb failed. %v", toNS, err))
			}
		}
	}
	return nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *utils.MongoCommunityConn,
	nsTrans *transform.NamespaceTransform) error {
	LOG.Info("document syncer namespace spec for sharding begin")

	var fromConn *utils.MongoCommunityConn
	var err error
	if fromConn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault,
		conf.Options.MongoSslRootCaFile); err != nil {
		LOG.Info("Connect to [%s] failed. err[%v]", csUrl, err)
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
	var docCursor *mongo.Cursor
	// enable sharding for db
	docCursor, err = fromConn.Client.Database("config").Collection("databases").Find(nil, bson.M{})
	if err != nil {
		return err
	}
	for docCursor.Next(nil) {
		err = bson.Unmarshal(docCursor.Current, &dbSpecDoc)
		if err != nil {
			LOG.Error("parse docCursor.Current[%v] failed", docCursor.Current)
			continue
		}
		if dbSpecDoc.Partitioned {
			if filterList.IterateFilter(dbSpecDoc.Db + ".$cmd") {
				LOG.Debug("DB is filtered. %v", dbSpecDoc.Db)
				continue
			}
			var todbSpecDoc dbSpec
			todbList := dbTrans.Transform(dbSpecDoc.Db)
			for _, todb := range todbList {

				err = toConn.Client.Database("config").Collection("databases").FindOne(nil,
					bson.D{{"_id", todb}}).Decode(&todbSpecDoc)
				if err == nil && todbSpecDoc.Partitioned {
					continue
				}
				err = toConn.Client.Database("admin").RunCommand(nil,
					bson.D{{"enablesharding", todb}}).Err()
				if err != nil {
					LOG.Critical("Enable sharding for db %v of dest mongodb failed. %v", todb, err)
					return errors.New(fmt.Sprintf("Enable sharding for db %v of dest mongodb failed. %v",
						todb, err))
				}
				LOG.Info("Enable sharding for db %v of dest mongodb successful", todb)
			}
		}
	}
	if err := docCursor.Close(nil); err != nil {
		LOG.Critical("Close iterator of config.database failed. %v", err)
	}

	type colSpec struct {
		Ns      string    `bson:"_id"`
		Key     *bson.Raw `bson:"key"`
		Unique  bool      `bson:"unique"`
		Dropped bool      `bson:"dropped"`
	}
	var colSpecDoc colSpec
	var colDocCursor *mongo.Cursor
	// enable sharding for db(shardCollection)
	colDocCursor, err = fromConn.Client.Database("config").Collection(
		"collections").Find(nil, bson.D{})
	for colDocCursor.Next(nil) {
		err = bson.Unmarshal(colDocCursor.Current, &colSpecDoc)
		if err != nil {
			LOG.Error("parse colDocCursor.Current[%v] failed", colDocCursor.Current)
			continue
		}

		if !colSpecDoc.Dropped {
			if filterList.IterateFilter(colSpecDoc.Ns) {
				LOG.Debug("Namespace is filtered. %v", colSpecDoc.Ns)
				continue
			}
			toNs := nsTrans.Transform(colSpecDoc.Ns)
			err = toConn.Client.Database("admin").RunCommand(nil, bson.D{{"shardCollection", toNs},
				{"key", colSpecDoc.Key}, {"unique", colSpecDoc.Unique}}).Err()
			if err != nil && !in(toNs, conf.Options.SkipNSShareKeyVerify) {
				LOG.Critical("Shard collection for ns %v of dest mongodb failed. %v", toNs, err)
				return errors.New(fmt.Sprintf("Shard collection for ns %v of dest mongodb failed. %v",
					toNs, err))
			}
			LOG.Info("Shard collection for ns %v of dest mongodb successful", toNs)
		}
	}
	if err = docCursor.Close(nil); err != nil {
		LOG.Critical("Close iterator of config.collections failed. %v", err)
	}

	LOG.Info("document syncer namespace spec for sharding successful")
	return nil
}

func StartIndexSync(indexMap map[utils.NS][]bson.D, toUrl string,
	nsTrans *transform.NamespaceTransform, background bool) (syncError error) {
	if conf.Options.FullSyncExecutorDebug {
		LOG.Info("full_sync.executor.debug set, no need to sync index")
		return nil
	}

	type IndexNS struct {
		ns        utils.NS
		indexList []bson.D
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

	var wg sync.WaitGroup
	wg.Add(collExecutorParallel)
	for i := 0; i < collExecutorParallel; i++ {
		nimo.GoRoutine(func() {
			var conn *utils.MongoCommunityConn
			var err error
			if conn, err = utils.NewMongoCommunityConn(toUrl, utils.VarMongoConnectModePrimary, true,
				utils.ReadWriteConcernLocal, utils.ReadWriteConcernMajority, conf.Options.TunnelMongoSslRootCaFile); err != nil {
				LOG.Error("write index but create client fail: %v", err)
				return
			}
			defer conn.Close()
			defer wg.Done()

			for {
				indexNs, ok := <-namespaces
				if !ok {
					break
				}
				ns := indexNs.ns
				toNS := ns
				if nsTrans != nil {
					toNS = utils.NewNS(nsTrans.Transform(ns.Str()))
				}

				for _, index := range indexNs.indexList {
					// ignore _id
					if utils.HaveIdIndexKey(index) {
						continue
					}

					newIndex := bson.D{}
					for _, v := range index {
						if v.Key == "ns" || v.Key == "v" || v.Key == "background" {
							continue
						}
						newIndex = append(newIndex, v)
					}
					newIndex = append(newIndex, primitive.E{Key: "background", Value: background})
					if out := conn.Client.Database(toNS.Database).RunCommand(nil, bson.D{
						{"createIndexes", toNS.Collection},
						{"indexes", []bson.D{newIndex}},
					}); out.Err() != nil {
						LOG.Warn("Create indexes for ns %v of dest mongodb failed. %v", ns, out.Err())
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
	fromReplset  string
	// destination mongodb url
	ToMongoUrl string
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
		replMetric:     utils.NewMetric(fromReplset, utils.TypeFull, utils.METRIC_TPS|utils.METRIC_SUCCESS),
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
	//sleep 1 second for metric routine exit gracefully
	time.Sleep(1 * time.Second)
}

func (syncer *DBSyncer) Start() (syncError error) {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	filterList := filter.NewDocFilterList()

	// get all namespace
	nsList, _, err := utils.GetDbNamespace(syncer.FromMongoUrl, filterList.IterateFilter,
		conf.Options.MongoSslRootCaFile)
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
	colExecutor := NewCollectionExecutor(collExecutorId, syncer.ToMongoUrl, toNS, syncer, conf.Options.TunnelMongoSslRootCaFile)
	if err := colExecutor.Start(); err != nil {
		return fmt.Errorf("start collectionSync failed: %v", err)
	}

	// splitter reader
	splitter := NewDocumentSplitter(syncer.FromMongoUrl, conf.Options.MongoSslRootCaFile, ns)
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
	wg.Add(conf.Options.FullSyncReaderParallelThread)
	for i := 0; i < conf.Options.FullSyncReaderParallelThread; i++ {
		go func() {
			defer wg.Done()
			for {
				reader, ok := <-splitter.readerChan
				if !ok || reader == nil {
					break
				}

				if err := syncer.splitSync(reader, colExecutor, collectionMetric); err != nil {
					LOG.Crashf("%v", err)
				}
			}
		}()
	}
	wg.Wait()
	LOG.Info("%s all readers finish, wait all writers finish", syncer)

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

func (syncer *DBSyncer) splitSync(reader *DocumentReader, colExecutor *CollectionExecutor,
	collectionMetric *CollectionMetric) error {
	bufferSize := conf.Options.FullSyncReaderDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)
	bufferByteSize := 0

	for {
		doc, err := reader.NextDoc()
		// doc, err := reader.NextDocMgo()
		if err != nil {
			return fmt.Errorf("splitter reader[%v] get next document failed: %v", reader, err)
		} else if doc == nil {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			syncer.replMetric.AddSuccess(uint64(len(buffer))) // only used to calculate the tps which is extract from "success"
			break
		}

		syncer.replMetric.AddGet(1)

		if bufferByteSize+len(doc) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			syncer.replMetric.AddSuccess(uint64(len(buffer))) // only used to calculate the tps which is extract from "success"
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// transform dbref for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.IncrSyncDBRef {
			var docData bson.D
			if err := bson.Unmarshal(doc, &docData); err != nil {
				LOG.Error("splitter reader[%v] do bson unmarshal %v failed. %v", reader, doc, err)
			} else {
				docData = transform.TransformDBRef(docData, reader.ns.Database, syncer.nsTrans)
				if v, err := bson.Marshal(docData); err != nil {
					LOG.Warn("splitter reader[%v] do bson marshal %v failed. %v", reader, docData, err)
				} else {
					doc = v
				}
			}
		}

		buffer = append(buffer, &doc)
		bufferByteSize += len(doc)
	}

	LOG.Info("splitter reader finishes: %v", reader)
	reader.Close()
	// reader.CloseMgo()
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
			ret.Progress = fmt.Sprintf("%.2f%%", float64(ret.FinishedCollection)/float64(ret.TotalCollection)*100)
		}

		return ret
	})

	/***************************************************/

}
