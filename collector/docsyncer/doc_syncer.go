package docsyncer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	"github.com/alibaba/MongoShake/v2/collector/transform"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	MaxBufferByteSize = 12 * 1024 * 1024
)

func IsShardingToSharding(fromIsSharding bool, toConn *utils.MongoCommunityConn) bool {
	if conf.Options.FullSyncExecutorDebug {
		l.Logger.Infof("full_sync.executor.debug set, no need to check IsShardingToSharding")
		return false
	}

	if conf.Options.FullSyncDoNotShardDest {
		l.Logger.Infof("full_sync.do_not_shard_destination set, no need to check IsShardingToSharding")
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

	l.Logger.Infof("replication from [%s] to [%s]", source, target)
	if source == "sharding" && target == "sharding" {
		return true
	}
	return false
}

func in(target string, strArray []string) bool {
	sort.Strings(strArray)
	index := sort.SearchStrings(strArray, target)
	if index < len(strArray) && strArray[index] == target {
		return true
	}
	return false
}

func StartDropDestCollection(nsSet map[utils.NS]struct{}, toConn *utils.MongoCommunityConn,
	nsTrans *transform.NamespaceTransform) error {
	if conf.Options.FullSyncExecutorDebug {
		l.Logger.Infof("full_sync.executor.debug set, no need to drop collection")
		return nil
	}

	for ns := range nsSet {
		toNS := utils.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.FullSyncCollectionDrop {
			// do not drop
			colNames, err := toConn.Client.Database(toNS.Database).ListCollectionNames(nil,
				utils.GetListCollectionQueryCondition(toConn))
			if err != nil {
				l.Logger.Criticalf("Get collection names of db %v of dest mongodb failed. %v", toNS.Database, err)
				return err
			}

			// judge whether toNs exists
			for _, colName := range colNames {
				if colName == toNS.Collection {
					l.Logger.Warnf("ns %v to be synced already exists in dest mongodb", toNS)
					break
				}
			}
		} else {
			// need drop
			err := toConn.Client.Database(toNS.Database).Collection(toNS.Collection).Drop(nil)
			if err != nil && err.Error() != "ns not found" {
				l.Logger.Criticalf("Drop collection ns %v of dest mongodb failed. %v", toNS, err)
				return errors.New(fmt.Sprintf("Drop collection ns %v of dest mongodb failed. %v", toNS, err))
			}
		}
	}
	return nil
}

func StartNamespaceSpecSyncForSharding(csUrl string, toConn *utils.MongoCommunityConn,
	nsTrans *transform.NamespaceTransform) error {
	l.Logger.Infof("document syncer namespace spec for sharding begin")

	var fromConn *utils.MongoCommunityConn
	var err error
	if fromConn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault,
		conf.Options.MongoSslRootCaFile); err != nil {
		l.Logger.Infof("connect to [%s] failed. err[%v]", csUrl, err)
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
			l.Logger.Errorf("parse docCursor.Current[%v] failed", docCursor.Current)
			continue
		}
		if dbSpecDoc.Partitioned {
			if filterList.IterateFilter(dbSpecDoc.Db + ".$cmd") {
				l.Logger.Debugf("db:%v is filtered", dbSpecDoc.Db)
				continue
			}
			var toDbSpecDoc dbSpec
			toDbList := dbTrans.Transform(dbSpecDoc.Db)
			for _, toDb := range toDbList {

				err = toConn.Client.Database("config").Collection("databases").FindOne(nil,
					bson.D{{"_id", toDb}}).Decode(&toDbSpecDoc)
				if err == nil && toDbSpecDoc.Partitioned {
					continue
				}
				err = toConn.Client.Database("admin").RunCommand(nil,
					bson.D{{"enablesharding", toDb}}).Err()
				if err != nil {
					l.Logger.Criticalf("enable sharding for db %v of dest mongodb failed. %v", toDb, err)
					return errors.New(fmt.Sprintf("enable sharding for db %v of dest mongodb failed. %v",
						toDb, err))
				}
				l.Logger.Infof("enable sharding for db %v of dest mongodb successful", toDb)
			}
		}
	}
	if err := docCursor.Close(nil); err != nil {
		l.Logger.Criticalf("close iterator of config.database failed. %v", err)
	}

	type colSpec struct {
		Ns      string    `bson:"_id"`
		Key     *bson.Raw `bson:"key"`
		Unique  bool      `bson:"unique"`
		Dropped bool      `bson:"dropped"`
	}
	var colSpecDoc colSpec
	var collDocCursor *mongo.Cursor
	// enable sharding for db(shardCollection)
	collDocCursor, err = fromConn.Client.Database("config").Collection(
		"collections").Find(nil, bson.D{})
	if err != nil {
		return fmt.Errorf("find config.collections failed:%v", err)
	}
	for collDocCursor.Next(nil) {
		err = bson.Unmarshal(collDocCursor.Current, &colSpecDoc)
		if err != nil {
			l.Logger.Errorf("parse colDocCursor.Current[%v] failed", collDocCursor.Current)
			continue
		}

		if !colSpecDoc.Dropped {
			if filterList.IterateFilter(colSpecDoc.Ns) {
				l.Logger.Debugf("Namespace is filtered. %v", colSpecDoc.Ns)
				continue
			}
			toNs := nsTrans.Transform(colSpecDoc.Ns)
			err = toConn.Client.Database("admin").RunCommand(nil,
				bson.D{
					{"shardCollection", toNs},
					{"key", colSpecDoc.Key},
					{"unique", colSpecDoc.Unique},
				}).Err()
			if err != nil && !in(toNs, conf.Options.SkipNSShareKeyVerify) {
				l.Logger.Criticalf("shardCollection for ns %v of dest mongodb failed. %v", toNs, err)
				return fmt.Errorf("shardCollection for ns %v of dest mongodb failed. %v", toNs, err)
			}
			l.Logger.Infof("shardCollection for ns %v of dest mongodb succeed", toNs)
		}
	}
	if err = docCursor.Close(nil); err != nil {
		l.Logger.Criticalf("close iterator of config.collections failed. %v", err)
	}

	l.Logger.Infof("document syncer namespace spec for sharding succeed")
	return nil
}

func StartIndexSync(indexMap map[utils.NS][]bson.D, toUrl string,
	nsTrans *transform.NamespaceTransform, background bool) (syncError error) {
	if conf.Options.FullSyncExecutorDebug {
		l.Logger.Infof("full_sync.executor.debug set, no need to sync index")
		return nil
	}

	type IndexNS struct {
		ns        utils.NS
		indexList []bson.D
	}

	l.Logger.Infof("start writing index with background[%v], indexMap length[%v]", background, len(indexMap))
	if len(indexMap) == 0 {
		l.Logger.Infof("finish writing index, but no data")
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
			conn, err = utils.NewMongoCommunityConn(toUrl, utils.VarMongoConnectModePrimary, true,
				utils.ReadWriteConcernLocal, utils.ReadWriteConcernMajority, conf.Options.TunnelMongoSslRootCaFile)
			if err != nil {
				l.Logger.Errorf("write index but create client fail: %v", err)
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
						l.Logger.Warnf("Create indexes for ns %v of dest mongodb failed. %v", ns, out.Err())
					}
				}
				l.Logger.Infof("Create indexes for ns %v of dest mongodb finish", toNS)
			}
		})
	}

	wg.Wait()
	l.Logger.Infof("finish writing index")
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

// DBSyncer (1 shard -> 1 DBSyncer)
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

	totalCollections      int64
	finishedCollections   int64
	processingCollections int64
	waitingCollections    int64
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
	syncer.updateCollectionProgressMetrics()

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
	l.Logger.Infof("syncer[%v] closed", syncer)
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
		l.Logger.Infof("%s finish, but no data", syncer)
		return
	}

	// create metric for each collection
	for _, ns := range nsList {
		metric := NewCollectionMetric()
		syncer.metricNsMap[ns] = metric
		syncer.updateSingleCollectionProgressMetric(ns, metric)
	}
	atomic.StoreInt64(&syncer.totalCollections, int64(len(nsList)))
	atomic.StoreInt64(&syncer.waitingCollections, int64(len(nsList)))
	syncer.updateCollectionProgressMetrics()

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

				l.Logger.Infof("%s collExecutor-%d sync ns %v to %v begin", syncer, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					l.Logger.Criticalf("%s collExecutor-%d sync ns %v to %v failed. %v",
						syncer, collExecutorId, ns, toNS, err)
					syncError = fmt.Errorf("document syncer sync ns %v to %v failed. %v", ns, toNS, err)
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					l.Logger.Infof("%s collExecutor-%d sync ns %v to %v successful. db syncer-%d progress %v%%",
						syncer, collExecutorId, ns, toNS, syncer.id, process)
				}
				wg.Done()
			}
			l.Logger.Infof("%s collExecutor-%d finish", syncer, collExecutorId)
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
	atomic.StoreUint64(&collectionMetric.TotalCount, splitter.count)
	syncer.markCollectionProcessing(ns, collectionMetric)

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
					l.Logger.Panicf("%v", err)
				}
			}
		}()
	}
	wg.Wait()
	l.Logger.Infof("%s all readers finish, wait all writers finish", syncer)

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

	// Verify that the number of documents actually read roughly matches
	// the expected total from collStats. A large discrepancy indicates
	// that a cursor was prematurely killed and the resume returned empty
	// results, causing silent data loss.
	totalCount := atomic.LoadUint64(&collectionMetric.TotalCount)
	finishCount := atomic.LoadUint64(&collectionMetric.FinishCount)
	if totalCount > 0 && finishCount > 0 {
		ratio := float64(finishCount) / float64(totalCount)
		if ratio < 0.9 {
			l.Logger.Criticalf("collection[%v] sync completed but finishCount[%v] is significantly less than totalCount[%v] (%.1f%%). "+
				"Possible data loss: cursor may have been killed and resume returned empty. "+
				"Verify target data manually!",
				ns, finishCount, totalCount, ratio*100)
		} else {
			l.Logger.Infof("collection[%v] sync verification: finishCount[%v]/totalCount[%v] = %.1f%%",
				ns, finishCount, totalCount, ratio*100)
		}
	}

	// set collection finish
	syncer.markCollectionFinished(ns, collectionMetric)

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
			syncer.addCollectionFinishedDocs(reader.ns, collectionMetric, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			syncer.replMetric.AddSuccess(uint64(len(buffer))) // only used to calculate the tps which is extract from "success"
			break
		}

		syncer.replMetric.AddGet(1)

		if bufferByteSize+len(doc) > MaxBufferByteSize || len(buffer) >= bufferSize {
			syncer.addCollectionFinishedDocs(reader.ns, collectionMetric, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			syncer.replMetric.AddSuccess(uint64(len(buffer))) // only used to calculate the tps which is extract from "success"
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// transform dbRef for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.IncrSyncDBRef {
			var docData bson.D
			if err := bson.Unmarshal(doc, &docData); err != nil {
				l.Logger.Errorf("splitter reader[%v] do bson unmarshal %v failed. %v", reader, doc, err)
			} else {
				docData = transform.TransformDBRef(docData, reader.ns.Database, syncer.nsTrans)
				if v, err := bson.Marshal(docData); err != nil {
					l.Logger.Warnf("splitter reader[%v] do bson marshal %v failed. %v", reader, docData, err)
				} else {
					doc = v
				}
			}
		}

		buffer = append(buffer, &doc)
		bufferByteSize += len(doc)
	}

	l.Logger.Infof("splitter reader finishes: %v", reader)
	reader.Close()
	// reader.CloseMgo()
	return nil
}

// RestAPI restful api
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
			switch collectionMetric.Status() {
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
}

func (syncer *DBSyncer) markCollectionProcessing(ns utils.NS, collectionMetric *CollectionMetric) {
	if collectionMetric == nil {
		return
	}

	if collectionMetric.Status() == StatusWaitStart {
		atomic.AddInt64(&syncer.waitingCollections, -1)
		atomic.AddInt64(&syncer.processingCollections, 1)
	}
	collectionMetric.SetStatus(StatusProcessing)
	syncer.updateCollectionProgressMetrics()
	syncer.updateSingleCollectionProgressMetric(ns, collectionMetric)
}

func (syncer *DBSyncer) markCollectionFinished(ns utils.NS, collectionMetric *CollectionMetric) {
	if collectionMetric == nil {
		return
	}

	switch collectionMetric.Status() {
	case StatusWaitStart:
		atomic.AddInt64(&syncer.waitingCollections, -1)
	case StatusProcessing:
		atomic.AddInt64(&syncer.processingCollections, -1)
	}
	atomic.AddInt64(&syncer.finishedCollections, 1)
	collectionMetric.SetStatus(StatusFinish)
	syncer.updateCollectionProgressMetrics()
	syncer.updateSingleCollectionProgressMetric(ns, collectionMetric)
}

func (syncer *DBSyncer) updateCollectionProgressMetrics() {
	total := atomic.LoadInt64(&syncer.totalCollections)
	finished := atomic.LoadInt64(&syncer.finishedCollections)
	progressRatio := 1.0
	if total > 0 {
		progressRatio = float64(finished) / float64(total)
	}

	utils.FullSyncCollectionsTotalProm.WithLabelValues(syncer.fromReplset, utils.TypeFull).
		Set(float64(total))
	utils.FullSyncCollectionsFinishedProm.WithLabelValues(syncer.fromReplset, utils.TypeFull).
		Set(float64(finished))
	utils.FullSyncCollectionsProcessingProm.WithLabelValues(syncer.fromReplset, utils.TypeFull).
		Set(float64(atomic.LoadInt64(&syncer.processingCollections)))
	utils.FullSyncCollectionsWaitingProm.WithLabelValues(syncer.fromReplset, utils.TypeFull).
		Set(float64(atomic.LoadInt64(&syncer.waitingCollections)))
	utils.FullSyncCollectionsProgressRatioProm.WithLabelValues(syncer.fromReplset, utils.TypeFull).
		Set(progressRatio)
}

func (syncer *DBSyncer) addCollectionFinishedDocs(ns utils.NS, collectionMetric *CollectionMetric, n uint64) {
	if collectionMetric == nil {
		return
	}
	atomic.AddUint64(&collectionMetric.FinishCount, n)
	syncer.updateSingleCollectionProgressMetric(ns, collectionMetric)
}

func (syncer *DBSyncer) updateSingleCollectionProgressMetric(ns utils.NS, collectionMetric *CollectionMetric) {
	if collectionMetric == nil {
		return
	}
	utils.FullSyncCollectionStatusProm.WithLabelValues(syncer.fromReplset, utils.TypeFull, ns.Database, ns.Collection).
		Set(collectionMetric.StatusCode())
	utils.FullSyncCollectionDocsTotalProm.WithLabelValues(syncer.fromReplset, utils.TypeFull, ns.Database, ns.Collection).
		Set(float64(atomic.LoadUint64(&collectionMetric.TotalCount)))
	utils.FullSyncCollectionDocsFinishedProm.WithLabelValues(syncer.fromReplset, utils.TypeFull, ns.Database, ns.Collection).
		Set(float64(atomic.LoadUint64(&collectionMetric.FinishCount)))
	utils.FullSyncCollectionProgressRatioProm.WithLabelValues(syncer.fromReplset, utils.TypeFull, ns.Database, ns.Collection).
		Set(collectionMetric.ProgressRatio())
}
