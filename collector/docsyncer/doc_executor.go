package docsyncer

import (
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync/atomic"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"sync"

	LOG "github.com/vinllen/log4go"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId  int32 = -1
)

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// mongo url
	mongoUrl    string
	sslRootFile string

	ns utils.NS

	wg sync.WaitGroup
	// batchCount int64

	conn *utils.MongoCommunityConn

	docBatch chan []*bson.Raw

	// not own
	syncer *DBSyncer
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, mongoUrl string, ns utils.NS, syncer *DBSyncer, sslRootFile string) *CollectionExecutor {
	return &CollectionExecutor{
		id:          id,
		mongoUrl:    mongoUrl,
		sslRootFile: sslRootFile,
		ns:          ns,
		syncer:      syncer,
		// batchCount: 0,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	var err error
	if !conf.Options.FullSyncExecutorDebug {
		writeConcern := utils.ReadWriteConcernDefault
		if conf.Options.FullSyncExecutorMajorityEnable {
			writeConcern = utils.ReadWriteConcernMajority
		}
		if colExecutor.conn, err = utils.NewMongoCommunityConn(colExecutor.mongoUrl,
			utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, writeConcern,
			colExecutor.sslRootFile); err != nil {
			return err
		}
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		// Client is a handle representing a pool of connections, can be use by multi routines
		// You Can get one idle connection, if all is idle, then always get the same one
		// connections pool default parameter(min_conn:0 max_conn:100 create_conn_once:2)
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, colExecutor.conn, colExecutor.syncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)
	// atomic.AddInt64(&colExecutor.batchCount, 1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	/*for v := atomic.LoadInt64(&colExecutor.batchCount); v != 0; {
		utils.YieldInMs(1000)
		LOG.Info("CollectionExecutor[%v %v] wait batchCount[%v] == 0", colExecutor.ns, colExecutor.id, v)
	}*/

	close(colExecutor.docBatch)
	if !conf.Options.FullSyncExecutorDebug {
		colExecutor.conn.Close()
	}

	for _, exec := range colExecutor.executors {
		if exec.error != nil {
			return errors.New(fmt.Sprintf("sync ns %v failed. %v", colExecutor.ns, exec.error))
		}
	}
	return nil
}

type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// colExecutor, not owned
	colExecutor *CollectionExecutor

	conn *utils.MongoCommunityConn

	error error

	// not own
	syncer *DBSyncer
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, conn *utils.MongoCommunityConn,
	syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		conn:        conn,
		syncer:      syncer,
	}
}

func (exec *DocExecutor) String() string {
	return fmt.Sprintf("DocExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns)
}

func (exec *DocExecutor) start() {
	if !conf.Options.FullSyncExecutorDebug {
		defer exec.conn.Close()
	}

	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// since v2.4.11: panic directly if meets error
				LOG.Crashf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
		// atomic.AddInt64(&exec.colExecutor.batchCount, -1)
	}
}

// use by full sync
func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var models []mongo.WriteModel
	for _, doc := range docs {

		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			var docData bson.D
			if err := bson.Unmarshal(*doc, &docData); err != nil {
				LOG.Error("doSync do bson unmarshal %v failed. %v", doc, err)
			}
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docData, ns.Database+"."+ns.Collection) {
				LOG.Info("orphan document [%v] filter", doc)
				continue
			}
		}

		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	// qps limit if enable
	if exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	if conf.Options.LogLevel == utils.VarLogLevelDebug {
		var docBeg, docEnd bson.M
		bson.Unmarshal(*docs[0], &docBeg)
		bson.Unmarshal(*docs[len(docs)-1], &docEnd)
		LOG.Debug("DBSyncer id[%v] doSync BulkWrite with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
			docBeg, docEnd)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, models, opts)

	if err != nil {
		if _, ok := err.(mongo.BulkWriteException); !ok {
			return fmt.Errorf("bulk run failed[%v]", err)
		}

		LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
			len(models), ns, (err.(mongo.BulkWriteException)).WriteErrors[0], res)

		var updateModels []mongo.WriteModel
		for _, wError := range (err.(mongo.BulkWriteException)).WriteErrors {
			if utils.DuplicateKey(wError) {
				if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
					return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, "+
						"or enable %v to solve, but full-sync stage needs restart",
						wError, "full_sync.executor.insert_on_dup_update")
				}

				dupDocument := *docs[wError.Index]
				var updateFilter bson.D
				updateFilterBool := false
				var docData bson.D
				if err := bson.Unmarshal(dupDocument, &docData); err == nil {
					for _, bsonE := range docData {
						if bsonE.Key == "_id" {
							updateFilter = bson.D{bsonE}
							updateFilterBool = true
						}
					}
				}
				if updateFilterBool == false {
					return fmt.Errorf("duplicate key error[%v], can't get _id from document", wError)
				}
				updateModels = append(updateModels, mongo.NewUpdateOneModel().
					SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))
			} else {
				return fmt.Errorf("bulk run failed[%v]", wError)
			}
		}

		if len(updateModels) != 0 {
			opts := options.BulkWrite().SetOrdered(false)
			_, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, updateModels, opts)
			if err != nil {
				return fmt.Errorf("bulk run updateForInsert failed[%v]", err)
			}
			LOG.Debug("updateForInsert succ updateModels.len:%d updateModules[0]:%v\n",
				len(updateModels), updateModels[0])
		} else {
			return fmt.Errorf("bulk run failed[%v]", err)
		}
	}

	return nil
}
