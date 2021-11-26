package docsyncer

import (
	"errors"
	"fmt"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"github.com/vinllen/mongo-go-driver/mongo"
	"sync/atomic"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"

	"sync"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
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

	docBatch chan []*bson2.D

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
		writeContern := utils.ReadWriteConcernDefault
		if conf.Options.FullSyncExecutorMajorityEnable {
			writeContern = utils.ReadWriteConcernDefault
		}
		if colExecutor.conn, err = utils.NewMongoCommunityConn(colExecutor.mongoUrl,
			utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, writeContern,
			colExecutor.sslRootFile); err != nil {
			return err
		}
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson2.D, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		//var docSession *mgo.Session
		//if !conf.Options.FullSyncExecutorDebug {
		//	docSession = colExecutor.conn.Session.Clone()
		//}

		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, colExecutor.conn, colExecutor.syncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Sync(docs []*bson2.D) {
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

	//session *mgo.Session
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
		conn:     conn,
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

func (exec *DocExecutor) doSync(docs []*bson2.D) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var models []mongo.WriteModel
	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	// qps limit if enable
	if exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	if conf.Options.LogLevel == utils.VarLogLevelDebug {
		LOG.Debug("DBSyncer id[%v] doSync BulkWrite with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
			docs[0], docs[len(docs)-1])
	}

	res, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, models, nil)

	if err != nil {
		LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
			len(models), ns, err, res)

		index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
		if index == -1 {
			return fmt.Errorf("bulk run failed[%v]", err)
		} else if !dup {
			return fmt.Errorf("bulk run message[%v], failed[%v], index[%v] dup[%v]", docs[index], errMsg, index, dup)
		} else {
			LOG.Warn("dup error found, try to solve error")
		}

		return exec.tryOneByOne(docList, index, exec.conn.Client.Database(ns.Database).Collection(ns.Collection))
	}

	return nil
}

// heavy operation. insert data one by one and handle the return error.
func (exec *DocExecutor) tryOneByOne(input []interface{}, index int, coll *mongo.Collection) error {
	for i := index; i < len(input); i++ {
		raw := input[i]
		var docD bson.D
		if err := bson.Unmarshal(raw.(*bson.Raw).Data, &docD); err != nil {
			return fmt.Errorf("unmarshal data[%v] failed[%v]", raw, err)
		}

		_, err := coll.InsertOne(nil, docD)
		//err := collectionHandler.Insert(docD)
		if err == nil {
			continue
		} else if !utils.DuplicateKey(err) {
			return err
		}

		// only handle duplicate key error
		id := oplog.GetKey(docD, "")


		// orphan document enable and source is sharding
		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docD, coll.Database().Name() + "." + coll.Name()) {
				LOG.Info("orphan document with _id[%v] filter", id)
				continue
			}
		}

		if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
			return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, "+
				"or enable %v to solve, but full-sync stage needs restart",
				err, "full_sync.executor.insert_on_dup_update")
		} else {
			// convert insert operation to update operation
			if id == nil {
				return fmt.Errorf("parse '_id' from document[%v] failed", docD)
			}
			if _, err := coll.UpdateOne(nil, bson2.D{{"_id", id}}, docD); err != nil {
				return fmt.Errorf("convert oplog[%v] from insert to update run failed[%v]", docD, err)
			}
		}
	}

	// all finish
	return nil
}
