package docsyncer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId int32 = -1
)

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// mongo url
	mongoUrl string

	ns utils.NS

	wg sync.WaitGroup

	conn *utils.MongoConn

	docBatch chan []*bson.Raw

	// not own
	syncer *DBSyncer
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, mongoUrl string, ns utils.NS, syncer *DBSyncer) *CollectionExecutor {
	return &CollectionExecutor{
		id:       id,
		mongoUrl: mongoUrl,
		ns:       ns,
		syncer:   syncer,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	var err error
	if colExecutor.conn, err = utils.NewMongoConn(colExecutor.mongoUrl, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); err != nil {
		return err
	}
	if conf.Options.FullSyncExecutorMajorityEnable {
		colExecutor.conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		docSession := colExecutor.conn.Session.Clone()
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, docSession, colExecutor.syncer)
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

	colExecutor.wg.Add(1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	close(colExecutor.docBatch)
	colExecutor.conn.Close()

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

	session *mgo.Session

	error error

	// not own
	syncer *DBSyncer
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, session *mgo.Session, syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		session:     session,
		syncer:      syncer,
	}
}

func (exec *DocExecutor) start() {
	defer exec.session.Close()
	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
			}
		}
		exec.colExecutor.wg.Done()
	}
}

func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
	}

	// qps limit if enable
	if exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	collectionHandler := exec.session.DB(ns.Database).C(ns.Collection)
	bulk := collectionHandler.Bulk()
	bulk.Insert(docList...)
	if _, err := bulk.Run(); err != nil {
		LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v]",
			len(docList), ns, err)
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
		if index == -1 {
			return fmt.Errorf("bulk run failed[%v]", err)
		} else if !dup {
			var docD bson.D
			bson.Unmarshal(docs[index].Data, &docD)
			return fmt.Errorf("bulk run message[%v], failed[%v], index[%v] dup[%v]", docD, errMsg, index, dup)
		} else {
			LOG.Warn("dup error found, try to solve error")
		}

		return exec.tryOneByOne(docList, index, collectionHandler)
	}

	return nil
}

// heavy operation. insert data one by one and handle the return error.
func (exec *DocExecutor) tryOneByOne(input []interface{}, index int, collectionHandler *mgo.Collection) error {
	for i := index; i < len(input); i++ {
		raw := input[i]
		var docD bson.D
		if err := bson.Unmarshal(raw.(*bson.Raw).Data, &docD); err != nil {
			return fmt.Errorf("unmarshal data[%v] failed[%v]", raw, err)
		}

		err := collectionHandler.Insert(docD)
		if err == nil {
			continue
		} else if !mgo.IsDup(err) {
			return err
		}

		// only handle duplicate key error
		id := oplog.GetKey(docD, "")

		// orphan document enable and source is sharding
		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docD, collectionHandler.FullName) {
				LOG.Info("orphan document with _id[%v] filter", id)
				continue
			}
		}

		if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
			return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, " +
				"or enable %v to solve, but full-sync stage needs restart",
				err, "full_sync.executor.insert_on_dup_update")
		} else {
			// convert insert operation to update operation
			if id == nil {
				return fmt.Errorf("parse '_id' from document[%v] failed", docD)
			}
			if err := collectionHandler.UpdateId(id, docD); err != nil {
				return fmt.Errorf("convert oplog[%v] from insert to update run failed[%v]", docD, err)
			}
		}
	}

	// all finish
	return nil
}
