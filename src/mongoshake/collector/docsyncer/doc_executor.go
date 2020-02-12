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
	if colExecutor.conn, err = utils.NewMongoConn(colExecutor.mongoUrl, utils.ConnectModePrimary, true); err != nil {
		return err
	}
	if conf.Options.MajorityWriteFull {
		colExecutor.conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	}

	parallel := conf.Options.FullSyncDocumentParallel
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
	if len(docs) == 0 {
		return nil
	}

	ns := exec.colExecutor.ns

	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
	}

	collectionHandler := exec.session.DB(ns.Database).C(ns.Collection)

	if err := collectionHandler.Insert(docList...); err != nil {
		LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v], try to handle error",
			len(docList), ns, err)
		return exec.tryOneByOne(docList, collectionHandler)
	}

	return nil
}

// heavy operation. insert data one by one and handle the return error.
func (exec *DocExecutor) tryOneByOne(input []interface{}, collectionHandler *mgo.Collection) error {
	for _, raw := range input {
		parsed := new(oplog.PartialLog)
		if err := bson.Unmarshal(raw.(*bson.Raw).Data, parsed); err != nil {
			return fmt.Errorf("unmarshal data[%v] failed[%v]", raw, err)
		}

		err := collectionHandler.Insert(parsed)
		if err == nil {
			continue
		} else if !mgo.IsDup(err) {
			return err
		}

		// only handle duplicate key error

		// orphan document enable and source is sharding
		if conf.Options.FullSyncExecutorFilterOrphanDocument && len(conf.Options.MongoUrls) > 1 {
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(raw.(*bson.Raw), parsed.Namespace) {
				continue
			}
		}

		if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
			return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, " +
				"or enable %v to solve, but full-sync stage needs restart",
				err, "full_sync.executor.insert_on_dup_update")
		} else {
			// convert insert operation to update operation
			id := oplog.GetKey(parsed.Object, "")
			if id == nil {
				return fmt.Errorf("parse '_id' from oplog[%v] failed", parsed)
			}
			if err := collectionHandler.UpdateId(id, parsed); err != nil {
				return fmt.Errorf("convert oplog[%v] from insert to update run failed[%v]", parsed, err)
			}
		}
	}

	// all finish
	return nil
}
