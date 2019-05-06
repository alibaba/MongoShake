package docsyncer

import (
	"errors"
	"fmt"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/dbpool"
	"sync"
	"sync/atomic"

	"mongoshake/collector/configure"

	LOG "github.com/vinllen/log4go"
)

var GlobalCollExecutorId int32 = -1

var GlobalDocExecutorId int32 = -1

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// mongo url
	mongoUrl string

	ns dbpool.NS

	wg sync.WaitGroup

	conn *dbpool.MongoConn

	docBatch chan []*bson.Raw
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, mongoUrl string, ns dbpool.NS) *CollectionExecutor {
	return &CollectionExecutor{
		id:       id,
		mongoUrl: mongoUrl,
		ns:       ns,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	var err error
	if colExecutor.conn, err = dbpool.NewMongoConn(colExecutor.mongoUrl, true); err != nil {
		LOG.Critical("Connect to mongodb url=%s failed. %v", colExecutor.mongoUrl, err)
		return errors.New(fmt.Sprintf("Connect to mongodb url=%s failed. %v", colExecutor.mongoUrl, err))
	}

	parallel := conf.Options.ReplayerDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		docSession := colExecutor.conn.Session.Clone()
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, docSession)
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
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, session *mgo.Session) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		session:     session,
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
	ns := exec.colExecutor.ns

	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
	}

	if err := exec.session.DB(ns.Database).C(ns.Collection).Insert(docList...); err != nil {
		return fmt.Errorf("Insert docs [%v] into ns %v of dest mongo failed. %v", docs, ns, err)
	}

	return nil
}
