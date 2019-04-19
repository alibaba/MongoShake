package docsyncer

import (
	"errors"
	"fmt"
	"github.com/vinllen/mgo/bson"
	"mongoshake/dbpool"
	"sync"
	"sync/atomic"

	"mongoshake/collector/configure"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
)

var GlobalDocExecutorId int32 = -1


type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	replayerId uint32
	// mongo url
	mongoUrl string

	ns dbpool.NS

	wg sync.WaitGroup

	docBatch chan []*bson.Raw
}

func NewCollectionExecutor(ReplayerId uint32, mongoUrl string, ns dbpool.NS) *CollectionExecutor {
	return &CollectionExecutor{
		replayerId: ReplayerId,
		mongoUrl:   mongoUrl,
		ns:         ns,
	}
}

func (batchExecutor *CollectionExecutor) Start() {
	parallel := conf.Options.ReplayerDocumentParallel
	batchExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), batchExecutor)
		go executors[i].start()
	}
	batchExecutor.executors = executors
}

func (batchExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	batchExecutor.wg.Add(1)
	batchExecutor.docBatch <- docs
}

func (batchExecutor *CollectionExecutor) Wait() error {
	batchExecutor.wg.Wait()
	close(batchExecutor.docBatch)

	for _, exec := range batchExecutor.executors {
		if exec.error != nil {
			return errors.New(fmt.Sprintf("batch sync ns %v failed. %v", batchExecutor.ns, exec.error))
		}
	}
	return nil
}


type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// batchExecutor, not owned
	batchExecutor *CollectionExecutor

	error error
	// mongo connection
	session *mgo.Session
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, batchExecutor *CollectionExecutor) *DocExecutor {
	return &DocExecutor{
		id:            	id,
		batchExecutor: 	batchExecutor,
	}
}

func (exec *DocExecutor) start() {
	for {
		docs, ok := <- exec.batchExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
			}
		}

		exec.batchExecutor.wg.Done()
	}

	exec.dropConnection()

	LOG.Info("document replayer-%d executor-%d end",
		exec.batchExecutor.replayerId, exec.id)
}

func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if !exec.ensureConnection() {
		return errors.New("network connection lost. we would retry for next connecting")
	}

	ns := exec.batchExecutor.ns

	var idocs []interface{}
	for _, doc := range docs {
		idocs = append(idocs, doc)
	}

	if err := exec.session.DB(ns.Database).C(ns.Collection).Insert(idocs...); err != nil {
		return fmt.Errorf("Insert docs [%v] into ns %v of dest mongo failed. %v", docs, ns, err)
	}

	return nil
}


func (exec *DocExecutor) ensureConnection() bool {
	// reconnect if necessary
	if exec.session == nil {
		if conn, err := dbpool.NewMongoConn(exec.batchExecutor.mongoUrl, true); err != nil {
			LOG.Critical("Connect to mongo cluster failed. %v", err)
			return false
		} else {
			exec.session = conn.Session
		}
	}

	return true
}

func (exec *DocExecutor) dropConnection() {
	if exec.session != nil {
		exec.session.Close()
		exec.session = nil
	}
}
