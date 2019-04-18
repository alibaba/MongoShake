package docsyncer

import (
	"errors"
	"fmt"
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/dbpool"
	"sync"
	"time"

	LOG "github.com/vinllen/log4go"
)

const (
	DocBufferCapacity = 1000
)

type DocumentSyncer struct {
	// source mongodb url
	fromMongoUrl 	string
	// destination mongodb url
	toMongoUrl 		string
	// namespace need to sync
	namespaces 		chan dbpool.NS
	// start time of sync
	startTime 		time.Time
	// destination mongodb connection
	toMongoConn		*dbpool.MongoConn

	replMetric 		*utils.ReplicationMetric
}

func NewDocumentSyncer(
	fromMongoUrl string,
	toMongoUrl string) *DocumentSyncer {

	syncer := &DocumentSyncer{
		fromMongoUrl: fromMongoUrl,
		toMongoUrl:   toMongoUrl,
		namespaces:   make(chan dbpool.NS, conf.Options.WorkerNum),
	}

	return syncer
}

func (syncer *DocumentSyncer) Start() error {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	if err := syncer.prepare(&wg); err != nil {
		return errors.New(fmt.Sprintf("document syncer init transfer error. %v", err))
	}

	var syncError error
	for i := 0; i < conf.Options.WorkerNum; i++ {
		ind := i
		nimo.GoRoutine(func() {
			for {
				ns, ok := <- syncer.namespaces
				if !ok {
					break
				}

				LOG.Info("Replayer-%v sync ns %v begin", ind, ns)
				if err := syncer.collectionSync(uint32(ind), ns); err != nil {
					LOG.Critical("Replayer-%v sync ns %v failed. %v", ind, ns, err)
					syncError = errors.New(fmt.Sprintf("document syncer sync ns %v failed. %v", ns, err))
				} else {
					LOG.Info("Replayer-%v sync ns %v successful", ind, ns)
				}

				wg.Done()
			}
			LOG.Info("Replayer-%v finish", ind)
		})
	}

	wg.Wait()
	close(syncer.namespaces)

	if syncer.toMongoConn != nil {
		syncer.toMongoConn.Close()
	}

	return syncError
}

func (syncer *DocumentSyncer) prepare(wg *sync.WaitGroup) (err error) {
	var nslist []dbpool.NS
	if nslist, err = GetAllNamespace(syncer.fromMongoUrl); err != nil {
		return err
	}

	if syncer.toMongoConn, err = dbpool.NewMongoConn(syncer.toMongoUrl,true); err != nil {
		return errors.New(fmt.Sprintf("Connect to dest mongo failed. %v", err))
	}

	wg.Add(len(nslist))

	nimo.GoRoutine(func() {
		for _, ns := range nslist {
			syncer.namespaces <- ns
		}
	})
	return nil
}

func (syncer *DocumentSyncer) collectionSync(replayerId uint32, ns dbpool.NS) error {
	reader := NewDocumentReader(syncer.fromMongoUrl, ns)

	toColName := fmt.Sprintf("%s_%s", ns.Collection, utils.APPNAME)

	toNS := dbpool.NS{Database:ns.Database, Collection:toColName}

	batchExecutor := NewCollectionExecutor(replayerId, syncer.toMongoUrl, toNS)
	batchExecutor.Start()

	buffer := make([]*bson.Raw, 0, DocBufferCapacity)

	var err error
	for {
		var doc *bson.Raw
		if doc, err = reader.NextDoc(); err != nil {
			return errors.New(fmt.Sprintf("Get next document from ns %v of src mongodb failed. %v", ns, err))
		} else if doc == nil {
			batchExecutor.Sync(buffer)
			if err := batchExecutor.Wait(); err != nil {
				return err
			}
			break
		}
		buffer = append(buffer, doc)
		if len(buffer) >= DocBufferCapacity {
			batchExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, DocBufferCapacity)
		}
	}

	LOG.Info("Replayer-%v sync index ns %v begin", replayerId, ns)

	if indexes, err := reader.GetIndexes(); err != nil {
		return errors.New(fmt.Sprintf("Get indexes from ns %v of src mongodb failed. %v", ns, err))
	} else {
		for _, index := range indexes {
			index.Background = false
			if err = syncer.toMongoConn.Session.DB(toNS.Database).C(toNS.Collection).EnsureIndex(index); err != nil {
				return errors.New(fmt.Sprintf("Create indexes for ns %v of dest mongodb failed. %v", ns, err))
			}
		}
	}

	reader.Close()
	LOG.Info("Replayer-%v sync index for ns %v successful", replayerId, ns)

	if conf.Options.CollectionRename {
		err := syncer.toMongoConn.Session.DB("admin").
			Run(bson.D{{"renameCollection",toNS.Str()},{"to",ns.Str()}},nil)
		if err != nil {
			return errors.New(fmt.Sprintf("Rename Collection ns %v of dest mongodb to ns %v failed. %v",
							toNS, ns, err))
		}
		LOG.Info("Rename collection ns %v of dest mongodb to ns %v successful", toNS, ns)
	}

	return nil
}
