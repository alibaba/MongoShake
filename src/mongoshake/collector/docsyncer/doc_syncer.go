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


type DocumentSyncer struct {
	// source mongodb url
	fromMongoUrl 	string
	// destination mongodb url
	toMongoUrl 		string
	// namespace need to sync
	namespaces 		chan dbpool.NS
	// start time of sync
	startTime 		time.Time

	replMetric 		*utils.ReplicationMetric
}

func NewDocumentSyncer(
	fromMongoUrl string,
	toMongoUrl string) *DocumentSyncer {

	syncer := &DocumentSyncer{
		fromMongoUrl: fromMongoUrl,
		toMongoUrl:   toMongoUrl,
		namespaces:   make(chan dbpool.NS, conf.Options.ReplayerCollectionParallel),
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
	for i := 0; i < conf.Options.ReplayerCollectionParallel; i++ {
		ind := i
		nimo.GoRoutine(func() {
			for {
				ns, ok := <- syncer.namespaces
				if !ok {
					break
				}

				LOG.Info("document replayer-%v sync ns %v begin", ind, ns)
				if err := syncer.collectionSync(uint32(ind), ns); err != nil {
					LOG.Critical("document replayer-%v sync ns %v failed. %v", ind, ns, err)
					syncError = errors.New(fmt.Sprintf("document syncer sync ns %v failed. %v", ns, err))
				} else {
					LOG.Info("document replayer-%v sync ns %v successful", ind, ns)
				}

				wg.Done()
			}
			LOG.Info("document replayer-%v finish", ind)
		})
	}

	wg.Wait()
	close(syncer.namespaces)

	return syncError
}

func (syncer *DocumentSyncer) prepare(wg *sync.WaitGroup) error {
	var nsList []dbpool.NS
	var err error
	if nsList, err = GetAllNamespace(syncer.fromMongoUrl); err != nil {
		return err
	}

	var conn *dbpool.MongoConn
	if conn, err = dbpool.NewMongoConn(syncer.toMongoUrl,true); err != nil {
		return errors.New(fmt.Sprintf("Connect to dest mongo failed. %v", err))
	}

	if conn != nil {
		conn.Close()
	}

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			syncer.namespaces <- ns
		}
	})
	return nil
}

func (syncer *DocumentSyncer) collectionSync(replayerId uint32, ns dbpool.NS) error {
	reader := NewDocumentReader(syncer.fromMongoUrl, ns)

	toColName := fmt.Sprintf("%s_%s", ns.Collection, utils.APPNAME)
	toNS := dbpool.NS{Database:ns.Database, Collection:toColName}

	var conn *dbpool.MongoConn
	var err error
	if conn, err = dbpool.NewMongoConn(syncer.toMongoUrl,true); err != nil {
		return errors.New(fmt.Sprintf("Connect to dest mongo failed. %v", err))
	}

	_ = conn.Session.DB(toNS.Database).C(toNS.Collection).DropCollection()

	batchExecutor := NewCollectionExecutor(replayerId, syncer.toMongoUrl, toNS)
	batchExecutor.Start()

	bufferSize := conf.Options.ReplayerDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)

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
		if len(buffer) >= bufferSize {
			batchExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
		}
	}

	LOG.Info("document replayer-%v sync index ns %v begin", replayerId, ns)

	if indexes, err := reader.GetIndexes(); err != nil {
		return errors.New(fmt.Sprintf("Get indexes from ns %v of src mongodb failed. %v", ns, err))
	} else {
		for _, index := range indexes {
			index.Background = false
			if err = conn.Session.DB(toNS.Database).C(toNS.Collection).EnsureIndex(index); err != nil {
				return errors.New(fmt.Sprintf("Create indexes for ns %v of dest mongodb failed. %v", ns, err))
			}
		}
	}

	reader.Close()
	LOG.Info("document replayer-%v sync index for ns %v successful", replayerId, ns)

	if conf.Options.ReplayerCollectionRename {
		err := conn.Session.DB("admin").
			Run(bson.D{{"renameCollection",toNS.Str()},{"to",ns.Str()}},nil)
		if err != nil {
			return errors.New(fmt.Sprintf("Rename Collection ns %v of dest mongodb to ns %v failed. %v",
							toNS, ns, err))
		}
		LOG.Info("Rename collection ns %v of dest mongodb to ns %v successful", toNS, ns)
	}

	if conn != nil {
		conn.Close()
	}

	return nil
}
