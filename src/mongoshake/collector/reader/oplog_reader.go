package sourceReader

// read oplog from source mongodb

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"sync/atomic"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/diskQueue"
)

const (
	QueryTs    = "ts"
	QueryGid   = "g"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"

	tailTimeout   = 7
	oplogChanSize = 0

	localDB = "local"
)

// TimeoutError. mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")
var CollectionCappedError = errors.New("collection capped error")

// OplogReader represents stream reader from mongodb that specified
// by an url. And with query options. user can iterate oplogs.
type OplogReader struct {
	// source mongo address url
	src     string
	replset string
	// mongo oplog reader
	conn           *utils.MongoConn
	oplogsIterator *mgo.Iter

	// stage of fetch and store oplog
	fetchStage int32
	// disk queue used to store oplog temporarily
	diskQueue     *diskQueue.DiskQueue
	disQueueMutex sync.Mutex // disk queue mutex

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead       bool
	diskQueueLastTs bson.MongoTimestamp // the last oplog timestamp in disk queue
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string, replset string) *OplogReader {
	return &OplogReader{
		src:             src,
		replset:         replset,
		query:           bson.M{},
		oplogChan:       make(chan *retOplog, oplogChanSize),
		firstRead:       true,
		diskQueueLastTs: -1,
	}
}

func (or *OplogReader) InitDiskQueue(dqName string) {
	fetchStage := or.fetchStage
	if fetchStage != utils.FetchStageStoreDiskNoApply && fetchStage != utils.FetchStageStoreDiskApply {
		LOG.Crashf("oplog_reader replset %v init disk queue in illegal fetchStage %v",
			or.replset, utils.LogFetchStage(fetchStage))
	}
	or.diskQueue = diskQueue.NewDiskQueue(dqName, conf.Options.LogDirectory,
		conf.Options.FullSyncOplogStoreDiskMaxSize, ReplayerOplogStoreDiskReadBatch,
		1 << 30, 0, 1 << 26,
		1000, 2 * time.Second)
}

func (or *OplogReader) UpdateFetchStage(fetchStage int32) {
	LOG.Info("oplog_reader replset[%v] update fetch status to: %v", or.replset, utils.LogFetchStage(fetchStage))
	atomic.StoreInt32(&or.fetchStage, fetchStage)
}

func (or *OplogReader) GetDiskQueueName() string {
	return or.diskQueue.Name()
}

func (or *OplogReader) GetQueryTsFromDiskQueue() bson.MongoTimestamp {
	if or.diskQueue == nil {
		LOG.Crashf("oplog_reader replset %v get query timestamp from nil disk queue", or.replset)
	}
	logData := or.diskQueue.GetLastWriteData()
	if len(logData) == 0 {
		return 0
	}
	log := new(oplog.PartialLog)
	if err := bson.Unmarshal(logData, log); err != nil {
		LOG.Crashf("unmarshal oplog[%v] failed[%v]", logData, err)
	}
	return log.Timestamp
}

// SetQueryTimestampOnEmpty set internal timestamp if
// not exist in this or. initial stage most of the time
func (or *OplogReader) SetQueryTimestampOnEmpty(ts bson.MongoTimestamp) {
	if _, exist := or.query[QueryTs]; !exist {
		or.UpdateQueryTimestamp(ts)
	}
}

func (or *OplogReader) UpdateQueryTimestamp(ts bson.MongoTimestamp) {
	or.query[QueryTs] = bson.M{QueryOpGT: ts}
}

func (or *OplogReader) getQueryTimestamp() bson.MongoTimestamp {
	return or.query[QueryTs].(bson.M)[QueryOpGT].(bson.MongoTimestamp)
}

// Next returns an oplog by raw bytes which is []byte
func (or *OplogReader) Next() ([]byte, error) {
	return or.get()
}

// NextOplog returns an oplog by oplog.GenericOplog struct
func (or *OplogReader) NextOplog() (log *oplog.GenericOplog, err error) {
	var raw []byte
	if raw, err = or.Next(); err != nil {
		return nil, err
	}

	log = &oplog.GenericOplog{Raw: raw, Parsed: new(oplog.PartialLog)}
	bson.Unmarshal(raw, log.Parsed)
	return log, nil
}

// internal get next oplog. Used in Next() and NextOplog(). The channel and current function may both return
// timeout which is acceptable.
func (or *OplogReader) get() (log []byte, err error) {
	select {
	case ret := <-or.oplogChan:
		return ret.log, ret.err
	case <-time.After(time.Second * time.Duration(conf.Options.SyncerReaderBufferTime)):
		return nil, TimeoutError
	}
}

// start fetcher if not exist
func (or *OplogReader) StartFetcher() {
	if or.fetcherExist == true {
		return
	}

	or.fetcherLock.Lock()
	if or.fetcherExist == false { // double check
		or.fetcherExist = true
		go or.fetcher()
		fetchStage := atomic.LoadInt32(&or.fetchStage)
		if fetchStage == utils.FetchStageStoreDiskNoApply || fetchStage == utils.FetchStageStoreDiskApply {
			go or.retrieve()
		}
	}
	or.fetcherLock.Unlock()
}

// fetch oplog tp store disk queue or memory
func (or *OplogReader) fetcher() {
	var log *bson.Raw
	for {
		if err := or.EnsureNetwork(); err != nil {
			or.oplogChan <- &retOplog{nil, err}
			continue
		}

		log = new(bson.Raw)
		if !or.oplogsIterator.Next(log) {
			if err := or.oplogsIterator.Err(); err != nil {
				// some internal error. need rebuild the oplogsIterator
				or.releaseIterator()
				if utils.IsCollectionCappedError(err) { // print it
					LOG.Error("oplog collection capped may happen: %v", err)
					or.oplogChan <- &retOplog{nil, CollectionCappedError}
				} else {
					or.oplogChan <- &retOplog{nil, fmt.Errorf("get next oplog failed. release oplogsIterator, %s", err.Error())}
				}
				// wait a moment
				time.Sleep(1 * time.Second)
			} else {
				// query timeout
				or.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}

		// fetch successfully
		fetchStage := atomic.LoadInt32(&or.fetchStage)
		// put oplog to the corresponding queue
		if fetchStage == utils.FetchStageStoreMemoryApply {
			or.oplogChan <- &retOplog{log, nil}
		} else if or.diskQueue != nil {
			or.disQueueMutex.Lock()
			if or.diskQueue != nil { // double check
				// should send to disQueue
				if err := or.diskQueue.Put(log.Data); err != nil {
					LOG.Crashf("oplog_reader fetch for replset %v put oplog to disk queue failed[%v]",
						or.replset, err)
				}
			} else {
				// should send to oplogChan
				or.oplogChan <- &retOplog{log.Data, nil}
			}

			or.disQueueMutex.Unlock()
		} else {
			LOG.Crashf("oplog_reader fetch for replset %v has no diskQueue with fetch stage[%v]. %v",
				or.replset, utils.LogFetchStage(fetchStage))
		}
	}
}

func (or *OplogReader) retrieve() {
	for {
		if atomic.LoadInt32(&or.fetchStage) == utils.FetchStageStoreDiskApply {
			break
		}
		time.Sleep(3 * time.Second)
	}

	LOG.Info("oplog_reader retrieve for replset %v begin to read from disk queue with depth[%v]",
		or.replset, or.diskQueue.Depth())
	ticker := time.NewTicker(time.Second)
Loop:
	for {
		select {
		case readData := <-or.diskQueue.ReadChan():
			if len(readData) > 0 {
				for _, data := range readData {
					or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
				}

				if err := or.diskQueue.Next(); err != nil {
					LOG.Crash(err)
				}
			}
		case <-ticker.C:
			if or.diskQueue.Depth() < or.diskQueue.BatchCount() {
				break Loop
			}
		}
	}
	LOG.Info("oplog_reader retrieve for replset %v block fetch with disk queue depth[%v]",
		or.replset, or.diskQueue.Depth())

	// wait to finish retrieve and continue fetch to store to memory
	or.disQueueMutex.Lock()
	defer or.disQueueMutex.Unlock() // lock till the end
	readData := or.diskQueue.ReadAll()
	if len(readData) > 0 {
		for _, data := range readData {
			or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
		}

		// parse the last oplog timestamp
		or.diskQueueLastTs = oplog.ParseTimstampFromBson(readData[len(readData)-1])

		if err := or.diskQueue.Next(); err != nil {
			LOG.Crash(err)
		}
	}
	if or.diskQueue.Depth() != 0 {
		LOG.Crashf("oplog_reader retrieve for replset %v finish, but disk queue depth[%v] is not empty",
			or.replset, or.diskQueue.Depth())
	}
	or.UpdateFetchStage(utils.FetchStageStoreMemoryApply)

	if err := or.diskQueue.Delete(); err != nil {
		LOG.Critical("oplog_reader retrieve for replset %v close disk queue error. %v", or.replset, err)
	}
	LOG.Info("oplog_reader retriever exits")
}

// ensureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (or *OplogReader) EnsureNetwork() (err error) {
	if or.oplogsIterator != nil {
		return nil
	}
	if or.conn == nil || (or.conn != nil && !or.conn.IsGood()) {
		if or.conn != nil {
			or.conn.Close()
		}
		// reconnect
		if or.conn, err = utils.NewMongoConn(or.src, conf.Options.MongoConnectMode, true); or.conn == nil || err != nil {
			err = fmt.Errorf("oplog_reader reconnect mongo instance [%s] error. %s", or.src, err.Error())
			return err
		}
	}

	var queryTs bson.MongoTimestamp
	// the given oplog timestamp shouldn't bigger than the newest
	if or.firstRead == true {
		// check whether the starting fetching timestamp is less than the newest timestamp exist in the oplog
		newestTs := or.getNewestTimestamp()
		queryTs = or.getQueryTimestamp()
		if newestTs < queryTs {
			LOG.Warn("oplog_reader current starting point[%v] is bigger than the newest timestamp[%v]!",
				utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(newestTs))
			queryTs = newestTs
		}
	}

	/*
	 * the given oplog timestamp shouldn't smaller than the oldest.
	 * this may happen when collection capped.
	 */
	oldestTs := or.getOldestTimestamp()
	queryTs = or.getQueryTimestamp()
	if oldestTs > queryTs {
		if !or.firstRead {
			return CollectionCappedError
		} else {
			LOG.Warn("oplog_reader current starting point[%v] is smaller than the oldest timestamp[%v]!",
				utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(oldestTs))
		}
	}
	or.firstRead = false

	// rebuild syncerGroup condition statement with current checkpoint timestamp
	or.conn.Session.SetBatch(8192) //
	or.conn.Session.SetPrefetch(0.2)
	or.oplogsIterator = or.conn.Session.DB(localDB).C(utils.OplogNS).
		Find(or.query).LogReplay().Tail(time.Second * tailTimeout) // this timeout is useless
	return
}

// get newest oplog
func (or *OplogReader) getNewestTimestamp() bson.MongoTimestamp {
	ts, _ := utils.GetNewestTimestampBySession(or.conn.Session)
	return ts
}

// get oldest oplog
func (or *OplogReader) getOldestTimestamp() bson.MongoTimestamp {
	ts, _ := utils.GetOldestTimestampBySession(or.conn.Session)
	return ts
}

func (or *OplogReader) releaseIterator() {
	if or.oplogsIterator != nil {
		or.oplogsIterator.Close()
	}
	or.oplogsIterator = nil
}

// GidOplogReader. query along with gid
type GidOplogReader struct {
	OplogReader
}

func (reader *GidOplogReader) SetQueryGid(gid string) {
	reader.query[QueryGid] = gid
}

func NewGidOplogReader(src string) *GidOplogReader {
	return &GidOplogReader{
		OplogReader: OplogReader{src: src, query: bson.M{}},
	}
}
