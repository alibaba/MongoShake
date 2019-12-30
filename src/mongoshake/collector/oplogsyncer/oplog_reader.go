package oplogsyncer

import (
	"errors"
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	"mongoshake/oplog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/common"
)

const (
	QueryTs    = "ts"
	QueryGid   = "g"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"

	tailTimeout   = 7
	oplogChanSize = 0

	LocalDB                    = "local"
	CollectionCapped           = "CollectionScan died due to position in capped" // bigger than 3.0
	CollectionCappedLowVersion = "UnknownError"                                  // <= 3.0 version

	FetchStatusStoreDiskNoApply int32 = 1
	FetchStatusStoreDiskApply   int32 = 2
	FetchStatusStoreMemoryApply int32 = 3

	BlockFetchDepth = 10
)

// TimeoutError. mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")
var CollectionCappedError = errors.New("collection capped error")

// used in internal channel
type retOplog struct {
	log *bson.Raw // log content
	err error     // error detail message
}

// OplogReader represents stream reader from mongodb that specified
// by an url. And with query options. user can iterate oplogs.
type OplogReader struct {
	// source mongo address url
	src     string
	replset string
	// mongo oplog reader
	conn           *utils.MongoConn
	oplogsIterator *mgo.Iter

	// status of fetch and store oplog
	fetchStatus int32
	// disk queue used to store oplog temporarily
	diskQueue *utils.DiskQueue
	// fetch timestamp of disk queue
	diskQueueFetchTs bson.MongoTimestamp

	mutex sync.Mutex

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead bool
}

// NewOplogReader creates reader with mongodb url
//
// fetchStatus: FetchStatusStoreDiskNoApply -> FetchStatusStoreDiskApply -> mutex -> FetchStatusStoreMemoryApply
//			  : FetchStatusStoreMemoryApply
//
func NewOplogReader(src, replset string, fetchStatus int32) *OplogReader {
	reader := &OplogReader{
		src:         src,
		replset:     replset,
		fetchStatus: fetchStatus,
		query:       bson.M{},
		oplogChan:   make(chan *retOplog, oplogChanSize),
		firstRead:   true,
	}
	return reader
}

func (reader *OplogReader) InitDiskQueue(dqName string) {
	if reader.fetchStatus != FetchStatusStoreMemoryApply {
		reader.diskQueue = utils.NewDiskQueue(dqName, conf.Options.LogDirectory, 1<<30, 0, 1<<26,
			1000, 2*time.Second, LOG.Global.Logf)
	}
}

// InitQueryTimestamp set internal timestamp if
// not exist in this reader. initial stage most of the time
func (reader *OplogReader) InitQueryTimestamp(ts bson.MongoTimestamp) {
	if _, exist := reader.query[QueryTs]; !exist {
		reader.UpdateQueryTimestamp(ts)
	}
}

func (reader *OplogReader) UpdateQueryTimestamp(ts bson.MongoTimestamp) {
	reader.query[QueryTs] = bson.M{QueryOpGT: ts}
}

func (reader *OplogReader) GetQueryTimestamp() bson.MongoTimestamp {
	return reader.query[QueryTs].(bson.M)[QueryOpGT].(bson.MongoTimestamp)
}

func (reader *OplogReader) UseDiskQueue() bool {
	return reader.fetchStatus != FetchStatusStoreMemoryApply
}

func (reader *OplogReader) GetDiskQueueName() string {
	return reader.diskQueue.Name()
}

func (reader *OplogReader) GetDiskQueueFetchTs() bson.MongoTimestamp {
	return reader.diskQueueFetchTs
}

func (reader *OplogReader) UpdateFetchStatus(fetchStatus int32) {
	LOG.Info("reader replset %v update fetch status to %v", reader.replset, logFetchStatus(fetchStatus))
	atomic.StoreInt32(&reader.fetchStatus, fetchStatus)
}

// internal get next oplog. The channel and current function may both return
// timeout which is acceptable.
func (reader *OplogReader) Next() (log *bson.Raw, err error) {
	select {
	case ret := <-reader.oplogChan:
		return ret.log, ret.err
	case <-time.After(time.Second * time.Duration(conf.Options.SyncerReaderBufferTime)):
		return nil, TimeoutError
	}
}

// start fetcher if not exist
func (reader *OplogReader) StartFetcher() {
	if reader.fetcherExist == true {
		return
	}

	// the oldest oplog is lost
	queryTs := reader.GetQueryTimestamp()
	if oldTs, err := utils.GetOldestTimestampByUrl(reader.src); err != nil {
		LOG.Crashf("reader fetch for replset %v connect to %v failed. %v", reader.replset, err)
	} else if oldTs > queryTs {
		LOG.Crashf("reader fetch for replset %v queryTs[%v] is less than oldTs[%v], "+
			"this error means user's oplog collection size is too small or document replication continues too long",
			reader.replset, utils.TimestampToLog(queryTs), utils.TimestampToLog(oldTs))
	}

	reader.fetcherLock.Lock()
	if reader.fetcherExist == false { // double check
		reader.fetcherExist = true
		go reader.fetch()
		if atomic.LoadInt32(&reader.fetchStatus) != FetchStatusStoreMemoryApply {
			go reader.retrieve()
		}
	}
	reader.fetcherLock.Unlock()
}

// fetch oplog tp store disk queue or memory
func (reader *OplogReader) fetch() {
	var log *bson.Raw
	checkTime := time.Now()
	for {
		if err := reader.ensureNetwork(); err != nil {
			if err == CollectionCappedError {
				LOG.Crashf("reader fetch for replset %v encounter collection oplog.rs capped. %v", reader.replset, err)
			}
			reader.oplogChan <- &retOplog{nil, err}
			continue
		}
		log = new(bson.Raw)
		if !reader.oplogsIterator.Next(log) {
			if err := reader.oplogsIterator.Err(); err != nil {
				// some internal error. need rebuild the oplogsIterator
				reader.releaseIterator()
				if reader.isCollectionCappedError(err) { // print it
					LOG.Crashf("reader fetch for replset %v encounter collection oplog.rs capped. %v", reader.replset, err)
				} else {
					reader.oplogChan <- &retOplog{nil, fmt.Errorf("get next oplog error, release oplogsIterator. %v", err)}
				}
			} else {
				// query timeout
				reader.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}

		reader.mutex.Lock()
		fetchStatus := atomic.LoadInt32(&reader.fetchStatus)
		// put oplog to the corresponding queue
		if fetchStatus == FetchStatusStoreMemoryApply {
			reader.oplogChan <- &retOplog{log, nil}
		} else if reader.diskQueue != nil {
			// store oplog to disk queue
			if err := reader.diskQueue.Put(log.Data); err != nil {
				LOG.Crashf("reader fetch for replset %v put oplog to disk queue error. %v", reader.replset, err)
			}
			if time.Now().After(checkTime.Add(5 * time.Second)) {
				// update fetch timestamp of disk queue
				partialLog := new(oplog.PartialLog)
				if err := bson.Unmarshal(log.Data, partialLog); err != nil {
					// impossible switch, need panic and exit
					LOG.Crashf("reader fetch for replset %v unmarshal oplog[%v] failed[%v]",
						reader.replset, log.Data, err)
				}
				reader.diskQueueFetchTs = partialLog.Timestamp
				// avoid disk queue size too much
				fileSizeMB := reader.diskQueue.FileSizeMB()
				if fileSizeMB > conf.Options.ReplayerOplogStoreDiskMaxSize {
					LOG.Crashf("reader fetch for replset %v disk queue[%v] reach the max size[%v]",
						reader.replset, fileSizeMB, conf.Options.ReplayerOplogStoreDiskMaxSize)
				}
				checkTime = time.Now()
			}
		} else {
			LOG.Crashf("reader fetch for replset %v has no diskqueue with fetch status[%v]. %v",
				reader.replset, logFetchStatus(fetchStatus))
		}
		reader.mutex.Unlock()
	}
}

func (reader *OplogReader) retrieve() {
	for {
		if atomic.LoadInt32(&reader.fetchStatus) == FetchStatusStoreDiskApply {
			break
		}
		time.Sleep(time.Second)
	}
	LOG.Info("reader retrieve for replset %v begin to read from disk queue with depth[%v]",
		reader.replset, reader.diskQueue.Depth())
	readExitChan := make(chan int)
	nimo.GoRoutine(func() {
		for {
			select {
			// TODO: remove disk queue file after all oplog have acked
			case data := <-reader.diskQueue.ReadChan():
				reader.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
			case <-readExitChan:
				LOG.Info("reader retrieve for replset %v end", reader.replset)
				return
			}
		}
	})
	// wait to block fetch
	for {
		if reader.diskQueue.Depth() <= BlockFetchDepth {
			break
		}
		time.Sleep(time.Second)
	}
	LOG.Info("reader retrieve for replset %v block fetch with disk queue depth[%v]",
		reader.replset, reader.diskQueue.Depth())
	// wait to finish retrieve and continue fetch to store to memory
	reader.mutex.Lock()
	for {
		if reader.diskQueue.Depth() == 0 {
			close(readExitChan)
			reader.UpdateFetchStatus(FetchStatusStoreMemoryApply)
			break
		}
		time.Sleep(time.Second)
	}
	reader.mutex.Unlock()
	time.Sleep(180*time.Second)
	if err := reader.diskQueue.Delete(); err != nil {
		LOG.Critical("reader retrieve for replset %v close disk queue error. %v", reader.replset, err)
	}
}

// ensureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (reader *OplogReader) ensureNetwork() (err error) {
	if reader.oplogsIterator != nil {
		return nil
	}
	if reader.conn == nil || (reader.conn != nil && !reader.conn.IsGood()) {
		if reader.conn != nil {
			reader.conn.Close()
		}
		// reconnect
		if reader.conn, err = utils.NewMongoConn(reader.src, conf.Options.MongoConnectMode, true); reader.conn == nil || err != nil {
			err = fmt.Errorf("reconnect mongo instance [%s] error. %s", reader.src, err)
			return err
		}
	}

	var queryTs bson.MongoTimestamp
	// the given oplog timestamp shouldn't bigger than the newest
	if reader.firstRead == true {
		// check whether the starting fetching timestamp is less than the newest timestamp exist in the oplog
		newestTs := reader.getNewestTimestamp()
		queryTs = reader.GetQueryTimestamp()
		if newestTs < queryTs {
			LOG.Warn("current starting point[%v] is bigger than the newest timestamp[%v]!",
				utils.TimestampToLog(queryTs), utils.TimestampToLog(newestTs))
			queryTs = newestTs
		}
	}

	/*
	 * the given oplog timestamp shouldn't smaller than the oldest.
	 * this may happen when collection capped.
	 */
	oldestTs := reader.getOldestTimestamp()
	queryTs = reader.GetQueryTimestamp()
	if oldestTs > queryTs {
		if !reader.firstRead {
			return CollectionCappedError
		} else {
			LOG.Warn("current starting point[%v] is smaller than the oldest timestamp[%v]!",
				utils.TimestampToLog(queryTs), utils.TimestampToLog(oldestTs))
		}
	}
	reader.firstRead = false

	// rebuild syncerGroup condition statement with current checkpoint timestamp
	reader.conn.Session.SetBatch(8192) //
	reader.conn.Session.SetPrefetch(0.2)
	reader.oplogsIterator = reader.conn.Session.DB(LocalDB).C(utils.OplogNS).
		Find(reader.query).LogReplay().Tail(time.Second * tailTimeout) // this timeout is useless
	return
}

// get newest oplog
func (reader *OplogReader) getNewestTimestamp() bson.MongoTimestamp {
	ts, _ := utils.GetNewestTimestampBySession(reader.conn.Session)
	return ts
}

// get oldest oplog
func (reader *OplogReader) getOldestTimestamp() bson.MongoTimestamp {
	ts, _ := utils.GetOldestTimestampBySession(reader.conn.Session)
	return ts
}

func (reader *OplogReader) releaseIterator() {
	if reader.oplogsIterator != nil {
		reader.oplogsIterator.Close()
	}
	reader.oplogsIterator = nil
}

func (reader *OplogReader) isCollectionCappedError(err error) bool {
	errMsg := err.Error()
	if strings.Contains(errMsg, CollectionCapped) || strings.Contains(errMsg, CollectionCappedLowVersion) {
		return true
	}
	return false
}

func logFetchStatus(status int32) string {
	switch status {
	case FetchStatusStoreDiskNoApply:
		return "store disk and no apply"
	case FetchStatusStoreDiskApply:
		return "store disk and apply"
	case FetchStatusStoreMemoryApply:
		return "store memory and apply"
	default:
		return fmt.Sprintf("invalid[%v]", status)
	}
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
