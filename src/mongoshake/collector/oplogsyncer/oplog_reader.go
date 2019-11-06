package oplogsyncer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	QueryTs    = "ts"
	QueryGid   = "g"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"

	tailTimeout   = 7
	oplogChanSize = 0

	LocalDB = "local"
)

const (
	CollectionCapped           = "CollectionScan died due to position in capped" // bigger than 3.0
	CollectionCappedLowVersion = "UnknownError"                                  // <= 3.0 version
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

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead bool
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src, replset string) *OplogReader {
	return &OplogReader{
		src:       src,
		replset:   replset,
		query:     bson.M{},
		oplogChan: make(chan *retOplog, oplogChanSize),
		firstRead: true,
	}
}

// SetQueryTimestampOnEmpty set internal timestamp if
// not exist in this reader. initial stage most of the time
func (reader *OplogReader) SetQueryTimestampOnEmpty(ts bson.MongoTimestamp) {
	if _, exist := reader.query[QueryTs]; !exist {
		reader.UpdateQueryTimestamp(ts)
	}
}

func (reader *OplogReader) UpdateQueryTimestamp(ts bson.MongoTimestamp) {
	reader.query[QueryTs] = bson.M{QueryOpGT: ts}
}

func (reader *OplogReader) getQueryTimestamp() bson.MongoTimestamp {
	return reader.query[QueryTs].(bson.M)[QueryOpGT].(bson.MongoTimestamp)
}

// Next returns an oplog by raw bytes which is []byte
func (reader *OplogReader) Next() (*bson.Raw, error) {
	return reader.get()
}

// NextOplog returns an oplog by oplog.GenericOplog struct
func (reader *OplogReader) NextOplog() (log *oplog.GenericOplog, err error) {
	var raw *bson.Raw
	if raw, err = reader.Next(); err != nil {
		return nil, err
	}

	log = &oplog.GenericOplog{Raw: raw.Data, Parsed: new(oplog.PartialLog)}
	bson.Unmarshal(raw.Data, log.Parsed)
	return log, nil
}

// internal get next oplog. Used in Next() and NextOplog(). The channel and current function may both return
// timeout which is acceptable.
func (reader *OplogReader) get() (log *bson.Raw, err error) {
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

	reader.fetcherLock.Lock()
	if reader.fetcherExist == false { // double check
		reader.fetcherExist = true
		go reader.fetcher()
	}
	reader.fetcherLock.Unlock()
}

// fetch oplog and put into channel, must be started manually
func (reader *OplogReader) fetcher() {
	var log *bson.Raw
	for {
		if err := reader.ensureNetwork(); err != nil {
			reader.oplogChan <- &retOplog{nil, err}
			continue
		}

		log = new(bson.Raw)
		if !reader.oplogsIterator.Next(log) {
			if err := reader.oplogsIterator.Err(); err != nil {
				// some internal error. need rebuild the oplogsIterator
				reader.releaseIterator()
				if reader.isCollectionCappedError(err) { // print it
					LOG.Crashf("oplog sync replset %v collection oplog.rs capped may happen: %v", reader.replset, err)
				} else {
					reader.oplogChan <- &retOplog{nil, fmt.Errorf("get next oplog failed. release oplogsIterator, %s", err.Error())}
				}
			} else {
				// query timeout
				reader.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}
		reader.oplogChan <- &retOplog{log, nil}
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
		queryTs = reader.getQueryTimestamp()
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
	queryTs = reader.getQueryTimestamp()
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
