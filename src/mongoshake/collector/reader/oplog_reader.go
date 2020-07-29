package sourceReader

// read oplog from source mongodb

import (
	"errors"
	"fmt"
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

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead       bool
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string, replset string) *OplogReader {
	return &OplogReader{
		src:             src,
		replset:         replset,
		query:           bson.M{},
		// the mgo driver already has cache mechanism(prefetch), so there is no need to buffer here again
		oplogChan:       make(chan *retOplog, 0),
		firstRead:       true,
	}
}

func (or *OplogReader) String() string {
	return fmt.Sprintf("oplogReader[src:%s replset:%s]", or.src, or.replset)
}

func (or *OplogReader) Name() string {
	return utils.VarIncrSyncMongoFetchMethodOplog
}

// SetQueryTimestampOnEmpty set internal timestamp if
// not exist in this or. initial stage most of the time
func (or *OplogReader) SetQueryTimestampOnEmpty(ts bson.MongoTimestamp) {
	if _, exist := or.query[QueryTs]; !exist {
		LOG.Info("set query timestamp: %v", utils.ExtractTimestampForLog(ts))
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
	case <-time.After(time.Second * time.Duration(conf.Options.IncrSyncReaderBufferTime)):
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
	}
	or.fetcherLock.Unlock()
}

// fetch oplog tp store disk queue or memory
func (or *OplogReader) fetcher() {
	LOG.Info("start fetcher with src[%v] replica-name[%v] query-ts[%v]",
		utils.BlockMongoUrlPassword(or.src, "***"), or.replset,
		utils.ExtractTimestampForLog(or.query[QueryTs].(bson.M)[QueryOpGT].(bson.MongoTimestamp)))
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
					or.oplogChan <- &retOplog{nil, fmt.Errorf("get next oplog failed, release oplogsIterator, %s", err.Error())}
				}
				// wait a moment
				time.Sleep(1 * time.Second)
			} else {
				// query timeout
				or.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}

		or.oplogChan <- &retOplog{log.Data, nil}
	}
}

// ensureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (or *OplogReader) EnsureNetwork() (err error) {
	if or.oplogsIterator != nil {
		return nil
	}
	LOG.Info("%s ensure network", or.String())

	if or.conn == nil || (or.conn != nil && !or.conn.IsGood()) {
		if or.conn != nil {
			or.conn.Close()
		}
		// reconnect
		if or.conn, err = utils.NewMongoConn(or.src, conf.Options.MongoConnectMode, true,
				utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); or.conn == nil || err != nil {
			err = fmt.Errorf("oplog_reader reconnect mongo instance [%s] error. %s", or.src, err.Error())
			return err
		}

		or.conn.Session.SetBatch(BatchSize)
		or.conn.Session.SetPrefetch(PrefetchPercent)
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
