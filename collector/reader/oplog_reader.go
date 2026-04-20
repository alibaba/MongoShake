package sourceReader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
)

const (
	QueryTs    = "ts"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"

	localDB = "local"
)

const (
	// MaxCappedRetry is the maximum number of automatic cursor reset retries
	// when CappedPositionLost error occurs before treating it as fatal.
	MaxCappedRetry = utils.VarOplogReaderMaxCappedRetry
)

// TimeoutError defines mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")
var CollectionCappedError = errors.New("collection capped error")
var CollectionCappedFatalError = errors.New("collection capped error persists after max retries, fatal")

// OplogReader represents stream reader from mongodb that specified
// by an url. And with query options. user can iterate oplogs.
type OplogReader struct {
	// source mongo address url
	src     string
	replset string

	// mongo oplog reader
	conn         *utils.MongoCommunityConn
	oplogsCursor *mongo.Cursor

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead bool

	// cappedErrorCount tracks consecutive CappedPositionLost errors
	cappedErrorCount int
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string, replset string) *OplogReader {
	return &OplogReader{
		src:       src,
		replset:   replset,
		query:     bson.M{},
		oplogChan: make(chan *retOplog, 10*conf.Options.IncrSyncReaderFetchBatchSize), // ten times of batchSize
		firstRead: true,
	}
}

func (or *OplogReader) String() string {
	return fmt.Sprintf("oplogReader[src:%s replset:%s]", utils.BlockMongoUrlPassword(or.src, "***"),
		or.replset)
}

func (or *OplogReader) Name() string {
	return utils.VarIncrSyncMongoFetchMethodOplog
}

// SetQueryTimestampOnEmpty set internal timestamp if
// not exist in this or. initial stage most of the time
func (or *OplogReader) SetQueryTimestampOnEmpty(ts interface{}) {
	tsB := ts.(int64)
	if _, exist := or.query[QueryTs]; !exist {
		LOG.Info("set query timestamp: %v", utils.ExtractTimestampForLog(tsB))
		or.UpdateQueryTimestamp(tsB)
	}
}

func (or *OplogReader) UpdateQueryTimestamp(ts int64) {
	or.query[QueryTs] = bson.M{QueryOpGT: utils.Int64ToTimestamp(ts)}
	LOG.Info("update or.query to %v", or.query)
}

func (or *OplogReader) getQueryTimestamp() int64 {
	return utils.TimeStampToInt64(or.query[QueryTs].(bson.M)[QueryOpGT].(primitive.Timestamp))
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
	if err = bson.Unmarshal(raw, log.Parsed); err != nil {
		return nil, err
	}
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

// StartFetcher start fetcher if not exist
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
	LOG.Info("start %s fetcher with src[%v] replica-name[%v] query-ts[%v]",
		or.String(), utils.BlockMongoUrlPassword(or.src, "***"), or.replset,
		or.query[QueryTs].(bson.M)[QueryOpGT].(primitive.Timestamp))
	for {
		if err := or.EnsureNetwork(); err != nil {
			// EnsureNetwork returns CollectionCappedError when queryTs falls behind oldest oplog.
			// Count consecutive capped errors and escalate to fatal after MaxCappedRetry.
			if errors.Is(err, CollectionCappedError) {
				or.cappedErrorCount++
				_ = LOG.Error("oplog collection capped error from EnsureNetwork [%d/%d]",
					or.cappedErrorCount, MaxCappedRetry)
				if or.cappedErrorCount > MaxCappedRetry {
					or.oplogChan <- &retOplog{nil, CollectionCappedFatalError}
				} else {
					or.oplogChan <- &retOplog{nil, err}
				}
			} else {
				or.oplogChan <- &retOplog{nil, err}
			}
			time.Sleep(1 * time.Second)
			continue
		}

		if !or.oplogsCursor.TryNext(context.Background()) {
			if err := or.oplogsCursor.Err(); err != nil {
				// some internal error. need rebuild the oplogsCursor
				or.releaseCursor()
				if utils.IsCollectionCappedError(err) {
					or.cappedErrorCount++
					_ = LOG.Error("oplog collection capped may happen [%d/%d]: %v",
						or.cappedErrorCount, MaxCappedRetry, err)
					if or.cappedErrorCount > MaxCappedRetry {
						_ = LOG.Error("oplog collection capped error persists after %d retries, treating as fatal",
							MaxCappedRetry)
						or.oplogChan <- &retOplog{nil, CollectionCappedFatalError}
					} else {
						or.oplogChan <- &retOplog{nil, CollectionCappedError}
					}
				} else {
					e := fmt.Errorf("get next oplog failed, release oplogsIterator, %s", err.Error())
					or.oplogChan <- &retOplog{nil, e}
				}
				// wait a moment
				time.Sleep(1 * time.Second)
			} else {
				// query timeout
				or.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}

		// successfully read data, reset capped error counter
		or.cappedErrorCount = 0
		or.oplogChan <- &retOplog{or.oplogsCursor.Current, nil}
	}
}

// EnsureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (or *OplogReader) EnsureNetwork() (err error) {
	if or.oplogsCursor != nil {
		return nil
	}
	LOG.Info("%s ensure network", or.String())

	if or.conn == nil || (or.conn != nil && !or.conn.IsGood()) {
		if or.conn != nil {
			or.conn.Close()
		}
		// reconnect
		if or.conn, err = utils.NewMongoCommunityConn(or.src, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault,
			conf.Options.MongoSslRootCaFile); or.conn == nil || err != nil {
			err = fmt.Errorf("oplog_reader reconnect mongo instance [%s] error. %s", or.src, err.Error())
			return err
		}
	}

	findOptions := options.Find().SetBatchSize(int32(conf.Options.IncrSyncReaderFetchBatchSize)).
		SetNoCursorTimeout(true).
		SetCursorType(options.TailableAwait).
		SetMaxAwaitTime(time.Millisecond * 1000).
		SetOplogReplay(true)

	var queryTs int64
	// the given oplog timestamp shouldn't bigger than the newest
	if or.firstRead == true {
		// check whether the starting fetching timestamp is less than the newest timestamp exist in the oplog
		newestTs := or.getNewestTimestamp()
		queryTs = or.getQueryTimestamp()
		if newestTs < queryTs {
			_ = LOG.Warn("oplog_reader current starting point[%v] is bigger than the newest timestamp[%v]!",
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
			_ = LOG.Error("oplog_reader queryTs[%v] is behind oldest oplog[%v], oplog may have been overwritten (CappedPositionLost)",
				utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(oldestTs))
			return CollectionCappedError
		} else {
			_ = LOG.Warn("oplog_reader current starting point[%v] is smaller than the oldest timestamp[%v]!",
				utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(oldestTs))
		}
	}
	or.firstRead = false

	or.oplogsCursor, err = or.conn.Client.Database(localDB).Collection(utils.OplogNS).Find(context.Background(),
		or.query, findOptions)
	if or.oplogsCursor == nil || err != nil {
		err = fmt.Errorf("oplog_reader Find mongo instance [%s] error. %s", or.src, err.Error())
		_ = LOG.Warn("oplog_reader failed err[%v] or.query[%v]", err, or.query)
		return err
	}

	LOG.Info("%s generates new cursor query[%v]", or.String(), or.query)

	return
}

// get newest oplog
func (or *OplogReader) getNewestTimestamp() int64 {
	ts, _ := utils.GetNewestTimestampByConn(or.conn)
	return ts
}

// get oldest oplog
func (or *OplogReader) getOldestTimestamp() int64 {
	ts, _ := utils.GetOldestTimestampByConn(or.conn)
	return ts
}

func (or *OplogReader) releaseCursor() {
	if or.oplogsCursor != nil {
		_ = or.oplogsCursor.Close(context.Background())
	}
	or.oplogsCursor = nil
}

func (or *OplogReader) FetchNewestTimestamp() (interface{}, error) {
	return nil, fmt.Errorf("interface not implement")
}
