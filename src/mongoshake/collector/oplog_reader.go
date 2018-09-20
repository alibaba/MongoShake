package collector

import (
	"errors"
	"fmt"
	"time"

	"mongoshake/dbpool"
	"mongoshake/oplog"
	"mongoshake/collector/configure"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	QueryTs   = "ts"
	QueryGid  = "g"
	QueryOpGT = "$gt"

	tailTimeout   = 7
	oplogChanSize = 0
)

// TimeoutError. mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")

// used in internal channel
type retOplog struct {
	log *bson.Raw // log content
	err error     // error
}

// OplogReader represents stream reader from mongodb that specified
// by an url. And with query options. user can iterate oplogs.
type OplogReader struct {
	// source mongo address url
	src string
	// mongo oplog reader
	conn           *dbpool.MongoConn
	oplogsIterator *mgo.Iter

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan chan *retOplog
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string) *OplogReader {
	return &OplogReader{
		src: src,
		query: bson.M{},
		oplogChan: make(chan *retOplog, oplogChanSize),
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

// fetch oplog and put into channel, must be started manually
func (reader *OplogReader) StartFetcher() {
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
				reader.oplogChan <- &retOplog{nil, fmt.Errorf("get next oplog failed. release oplogsIterator, %s", err.Error())}
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
		if reader.conn, err = dbpool.NewMongoConn(reader.src, false); reader.conn == nil || err != nil {
			err = fmt.Errorf("reconnect mongo instance [%s] error. %s", reader.src, err.Error())
			return err
		}
	}

	// rebuild syncerGroup condition statement with current checkpoint timestamp
	reader.conn.Session.SetBatch(8192)
	reader.conn.Session.SetPrefetch(0.2)
	reader.oplogsIterator = reader.conn.Session.DB("local").C(dbpool.OplogNS).
		Find(reader.query).LogReplay().Tail(time.Second * tailTimeout) // this timeout is useless
	return
}

func (reader *OplogReader) releaseIterator() {
	if reader.oplogsIterator != nil {
		reader.oplogsIterator.Close()
	}
	reader.oplogsIterator = nil
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
