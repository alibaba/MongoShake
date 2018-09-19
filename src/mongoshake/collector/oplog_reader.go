package collector

import (
	"errors"
	"fmt"
	"time"

	"mongoshake/dbpool"
	"mongoshake/oplog"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
)

const (
	QueryTs   = "ts"
	QueryGid  = "g"
	QueryOpGT = "$gt"
)

// TimeoutError. mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")

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
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string) *OplogReader {
	return &OplogReader{src: src, query: bson.M{}}
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
	if err := reader.ensureNetwork(); err != nil {
		return nil, err
	}
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

// internal get next oplog. used in Next() and NextOplog()
func (reader *OplogReader) get() (log *bson.Raw, err error) {
	if reader.oplogsIterator == nil {
		return nil, errors.New("internal syncer oplogs iterator is not valid")
	}
	log = new(bson.Raw)

	if !reader.oplogsIterator.Next(log) {
		if err := reader.oplogsIterator.Err(); err != nil {
			// some internal error. need rebuild the oplogsIterator
			reader.releaseIterator()
			return nil, fmt.Errorf("get next oplog failed. release oplogsIterator, %s", err.Error())
		} else {
			// query timeout
			return nil, TimeoutError
		}
	}
	return log, nil
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
		Find(reader.query).LogReplay().Tail(time.Second * time.Duration(conf.Options.SyncerBufferTime))
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
