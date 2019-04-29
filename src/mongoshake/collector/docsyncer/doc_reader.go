package docsyncer

import (
	"fmt"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
	"mongoshake/dbpool"
)


func GetAllNamespace(url string) (nsList []dbpool.NS, err error) {
	var conn *dbpool.MongoConn
	if conn, err = dbpool.NewMongoConn(url, false); conn == nil || err != nil {
		err = fmt.Errorf("connect mongodb url=%s error. %v", url, err)
		return nil, err
	}
	defer conn.Close()

	var dbNames []string
	if dbNames, err = conn.Session.DatabaseNames(); err != nil {
		err = fmt.Errorf("get database names of mongodb url=%s error. %v", url, err)
		return nil, err
	}
	nsList = make([]dbpool.NS, 0, 128)
	for _, db := range dbNames {
		if db != "admin" && db != "local" && db != utils.AppDatabase {
			colNames, err := conn.Session.DB(db).CollectionNames()
			if err != nil {
				err = fmt.Errorf("get collection names of mongodb url=%s error. %v", url, err)
				return nil, err
			}
			for _, col := range colNames {
				if col != "system.profile" {
					nsList = append(nsList, dbpool.NS{Database:db, Collection:col})
				}
			}
		}
	}

	return nsList, nil
}


type DocumentReader struct {
	// source mongo address url
	src string
	ns dbpool.NS

	// mongo document reader
	conn          	*dbpool.MongoConn
	docIterator 	*mgo.Iter

	// query statement and current max cursor
	query bson.M
}

// NewDocumentReader creates reader with mongodb url
func NewDocumentReader(src string, ns dbpool.NS) *DocumentReader {
	return &DocumentReader{src: src, ns: ns, query: bson.M{}}
}


// NextDoc returns an document by raw bytes which is []byte
func (reader *DocumentReader) NextDoc() (doc *bson.Raw, err error) {
	if err := reader.ensureNetwork(); err != nil {
		return nil, err
	}

	doc = new(bson.Raw)

	if !reader.docIterator.Next(doc) {
		if err := reader.docIterator.Err(); err != nil {
			// some internal error. need rebuild the oplogsIterator
			reader.releaseIterator()
			return nil, fmt.Errorf("get next doc failed. release oplogsIterator, %v", err)
		} else {
			return nil, nil
		}
	}
	return doc, nil
}

func (reader *DocumentReader) GetIndexes() ([]mgo.Index, error) {
	return reader.conn.Session.DB(reader.ns.Database).C(reader.ns.Collection).Indexes()
}

// ensureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (reader *DocumentReader) ensureNetwork() (err error) {
	if reader.docIterator != nil {
		return nil
	}
	if reader.conn == nil || (reader.conn != nil && !reader.conn.IsGood()) {
		if reader.conn != nil {
			reader.conn.Close()
		}
		// reconnect
		if reader.conn, err = dbpool.NewMongoConn(reader.src, false); reader.conn == nil || err != nil {
			err = fmt.Errorf("reconnect mongodb url=%s error. %v", reader.src, err)
			return err
		}
	}

	// rebuild syncerGroup condition statement with current checkpoint timestamp
	reader.conn.Session.SetBatch(8192)
	reader.conn.Session.SetPrefetch(0.2)
	reader.conn.Session.SetCursorTimeout(0)
	reader.docIterator = reader.conn.Session.DB(reader.ns.Database).C(reader.ns.Collection).
		Find(reader.query).Snapshot().Iter()
	return nil
}

func (reader *DocumentReader) releaseIterator() {
	if reader.docIterator != nil {
		_ = reader.docIterator.Close()
	}
	reader.docIterator = nil
}

func (reader *DocumentReader) Close() {
	if reader.conn != nil {
		reader.conn.Close()
		reader.conn = nil
	}
}