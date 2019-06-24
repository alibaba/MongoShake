package docsyncer

import (
	"fmt"
	"strings"

	"mongoshake/collector/filter"
	"mongoshake/common"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"mongoshake/collector/configure"
)

func GetAllNamespace(sources []*utils.MongoSource) (map[utils.NS]bool, error) {
	nsSet := make(map[utils.NS]bool)
	for _, src := range sources {
		nsList, err := getDbNamespace(src.URL)
		if err != nil {
			return nil, err
		}
		for _, ns := range nsList {
			nsSet[ns] = true
		}
	}
	return nsSet, nil
}

func getDbNamespace(url string) (nsList []utils.NS, err error) {
	var conn *utils.MongoConn
	if conn, err = utils.NewMongoConn(url, utils.ConnectModeSecondaryPreferred, true); conn == nil || err != nil {
		return nil, err
	}
	defer conn.Close()

	var dbNames []string
	if dbNames, err = conn.Session.DatabaseNames(); err != nil {
		err = fmt.Errorf("get database names of mongodb url=%s error. %v", url, err)
		return nil, err
	}

	filterList := filter.NewDocFilterList()

	nsList = make([]utils.NS, 0, 128)
	for _, db := range dbNames {
		colNames, err := conn.Session.DB(db).CollectionNames()
		if err != nil {
			err = fmt.Errorf("get collection names of mongodb url=%s error. %v", url, err)
			return nil, err
		}
		for _, col := range colNames {
			ns := utils.NS{Database:db, Collection:col}
			if strings.HasPrefix(col, "system.") {
				continue
			}
			if filterList.IterateFilter(ns.Str()) {
				LOG.Debug("Namespace is filtered. %v", ns.Str())
				continue
			}
			nsList = append(nsList, ns)
		}
	}

	return nsList, nil
}

type DocumentReader struct {
	// source mongo address url
	src string
	ns utils.NS

	// mongo document reader
	conn          	*utils.MongoConn
	docIterator 	*mgo.Iter

	// query statement and current max cursor
	query bson.M
}

// NewDocumentReader creates reader with mongodb url
func NewDocumentReader(src string, ns utils.NS) *DocumentReader {
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
		if reader.conn, err = utils.NewMongoConn(reader.src, conf.Options.MongoConnectMode, true); reader.conn == nil || err != nil {
			return err
		}
	}

	// rebuild syncerGroup condition statement with current checkpoint timestamp
	reader.conn.Session.SetBatch(8192)
	reader.conn.Session.SetPrefetch(0.2)
	reader.conn.Session.SetCursorTimeout(0)
	reader.docIterator = reader.conn.Session.DB(reader.ns.Database).C(reader.ns.Collection).
		Find(reader.query).Iter()
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