package docsyncer

import (
	"fmt"
	"strings"

	"mongoshake/collector/filter"
	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"math"
)

/**
 * return all namespace. return:
 *     @map[utils.NS]struct{}: namespace set where key is the namespace while value is useless, e.g., "a.b"->nil, "a.c"->nil
 *     @map[string][]string: db->collection map. e.g., "a"->[]string{"b", "c"}
 *     @error: error info
 */
func GetAllNamespace(sources []*utils.MongoSource) (map[utils.NS]struct{}, map[string][]string, error) {
	nsSet := make(map[utils.NS]struct{})
	for _, src := range sources {
		nsList, _, err := GetDbNamespace(src.URL)
		if err != nil {
			return nil, nil, err
		}
		for _, ns := range nsList {
			nsSet[ns] = struct{}{}
		}
	}

	// copy
	nsMap := make(map[string][]string, len(sources))
	for ns := range nsSet {
		if _, ok := nsMap[ns.Database]; !ok {
			nsMap[ns.Database] = make([]string, 0)
		}
		nsMap[ns.Database] = append(nsMap[ns.Database], ns.Collection)
	}

	return nsSet, nsMap, nil
}

/**
 * return db namespace. return:
 *     @[]utils.NS: namespace list, e.g., []{"a.b", "a.c"}
 *     @map[string][]string: db->collection map. e.g., "a"->[]string{"b", "c"}
 *     @error: error info
 */
func GetDbNamespace(url string) ([]utils.NS, map[string][]string, error) {
	var err error
	var conn *utils.MongoConn
	if conn, err = utils.NewMongoConn(url, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault); conn == nil || err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	var dbNames []string
	if dbNames, err = conn.Session.DatabaseNames(); err != nil {
		err = fmt.Errorf("get database names of mongodb url=%s error. %v", url, err)
		return nil, nil, err
	}

	filterList := filter.NewDocFilterList()

	nsList := make([]utils.NS, 0, 128)
	for _, db := range dbNames {
		colNames, err := conn.Session.DB(db).CollectionNames()
		if err != nil {
			err = fmt.Errorf("get collection names of mongodb url=%s error. %v", url, err)
			return nil, nil, err
		}
		for _, col := range colNames {
			ns := utils.NS{Database: db, Collection: col}
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

	// copy, convert nsList to map
	nsMap := make(map[string][]string, 0)
	for _, ns := range nsList {
		if _, ok := nsMap[ns.Database]; !ok {
			nsMap[ns.Database] = make([]string, 0)
		}
		nsMap[ns.Database] = append(nsMap[ns.Database], ns.Collection)
	}

	return nsList, nsMap, nil
}

/*************************************************/
// splitter: pre-split the collection into several pieces
type DocumentSplitter struct {
	src         string               // source mongo address url
	ns          utils.NS             // namespace
	conn        *utils.MongoConn     // connection
	readerChan  chan *DocumentReader // reader chan
	pieceSize   uint64               // each piece max size
	count       uint64               // total document number
	pieceNumber int                  // how many piece
}

func NewDocumentSplitter(src string, ns utils.NS) *DocumentSplitter {
	ds := &DocumentSplitter{
		src:       src,
		ns:        ns,
		pieceSize: conf.Options.FullSyncReaderReadDocumentCount,
	}

	// create connection
	var err error
	ds.conn, err = utils.NewMongoConn(ds.src, conf.Options.MongoConnectMode, true,
		utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault)
	if err != nil {
		LOG.Error("splitter[%s] connection mongo[%v] failed[%v]", ds, ds.src, err)
		return nil
	}

	// get total count
	count, err := ds.conn.Session.DB(ds.ns.Database).C(ds.ns.Collection).Count()
	if err != nil {
		LOG.Error("splitter[%s] connection mongo[%v] failed[%v]", ds, ds.src, err)
		return nil
	}
	ds.count = uint64(count)

	if ds.pieceSize <= 0 {
		ds.pieceNumber = 1
		ds.readerChan = make(chan *DocumentReader, 1)
	} else {
		ds.pieceNumber = int(math.Ceil(float64(ds.count) / float64(ds.pieceSize)))
		ds.readerChan = make(chan *DocumentReader, SpliterReader)
	}

	go func() {
		if err := ds.Run(); err != nil {
			LOG.Crash(err)
		}
	}()
	return ds
}

func (ds *DocumentSplitter) Close() {
	ds.conn.Close()
}

func (ds *DocumentSplitter) String() string {
	return fmt.Sprintf("DocumentSplitter src[%s] ns[%s] count[%v] pieceSize[%v] pieceNumber[%v]",
		utils.BlockMongoUrlPassword(ds.src, "***"), ds.ns, ds.count, ds.pieceSize, ds.pieceNumber)
}

// TODO, need add retry
func (ds *DocumentSplitter) Run() error {
	// close channel
	defer close(ds.readerChan)

	// disable split
	if ds.pieceNumber == 1 {
		LOG.Info("splitter[%s] disable split or no need", ds)
		ds.readerChan <- NewDocumentReader(ds.src, ds.ns, nil, nil)
		LOG.Info("splitter[%s] exits", ds)
		return nil
	}

	LOG.Info("splitter[%s] enable split: piece size[%v], count[%v]", ds, ds.pieceSize, ds.count)

	var start interface{}
	// cut into piece
	cnt := ds.count
	for i := 0; cnt > 0; i++ {
		result := make(bson.M)
		// current window size
		windowSize := ds.pieceSize
		if cnt < windowSize {
			windowSize = cnt
		}

		query := make(bson.M)
		if start != nil {
			query["_id"] = bson.M{"$gt": start}
		}

		// find the right boundary. the parameter of Skip() is int, what if overflow?
		err := ds.conn.Session.DB(ds.ns.Database).C(ds.ns.Collection).Find(query).Sort("_id").
			Skip(int(windowSize - 1)).Limit(1).One(&result)
		if err != nil {
			return fmt.Errorf("splitter[%s] piece[%d] with query[%v] and skip[%v] fetch boundary failed[%v]",
				ds, i, query, windowSize-1, err)
		}

		end := result["_id"]

		LOG.Info("splitter[%s] piece[%d] create reader with boundary(%v, %v]", ds, i, start, end)
		// inject new DocumentReader into channel
		ds.readerChan <- NewDocumentReader(ds.src, ds.ns, start, end)

		// new start
		start = end
		cnt -= windowSize
	}

	LOG.Info("splitter[%s] exits", ds)
	return nil
}

// @deprecated
func (ds *DocumentSplitter) GetIndexes() ([]mgo.Index, error) {
	return ds.conn.Session.DB(ds.ns.Database).C(ds.ns.Collection).Indexes()
}

/*************************************************/
// DocumentReader: the reader of single piece
type DocumentReader struct {
	// source mongo address url
	src string
	ns  utils.NS

	// mongo document reader
	conn        *utils.MongoConn
	docIterator *mgo.Iter

	// query statement and current max cursor
	query bson.M
}

// NewDocumentReader creates reader with mongodb url
func NewDocumentReader(src string, ns utils.NS, start, end interface{}) *DocumentReader {
	q := make(bson.M)
	if start != nil || end != nil {
		innerQ := make(bson.M)
		if start != nil {
			innerQ["$gt"] = start
		}
		if end != nil {
			innerQ["$lte"] = end
		}
		q["_id"] = innerQ
	}

	return &DocumentReader{
		src:   src,
		ns:    ns,
		query: q,
	}
}

func (reader *DocumentReader) String() string {
	return fmt.Sprintf("DocumentReader src[%v] ns[%s] query[%v]",
		utils.BlockMongoUrlPassword(reader.src, "***"), reader.ns, reader.query)
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
			return nil, err
		} else {
			return nil, nil
		}
	}
	return doc, nil
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
		if reader.conn, err = utils.NewMongoConn(reader.src, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault); reader.conn == nil || err != nil {
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
