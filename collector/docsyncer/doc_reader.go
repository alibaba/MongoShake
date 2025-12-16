package docsyncer

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
)

// DocumentSplitter pre-split the big collection into several pieces
type DocumentSplitter struct {
	src           string   // source mongo address url
	sslRootCaFile string   // source root ca ssl
	ns            utils.NS // namespace
	client        *utils.MongoCommunityConn
	readerChan    chan *DocumentReader // reader chan
	pieceByteSize uint64               // each piece max byte size
	count         uint64               // total document number
	pieceNumber   int                  // how many piece
}

// SplitVectorResult is the result of splitVector cmd, contains multi split points.
type SplitVectorResult struct {
	SplitKeys []bson.Raw `bson:"splitKeys"`
}

func NewDocumentSplitter(src, sslRootCaFile string, ns utils.NS) *DocumentSplitter {
	ds := &DocumentSplitter{
		src:           src,
		sslRootCaFile: sslRootCaFile,
		ns:            ns,
	}

	// create connection
	var err error
	// disable timeout
	ds.client, err = utils.NewMongoCommunityConn(ds.src, conf.Options.MongoConnectMode, false,
		utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, sslRootCaFile)
	if err != nil {
		LOG.Error("splitter[%s] connection mongo[%v] failed[%v]", ds,
			utils.BlockMongoUrlPassword(ds.src, "***"), err)
		return nil
	}

	// get total count
	var res struct {
		Count       int64   `bson:"count"`
		Size        float64 `bson:"size"`
		StorageSize float64 `bson:"storageSize"`
	}
	if err := ds.client.Client.Database(ds.ns.Database).RunCommand(nil,
		bson.D{{"collStats", ds.ns.Collection}}).Decode(&res); err != nil {

		LOG.Error("splitter[%s] connection mongo[%v] failed[%v]", ds,
			utils.BlockMongoUrlPassword(ds.src, "***"), err)
		return nil
	}
	ds.count = uint64(res.Count)
	// max pieceByteSize should be limited by maxChunkSize of splitVector cmd
	ds.pieceByteSize = uint64(res.Size / float64(conf.Options.FullSyncReaderParallelThread))
	if ds.pieceByteSize > 1024*utils.MB {
		// at most 8GB per chunk
		ds.pieceByteSize = 1024 * utils.MB
	}

	LOG.Info("NewDocumentSplitter db[%v] col[%v] res[%v], pieceByteSize[%v]",
		ds.ns.Database, ds.ns.Collection, res, ds.pieceByteSize)

	if conf.Options.FullSyncReaderParallelThread <= 1 {
		ds.readerChan = make(chan *DocumentReader, 1)
	} else {
		ds.readerChan = make(chan *DocumentReader, 4196)
	}

	go func() {
		if err := ds.Run(); err != nil {
			LOG.Crash(err)
		}
	}()
	return ds
}

func (ds *DocumentSplitter) Close() {
	ds.client.Close()
}

func (ds *DocumentSplitter) String() string {
	return fmt.Sprintf("DocumentSplitter src[%s] ns[%s] count[%v] pieceByteSize[%v MB] pieceNumber[%v]",
		utils.BlockMongoUrlPassword(ds.src, "***"), ds.ns, ds.count, ds.pieceByteSize/utils.MB, ds.pieceNumber)
}

func (ds *DocumentSplitter) Run() error {
	// close channel
	defer close(ds.readerChan)

	// disable split
	if conf.Options.FullSyncReaderParallelThread <= 1 {
		LOG.Info("splitter[%s] disable split or no need", ds)
		ds.readerChan <- NewDocumentReader(0, ds.src, ds.ns, "", nil, nil, ds.sslRootCaFile)
		LOG.Info("splitter[%s] exits", ds)
		return nil
	}

	maxChunkSize := conf.Options.FullSyncReaderSplitMaxChunkSize // default 1024MB, could be set by user
	if uint64(maxChunkSize*utils.MB) > ds.pieceByteSize {
		maxChunkSize = int(ds.pieceByteSize / uint64(utils.MB))
	}
	splitVectorCmd := bson.D{
		{"splitVector", ds.ns.Str()},
		{"keyPattern", bson.M{conf.Options.FullSyncReaderParallelIndex: 1}}, // {_id:1} or {shardKey:1}
		// {"maxSplitPoints", ds.pieceNumber - 1},
		{"maxChunkSize", maxChunkSize},
	}
	LOG.Info("splitter[%s] splitVector cmd: %v, waiting splitVector return...", ds, splitVectorCmd)

	res := SplitVectorResult{}
	err := ds.client.Client.Database(ds.ns.Database).RunCommand(context.Background(), splitVectorCmd).Decode(&res)
	// if failed, do not panic, run single thread fetching
	if err != nil {
		LOG.Warn("splitter[%s] run splitVector failed[%v], give up parallel fetching", ds, err)
		ds.readerChan <- NewDocumentReader(0, ds.src, ds.ns, "", nil, nil, ds.sslRootCaFile)
		LOG.Info("splitter[%s] exits", ds)
		return nil
	}
	// only print if len(splitKeys) < 100
	if len(res.SplitKeys) > 100 {
		LOG.Info("splitter[%s] run splitVector result len(splitKeys): %v", ds, len(res.SplitKeys))
	} else {
		LOG.Info("splitter[%s] run splitVector result splitKeys: %v", ds, res)
	}

	if len(res.SplitKeys) > 0 {
		// return list is sorted
		ds.pieceNumber = len(res.SplitKeys) + 1

		var start interface{}
		cnt := 0
		for i, keyDoc := range res.SplitKeys {
			// check key == conf.Options.FullSyncReaderParallelIndex
			doc := bson.M{}
			err := bson.Unmarshal(keyDoc, &doc)
			if err != nil {
				LOG.Crash("splitter[%s] unmarshal doc [%v] failed: %v", ds, keyDoc, err)
			}
			key, val, err := parseDocKeyValue(doc)
			if err != nil {
				LOG.Crash("splitter[%s] parse doc key failed: %v", ds, err)
			}
			if key != conf.Options.FullSyncReaderParallelIndex {
				LOG.Crash("splitter[%s] parse doc invalid key: %v", ds, key)
			}

			LOG.Info("splitter[%s] piece[%d] create reader with boundary(%v, %v]", ds, cnt, start, val)
			// inject new DocumentReader into channel
			ds.readerChan <- NewDocumentReader(cnt, ds.src, ds.ns, key, start, val, ds.sslRootCaFile)

			// new start
			start = val
			cnt++

			// last one
			if i == len(res.SplitKeys)-1 {
				LOG.Info("splitter[%s] piece[%d] create reader with boundary(%v, INF)", ds, cnt, start)
				// inject new DocumentReader into channel
				ds.readerChan <- NewDocumentReader(cnt, ds.src, ds.ns, key, start, nil, ds.sslRootCaFile)
			}
		}

		return nil
	} else {
		LOG.Warn("splitter[%s] run splitVector return null result[%v]", ds, res)
	}

	LOG.Warn("splitter[%s] give up parallel fetching", ds, err)
	ds.readerChan <- NewDocumentReader(0, ds.src, ds.ns, "", nil, nil, ds.sslRootCaFile)
	LOG.Info("splitter[%s] exits", ds)

	LOG.Info("splitter[%s] exits", ds)
	return nil
}

func parseDocKeyValue(x interface{}) (string, interface{}, error) {
	keyDocM := x.(bson.M)
	if len(keyDocM) > 1 {
		return "", nil, fmt.Errorf("invalid key doc[%v]", keyDocM)
	}
	var key string
	var val interface{}
	for key, val = range keyDocM {
	}
	return key, val, nil
}

// DocumentReader is the reader for single piece
type DocumentReader struct {
	// source mongo address url
	src           string
	ns            utils.NS
	sslRootCaFile string // source root ca ssl

	// mongo document reader
	client      *utils.MongoCommunityConn
	docCursor   *mongo.Cursor
	ctx         context.Context
	rebuild     int   // rebuild times
	concurrency int32 // for test only

	// query statement and current max cursor
	query bson.M
	key   string
	id    int
}

// NewDocumentReader creates reader with mongodb url
func NewDocumentReader(id int, src string, ns utils.NS, key string, start, end interface{},
	sslRootCaFile string) *DocumentReader {
	q := make(bson.M)
	if start != nil || end != nil {
		innerQ := make(bson.M)
		if start != nil {
			innerQ["$gt"] = start
		}
		if end != nil {
			innerQ["$lte"] = end
		}
		q[key] = innerQ
	}

	ctx := context.Background()

	return &DocumentReader{
		id:            id,
		src:           src,
		ns:            ns,
		sslRootCaFile: sslRootCaFile,
		query:         q,
		key:           key,
		ctx:           ctx,
	}
}

func (reader *DocumentReader) String() string {
	ret := fmt.Sprintf("DocumentReader id[%v], src[%v] ns[%s] query[%v]", reader.id,
		utils.BlockMongoUrlPassword(reader.src, "***"), reader.ns, reader.query)
	if reader.docCursor != nil {
		ret = fmt.Sprintf("%s docCursorId[%v]", ret, reader.docCursor.ID())
	}
	return ret
}

// NextDoc returns a document by raw bytes which is []byte
// reader.docCursor.Current is valid only before next docCursor.Next(), So must be copy
func (reader *DocumentReader) NextDoc() (doc bson.Raw, err error) {
	if err := reader.ensureNetwork(); err != nil {
		return nil, err
	}

	atomic.AddInt32(&reader.concurrency, 1)
	defer atomic.AddInt32(&reader.concurrency, -1)

	if !reader.docCursor.Next(reader.ctx) {
		if err := reader.docCursor.Err(); err != nil {
			reader.releaseCursor()
			return nil, err
		} else {
			LOG.Info("reader[%s] finish", reader.String())
			return nil, nil
		}
	}

	return reader.docCursor.Current, err
}

// ensureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (reader *DocumentReader) ensureNetwork() (err error) {
	if reader.docCursor != nil {
		return nil
	}

	if reader.client == nil {
		LOG.Info("reader[%s] client is empty, create one", reader.String())
		reader.client, err = utils.NewMongoCommunityConn(reader.src, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			return err
		}
	}

	reader.rebuild += 1
	if reader.rebuild > 1 {
		return fmt.Errorf("reader[%s] rebuild illegal", reader.String())
	}

	findOptions := new(options.FindOptions)
	findOptions.SetSort(map[string]interface{}{
		"_id": 1,
	})
	findOptions.SetBatchSize(int32(conf.Options.FullSyncReaderFetchBatchSize)) // set big for test
	findOptions.SetHint(map[string]interface{}{
		"_id": 1,
	})

	// enable noCursorTimeout anyway! #451 #784
	if reader.client.IsTimeSeriesCollection(reader.ns.Database, reader.ns.Collection) == false {
		findOptions.SetNoCursorTimeout(true)
	}
	findOptions.SetComment(fmt.Sprintf("mongo-shake full sync: ns[%v] query[%v] rebuid-times[%v]",
		reader.ns, reader.query, reader.rebuild))

	reader.docCursor, err = reader.client.Client.Database(reader.ns.Database).Collection(reader.ns.Collection, nil).
		Find(nil, reader.query, findOptions)
	if err != nil {
		return fmt.Errorf("run find failed: %v", err)
	}

	LOG.Info("reader[%s] generates new cursor", reader.String())

	return nil
}

func (reader *DocumentReader) releaseCursor() {
	if reader.docCursor != nil {
		LOG.Info("reader[%s] closes cursor[%v]", reader, reader.docCursor.ID())
		err := reader.docCursor.Close(reader.ctx)
		if err != nil {
			LOG.Error("release cursor fail: %v", err)
		}
	}
	reader.docCursor = nil
}

func (reader *DocumentReader) Close() {
	LOG.Info("reader[%s] close", reader)
	if reader.docCursor != nil {
		reader.docCursor.Close(reader.ctx)
	}

	if reader.client != nil {
		reader.client.Client.Disconnect(reader.ctx)
		reader.client = nil
	}
}
