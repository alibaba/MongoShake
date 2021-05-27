package sourceReader

import (
	"fmt"
	"sync"
	"mongoshake/common"

	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"io"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"os"
	"compress/gzip"
	"sync/atomic"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/util"
	"path/filepath"
	"time"
	"mongoshake/collector/configure"
	"mongoshake/oplog"
)

type FileReader struct {
	src          string
	fetcherExist bool
	fetcherLock  sync.Mutex
	oplogChan    chan *retOplog

	files    []string
	oplogDir string
	filesPos int

	oplogIntent *intents.Intent
	bsonSource  *db.DecodedBSONSource
}

func NewFileReader(src string, replset string, files []string, dir string) *FileReader {
	oplogDir := ""
	if dir != "" {
		oplogDir = util.ToUniversalPath(dir)
	}

	return &FileReader{
		src:          src,
		fetcherExist: false,
		oplogChan:    make(chan *retOplog, 0),
		files:        files,
		oplogDir:     oplogDir,
		filesPos:     0,
	}
}

func (fr *FileReader) String() string {
	return fmt.Sprintf("fileReader[src:%s, dir:%s]", utils.BlockMongoUrlPassword(fr.files[fr.filesPos], "***"),
		fr.oplogDir)
}

func (fr *FileReader) Name() string {
	return utils.VarIncrSyncMongoFetchMethodFile
}

// do nothing for file
func (fr *FileReader) SetQueryTimestampOnEmpty(ts interface{}) {
}

// do nothing for file
func (fr *FileReader) UpdateQueryTimestamp(ts bson.MongoTimestamp) {
}

func (fr *FileReader) Next() ([]byte, error) {
	select {
	case ret := <-fr.oplogChan:
		return ret.log, ret.err
	case <-time.After(time.Second * time.Duration(conf.Options.IncrSyncReaderBufferTime)):
		return nil, TimeoutError
	}
}

func (fr *FileReader) EnsureNetwork() (err error) {
	fr.oplogIntent = &intents.Intent{
		C:        "oplog",
		Location: fr.files[fr.filesPos],
	}

	oplogFile := filepath.Join(fr.oplogDir, fr.files[fr.filesPos])
	LOG.Info("%s EnsureNetwork open file: %v", fr, oplogFile)

	if oplogFile != "-" {
		fr.oplogIntent.BSONFile = &realBSONFile{path: oplogFile, intent: fr.oplogIntent, gzip: false}
	} else {
		log.Logvf(log.Always, "replay oplog from standard input")
		fr.oplogIntent.BSONFile = &stdinFile{Reader: os.Stdin}
	}

	if err := fr.oplogIntent.BSONFile.Open(); err != nil {
		return err
	}

	fr.bsonSource = db.NewDecodedBSONSource(db.NewBufferlessBSONSource(fr.oplogIntent.BSONFile))
	return nil
}

// do nothing for file
func (fr *FileReader) FetchNewestTimestamp() (interface{}, error) {
	return nil, nil
}

func (fr *FileReader) StartFetcher() {
	if fr.fetcherExist == true {
		return
	}

	fr.fetcherLock.Lock()
	if fr.fetcherExist == false { // double check
		fr.fetcherExist = true
		go fr.fetcher()
	}
	fr.fetcherLock.Unlock()
}

func (fr *FileReader) Close() {
	if fr.oplogIntent.BSONFile != nil {
		fr.oplogIntent.BSONFile.Close()
	}

	if fr.bsonSource != nil {
		fr.bsonSource.Close()
	}
}

func (fr *FileReader) fetcher() {
	LOG.Info("start fetcher with src[%v]", fr.files[fr.filesPos])
	var err error
	if err = fr.EnsureNetwork(); err != nil {
		LOG.Crashf("%v shutdown abnormally: %v", fr, err)
		return
	}
	fr.filesPos++

	// store the last oplog
	var prev []byte
	for {
		rawOplogEntry := fr.bsonSource.LoadNext()
		if rawOplogEntry == nil {
			if fr.filesPos >= len(fr.files) {
				// unmarshal the last oplog and parse the timestamp
				log := oplog.ParsedLog{}
				if err := bson.Unmarshal(prev, &log); err == nil {
					utils.IncrSentinelOptions.ExitPoint = int64(log.Timestamp)
					LOG.Info("fetcher meets the end file with ts: %v", utils.IncrSentinelOptions.ExitPoint)
				} else {
					LOG.Info("fetcher parse last log[%v] failed: %v", log, err)
				}
				break
			} else {
				if err = fr.EnsureNetwork(); err != nil {
					LOG.Critical("%v shutdown abnormally: %v", fr, err)
					return
				}
				fr.filesPos++
			}
		}

		fr.oplogChan <- &retOplog{rawOplogEntry, nil}
		prev = rawOplogEntry
	}

	if err := fr.bsonSource.Err(); err != nil {
		LOG.Critical("error reading oplog bson input: %v", err)
	}

	LOG.Info("fetcher exit")
}

/**************************************/
// copy filepath from mongooplogreplay

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, os.ErrInvalid
}

// PosReader is a ReadCloser which maintains the position of what has been
// read from the Reader.
type PosReader interface {
	io.ReadCloser
	Pos() int64
}

// posTrackingReader is a type for reading from a file and being able to determine
// what position the file is at.
type posTrackingReader struct {
	pos int64 // updated atomically, aligned at the beginning of the struct
	io.ReadCloser
}

func (f *posTrackingReader) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	atomic.AddInt64(&f.pos, int64(n))
	return n, err
}

func (f *posTrackingReader) Pos() int64 {
	return atomic.LoadInt64(&f.pos)
}

// mixedPosTrackingReader is a type for reading from one file but getting the position of a
// different file. This is useful for compressed files where the appropriate position for progress
// bars is that of the compressed file, but file should be read from the uncompressed file.
type mixedPosTrackingReader struct {
	readHolder PosReader
	posHolder  PosReader
}

func (f *mixedPosTrackingReader) Read(p []byte) (int, error) {
	return f.readHolder.Read(p)
}

func (f *mixedPosTrackingReader) Pos() int64 {
	return f.posHolder.Pos()
}

func (f *mixedPosTrackingReader) Close() error {
	err := f.readHolder.Close()
	if err != nil {
		return err
	}
	return f.posHolder.Close()
}

// realBSONFile implements the intents.file interface. It lets intents read from real BSON files
// ok disk via an embedded os.File
// The Read, Write and Close methods of the intents.file interface is implemented here by the
// embedded os.File, the Write will return an error and not succeed
type realBSONFile struct {
	path string
	PosReader
	// errorWrite adds a Write() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	errorWriter
	intent *intents.Intent
	gzip   bool
}

// Open is part of the intents.file interface. realBSONFiles need to be Opened before Read
// can be called on them.
func (f *realBSONFile) Open() (err error) {
	if f.path == "" {
		// this error shouldn't happen normally
		return fmt.Errorf("error reading BSON file for %v", f.intent.Namespace())
	}
	file, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("error reading BSON file %v: %v", f.path, err)
	}
	posFile := &posTrackingReader{0, file}
	if f.gzip {
		gzFile, err := gzip.NewReader(posFile)
		posUncompressedFile := &posTrackingReader{0, gzFile}
		if err != nil {
			return fmt.Errorf("error decompressing compresed BSON file %v: %v", f.path, err)
		}
		f.PosReader = &mixedPosTrackingReader{
			readHolder: posUncompressedFile,
			posHolder:  posFile}
	} else {
		f.PosReader = posFile
	}
	return nil
}

// stdinFile implements the intents.file interface. They allow intents to read single collections
// from standard input
type stdinFile struct {
	pos int64 // updated atomically, aligned at the beginning of the struct
	io.Reader
	errorWriter
}

// Open is part of the intents.file interface. stdinFile needs to have Open called on it before
// Read can be called on it.
func (f *stdinFile) Open() error {
	return nil
}

func (f *stdinFile) Read(p []byte) (int, error) {
	n, err := f.Reader.Read(p)
	atomic.AddInt64(&f.pos, int64(n))
	return n, err
}

func (f *stdinFile) Pos() int64 {
	return atomic.LoadInt64(&f.pos)
}

// Close is part of the intents.file interface. After Close is called, Read will fail.
func (f *stdinFile) Close() error {
	f.Reader = nil
	return nil
}
