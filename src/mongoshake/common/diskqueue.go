package utils

// copied from github.com/nsqio/go-diskqueue, need change log module

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	LOG "github.com/vinllen/log4go"
)

// diskQueue implements a filesystem backed FIFO queue
type DiskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	sync.RWMutex

	// instantiation time metadata
	name       string
	dataPath   string
	maxBytesMB int64
	batchCount int64

	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64         // number of writes per fsync
	syncTimeout     time.Duration // duration of time per fsync
	exitFlag        int32
	needSync        bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64
	nextReadCount   int64

	readFile      *os.File
	writeFile     *os.File
	reader        *bufio.Reader
	writeBuf      bytes.Buffer
	lastWriteData []byte

	// exposed via ReadChan()
	readChan chan [][]byte

	// internal channels
	writeChan           chan []byte
	writeResponseChan   chan error
	nextChan            chan int
	nextResponseChan    chan error
	readAllChan         chan int
	readAllResponseChan chan [][]byte
	emptyChan           chan int
	emptyResponseChan   chan error
	exitChan            chan int
	exitSyncChan        chan int
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(name string, dataPath string, maxBytesMB int64, batchCount int64,
	maxBytesPerFile int64, minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration) *DiskQueue {
	d := DiskQueue{
		name:                name,
		dataPath:            dataPath,
		maxBytesMB:          maxBytesMB,
		batchCount:          batchCount,
		maxBytesPerFile:     maxBytesPerFile,
		minMsgSize:          minMsgSize,
		maxMsgSize:          maxMsgSize,
		readChan:            make(chan [][]byte),
		writeChan:           make(chan []byte),
		writeResponseChan:   make(chan error),
		nextChan:            make(chan int),
		nextResponseChan:    make(chan error),
		readAllChan:         make(chan int),
		readAllResponseChan: make(chan [][]byte),
		emptyChan:           make(chan int),
		emptyResponseChan:   make(chan error),
		exitChan:            make(chan int),
		exitSyncChan:        make(chan int),
		syncEvery:           syncEvery,
		syncTimeout:         syncTimeout,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		LOG.Crashf("DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	return &d
}

func (d *DiskQueue) Name() string {
	return d.name
}

// Depth returns the depth of the queue
func (d *DiskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
func (d *DiskQueue) ReadChan() chan [][]byte {
	return d.readChan
}

func (d *DiskQueue) BatchCount() int64 {
	return d.batchCount
}

func (d *DiskQueue) GetLastWriteData() []byte {
	return d.lastWriteData
}

// Put writes a []byte to the queue
func (d *DiskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *DiskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *DiskQueue) Delete() error {
	if err := d.deleteAllFiles(); err != nil {
		LOG.Error("DISKQUEUE(%s) failed to remove files - %s", d.name, err)
	}
	return d.exit(true)
}

func (d *DiskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		LOG.Info("DISKQUEUE(%s): deleting", d.name)
	} else {
		LOG.Info("DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *DiskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	LOG.Info("DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *DiskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		LOG.Error("DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *DiskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			LOG.Error("DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.lastWriteData = nil
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.nextReadCount = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readBatch performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueue) readBatch(all bool) ([][]byte, error) {
	var dataRead [][]byte
	var count int64
	nextReadPos := d.readPos
	nextReadFileNum := d.readFileNum

	for count = 0; (nextReadFileNum < d.writeFileNum) || (nextReadPos < d.writePos); count++ {

		if !all && count >= d.batchCount {
			break
		}

		var err error
		var msgSize int32

		if d.readFile == nil {
			curFileName := d.fileName(d.readFileNum)
			d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
			if err != nil {
				return nil, err
			}

			LOG.Info("DISKQUEUE(%s): readBatch() opened %s", d.name, curFileName)

			if d.readPos > 0 {
				_, err = d.readFile.Seek(d.readPos, 0)
				if err != nil {
					d.readFile.Close()
					d.readFile = nil
					return nil, err
				}
			}

			d.reader = bufio.NewReader(d.readFile)
		}

		err = binary.Read(d.reader, binary.BigEndian, &msgSize)
		if err != nil {
			d.readFile.Close()
			d.readFile = nil
			return nil, err
		}

		if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
			// this file is corrupt and we have no reasonable guarantee on
			// where a new message should begin
			d.readFile.Close()
			d.readFile = nil
			return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
		}

		readBuf := make([]byte, msgSize)
		_, err = io.ReadFull(d.reader, readBuf)
		if err != nil {
			d.readFile.Close()
			d.readFile = nil
			return nil, err
		}

		dataRead = append(dataRead, readBuf)

		totalBytes := int64(4 + msgSize)

		nextReadPos += totalBytes
		if nextReadPos > d.maxBytesPerFile {
			if d.readFile != nil {
				d.readFile.Close()
				d.readFile = nil
			}

			nextReadFileNum++
			nextReadPos = 0
		}
	}

	d.nextReadFileNum = nextReadFileNum
	d.nextReadPos = nextReadPos
	d.nextReadCount = count

	return dataRead, nil
}

func (d *DiskQueue) Next() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.nextChan <- 1
	return <-d.nextResponseChan
}

func (d *DiskQueue) ReadAll() [][]byte {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return nil
	}
	d.readAllChan <- 1
	return <-d.readAllResponseChan
}

func (d *DiskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -d.nextReadCount)

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		for i := oldReadFileNum; i < d.nextReadFileNum; i++ {
			fn := d.fileName(oldReadFileNum)
			err := os.Remove(fn)
			if err != nil {
				LOG.Error("DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
			}
		}
	}

	d.checkTailCorruption(depth)
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *DiskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		LOG.Info("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	d.lastWriteData = data
	atomic.AddInt64(&d.depth, 1)

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			LOG.Crashf("DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *DiskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos
	d.nextReadCount = 0

	var msgSize int32
	reader := bufio.NewReader(f)
	err = binary.Read(reader, binary.BigEndian, &msgSize)
	if err != nil {
		return err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		return fmt.Errorf("DISKQUEUE(%s) retrieveMetaData invalid message read size (%d)",
			d.name, msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(reader, readBuf)
	if err != nil {
		return err
	}
	d.lastWriteData = readBuf

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	dataLen := int32(len(d.lastWriteData))
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		f.Close()
		return err
	}
	d.writeBuf.Write(d.lastWriteData)
	_, err = f.Write(d.writeBuf.Bytes())
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.meta.dat"), d.name)
}

func (d *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.%06d.dat"), d.name, fileNum)
}

func (d *DiskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			LOG.Crashf("DISKQUEUE(%s) negative depth at tail (%d), metadata corruption",
				d.name, depth)
		} else if depth > 0 {
			LOG.Crashf("DISKQUEUE(%s) positive depth at tail (%d), data loss",
				d.name, depth)
		}
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			LOG.Crashf("DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption",
				d.name, d.readFileNum, d.writeFileNum)
		}
		if d.readPos > d.writePos {
			LOG.Crashf(
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}
	}
}

func (d *DiskQueue) fileSizeMB() int64 {
	var fileSizeMB int64
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		if fileInfo, err := os.Stat(fn); err == nil {
			fileSizeMB += fileInfo.Size() / 1024 / 1024
		}
	}
	return fileSizeMB
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *DiskQueue) ioLoop() {
	var dataRead [][]byte
	var err error
	var count int64
	var r chan [][]byte

	syncTicker := time.NewTicker(d.syncTimeout)
	checkTicker := time.NewTicker(time.Second)

	waitMoveForward := false
	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				LOG.Crashf("DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if d.depth >= d.batchCount && !waitMoveForward {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readBatch(false)
				if err != nil {
					LOG.Crashf("DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			count += int64(len(dataRead))
			waitMoveForward = true
		case <-d.readAllChan:
			dataRead, err = d.readBatch(true)
			if err != nil {
				LOG.Crashf("DISKQUEUE(%s) reading at %d of %s - %s",
					d.name, d.readPos, d.fileName(d.readFileNum), err)
			}
			waitMoveForward = true
			d.readAllResponseChan <- dataRead
		case <-d.nextChan:
			waitMoveForward = false
			d.moveForward()
			d.nextResponseChan <- nil
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		case <-checkTicker.C:
			fileSizeMB := d.fileSizeMB()
			if fileSizeMB > d.maxBytesMB {
				LOG.Crashf("DISKQUEUE(%s) total size %vMB reach the max size %vMB",
					d.name, fileSizeMB, d.maxBytesMB)
			}
		}
	}

exit:
	LOG.Info("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
