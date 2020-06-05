package tunnel

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"
	"time"

	LOG "github.com/vinllen/log4go"
)

const (
	OPEN_FILE_FLAGS = os.O_CREATE | os.O_RDWR | os.O_TRUNC
)

const (
	FILE_MAGIC_NUMBER    uint64 = 0xeeeeeeeeee201314
	FILE_PROTOCOL_NUMBER uint32 = 1
	BLOCK_HEADER_SIZE           = 20
)

var globalInitializer = int32(0)
var oplogMessage chan *TMessage

type FileWriter struct {
	// local file folder path
	Local string

	// data file header
	fileHeader *FileHeader
	// data file handle
	dataFile *DataFile

	logs uint64
}

/**
 *  File Structure
 *
 *  |----- Header ------|------ OplogBlock ------|------ OplogBlock --------| ......
 *  |<--- 32bytes ---->|
 *
 */
type FileHeader struct {
	Magic    uint64
	Protocol uint32
	Checksum uint32
	Reserved [16]byte
}

type DataFile struct {
	filehandle *os.File
}

func (dataFile *DataFile) WriteHeader() {
	fileHeader := new(FileHeader)
	fileHeader.Magic = FILE_MAGIC_NUMBER
	fileHeader.Protocol = FILE_PROTOCOL_NUMBER

	buffer := bytes.Buffer{}
	binary.Write(&buffer, binary.BigEndian, fileHeader.Magic)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Protocol)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Checksum)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Reserved)

	dataFile.filehandle.Write(buffer.Bytes())
	dataFile.filehandle.Sync()
	dataFile.filehandle.Seek(32, 0)
}

func (dataFile *DataFile) ReadHeader() *FileHeader {
	fileHeader := &FileHeader{}
	header := [32]byte{}

	io.ReadFull(dataFile.filehandle, header[:])
	buffer := bytes.NewBuffer(header[:])

	binary.Read(buffer, binary.BigEndian, &fileHeader.Magic)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Protocol)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Checksum)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Reserved)

	return fileHeader
}

func (tunnel *FileWriter) Name() string {
	return "file"
}

func (tunnel *FileWriter) Send(message *WMessage) int64 {
	if message.Tag&MsgProbe == 0 {
		oplogMessage <- message.TMessage
	}
	return 0
}

func (tunnel *FileWriter) SyncToDisk() {
	buffer := &bytes.Buffer{}

	for {
		select {
		case message := <-oplogMessage:
			// oplogs array
			for _, log := range message.RawLogs {
				tunnel.logs++
				binary.Write(buffer, binary.BigEndian, uint32(len(log)))
				binary.Write(buffer, binary.BigEndian, log)
			}

			tag := message.Tag | MsgPersistent | MsgStorageBackend

			headerBuffer := &bytes.Buffer{}
			binary.Write(headerBuffer, binary.BigEndian, message.Checksum)
			binary.Write(headerBuffer, binary.BigEndian, tag)
			binary.Write(headerBuffer, binary.BigEndian, message.Shard)
			binary.Write(headerBuffer, binary.BigEndian, message.Compress)
			binary.Write(headerBuffer, binary.BigEndian, uint32(0xeeeeeeee))
			binary.Write(headerBuffer, binary.BigEndian, uint32(buffer.Len()))
			tunnel.dataFile.filehandle.Write(headerBuffer.Bytes())
			tunnel.dataFile.filehandle.Write(buffer.Bytes())
			buffer.Reset()
		case <-time.After(time.Millisecond * 1000):
			LOG.Info("File tunnel sync flush. total oplogs %d", tunnel.logs)
			tunnel.dataFile.filehandle.Sync()
		}
	}
}

func _Open(path string) (*os.File, bool) {
	if file, err := os.OpenFile(path, OPEN_FILE_FLAGS, os.ModePerm); err == nil {
		return file, true
	}
	LOG.Critical("File tunnel create data file failed")
	return nil, false
}

func (tunnel *FileWriter) Prepare() bool {
	if atomic.CompareAndSwapInt32(&globalInitializer, 0, 1) {
		if file, ok := _Open(tunnel.Local); ok {
			tunnel.dataFile = &DataFile{filehandle: file}
		} else {
			LOG.Critical("File tunnel open failed")
			return false
		}

		if info, err := os.Stat(tunnel.Local); err != nil || info.IsDir() {
			LOG.Critical("File tunnel check path failed. %v", err)
			return false
		}
		tunnel.dataFile.WriteHeader()

		oplogMessage = make(chan *TMessage, 8192)

		go tunnel.SyncToDisk()
	}

	return true
}

func (tunnel *FileWriter) AckRequired() bool {
	return false
}

func (tunnel *FileWriter) ParsedLogsRequired() bool {
	return false
}
