package module

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io/ioutil"

	"mongoshake/collector/configure"
	"mongoshake/tunnel"

	LOG "github.com/vinllen/log4go"
	"mongoshake/common"
)

const (
	NoCompress          uint32 = 0
	CompressWithGzip    uint32 = 1
	CompressWithSnappy  uint32 = 2
	CompressWithZlib    uint32 = 3
	CompressWithDeflate uint32 = 4
)

const (
	BestSpeed         = flate.BestSpeed
	BestCompression   = flate.BestCompression
	NormalCompression = flate.DefaultCompression
)

var CompressLevel = BestCompression

type Compress interface {
	Name() string
	Id() uint32
	Compress(chunk []byte) ([]byte, error)
	Decompress(compressed []byte) ([]byte, error)
}

func GetCompressorByName(name string) (Compress, error) {
	switch name {
	case utils.VarIncrSyncWorkerOplogCompressorGzip:
		return compressorGzip, nil
	case utils.VarIncrSyncWorkerOplogCompressorSnappy:
		return compressorSnappy, nil
	case utils.VarIncrSyncWorkerOplogCompressorZlib:
		return compressorZlib, nil
	case utils.VarIncrSyncWorkerOplogCompressorDeflate:
		return compressorDeflate, nil
	case utils.VarIncrSyncWorkerOplogCompressorNone:
		fallthrough
	default:
		return nil, errors.New("invalid compressor name")
	}
}

func GetCompressorById(id uint32) (Compress, error) {
	switch id {
	case CompressWithGzip:
		return compressorGzip, nil
	case CompressWithSnappy:
		return compressorSnappy, nil
	case CompressWithZlib:
		return compressorZlib, nil
	case CompressWithDeflate:
		return compressorDeflate, nil
	case NoCompress:
		fallthrough
	default:
		return nil, errors.New("invalid compressor id")
	}
}

/*
 * ====== Compressor =======
 *
 */
type Compressor struct {
	// compressor nil if compress is not enable
	zipper Compress
}

func (compressor *Compressor) IsRegistered() bool {
	return conf.Options.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone
}

func (compressor *Compressor) Install() bool {
	var err error
	if compressor.zipper, err = GetCompressorByName(conf.Options.IncrSyncWorkerOplogCompressor); err != nil {
		LOG.Critical("Worker create compressor %s failed", conf.Options.IncrSyncWorkerOplogCompressor)
		return false
	}

	// use high compress ratio by default
	CompressLevel = BestCompression

	return true
}

func (compressor *Compressor) Handle(message *tunnel.WMessage) int64 {
	var originSize, compressedSize = 0, 0
	// compress log entry data
	if len(message.RawLogs) != 0 {
		compressed := [][]byte{}
		// every log entry compress
		for _, log := range message.RawLogs {
			originSize += len(log)
			zipped, err := compressor.zipper.Compress(log)
			if err == nil {
				compressedSize += len(zipped)
				compressed = append(compressed, zipped)
			}
		}

		if compressedSize == 0 || len(compressed) != len(message.RawLogs) {
			LOG.Critical("Compressor result isn't equivalent. len(compressed) %d, len(Logs) %d", len(compressed), len(message.RawLogs))
			return tunnel.ReplyServerFault
		}

		LOG.Debug("Compressor-%s condense raw_size(%d), compress_size(%d), compress_ratio %d%%", compressor.zipper.Name(),
			originSize, compressedSize, compressedSize*100/originSize)
		message.Compress = compressor.zipper.Id()
		message.RawLogs = compressed
	} else {
		message.Compress = NoCompress
	}

	return tunnel.ReplyOK
}

type Writable struct {
	buffer *bytes.Buffer
}

type GZip struct {
	Writable
}

func NewGZipCompressor() *GZip {
	compressor := &GZip{Writable: Writable{buffer: new(bytes.Buffer)}}
	return compressor
}

func (Gzip *GZip) Name() string {
	return utils.VarIncrSyncWorkerOplogCompressorGzip
}

func (Gzip *GZip) Id() uint32 {
	return CompressWithGzip
}

func (Gzip *GZip) Compress(chunk []byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buffer, CompressLevel)
	w.Write(chunk)
	w.Close()
	if compressed, err := ioutil.ReadAll(buffer); err == nil {
		return compressed, nil
	}

	return nil, errors.New("GZip compress failed")
}

func (Gzip *GZip) Decompress(compressed []byte) ([]byte, error) {
	r, _ := gzip.NewReader(bytes.NewBuffer(compressed))
	if uncompress, err := ioutil.ReadAll(r); err == nil {
		return uncompress, nil
	}

	return nil, errors.New("GZip uncompress failed")
}

type Snappy struct {
	Writable
}

func NewSnappyCompressor() *Snappy {
	return &Snappy{}
}

func (snappy *Snappy) Name() string {
	return utils.VarIncrSyncWorkerOplogCompressorSnappy
}

func (snappy *Snappy) Id() uint32 {
	return CompressWithSnappy
}

func (snappy *Snappy) Compress(chunk []byte) ([]byte, error) {
	return chunk, nil
}

func (snappy *Snappy) Decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}

type Zlib struct {
	Writable
}

func NewZlibCompressor() *Zlib {
	compressor := &Zlib{Writable: Writable{buffer: new(bytes.Buffer)}}
	return compressor
}

func (zlib *Zlib) Name() string {
	return utils.VarIncrSyncWorkerOplogCompressorZlib
}

func (zlib *Zlib) Id() uint32 {
	return CompressWithZlib
}

func (ZLIB *Zlib) Compress(chunk []byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	w, _ := zlib.NewWriterLevel(buffer, CompressLevel)
	w.Write(chunk)
	w.Close()
	if compressed, err := ioutil.ReadAll(buffer); err == nil {
		return compressed, nil
	}

	return nil, errors.New("Zlib compress failed")
}

func (ZLIB *Zlib) Decompress(compressed []byte) ([]byte, error) {
	r, _ := zlib.NewReader(bytes.NewBuffer(compressed))
	if uncompress, err := ioutil.ReadAll(r); err == nil {
		return uncompress, nil
	}

	return nil, errors.New("Zlib uncompress failed")
}

type Deflate struct {
	Writable
}

func NewDeflateCompressor() *Deflate {
	compressor := &Deflate{Writable: Writable{buffer: new(bytes.Buffer)}}
	return compressor
}

func (deflate *Deflate) Name() string {
	return utils.VarIncrSyncWorkerOplogCompressorDeflate
}

func (deflate *Deflate) Id() uint32 {
	return CompressWithDeflate
}

func (deflate *Deflate) Compress(chunk []byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	w, _ := flate.NewWriter(buffer, CompressLevel)
	w.Write(chunk)
	w.Close()
	if compressed, err := ioutil.ReadAll(buffer); err == nil {
		return compressed, nil
	}

	return nil, errors.New("deflate compress failed")
}

func (deflate *Deflate) Decompress(compressed []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewBuffer(compressed))
	if uncompress, err := ioutil.ReadAll(r); err == nil {
		return uncompress, nil
	}

	return nil, errors.New("deflate uncompressed failed")
}
