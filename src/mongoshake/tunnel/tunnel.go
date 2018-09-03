package tunnel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

const InitialStageChecking = false

const (
	MsgNormal         = 0x00000000
	MsgRetransmission = 0x00000001
	MsgProbe          = 0x00000010
	MsgResident       = 0x00000100
	MsgPersistent     = 0x00001000
	MsgStorageBackend = 0x00010000
)

const (
	ReplyOK                           = 0
	ReplyError                  int64 = -1
	ReplyNetworkOpFail          int64 = -2
	ReplyNetworkTimeout         int64 = -3
	ReplyRetransmission         int64 = -4
	ReplyServerFault            int64 = -5
	ReplyChecksumInvalid        int64 = -6
	ReplyCompressorNotSupported int64 = -7
	ReplyDecompressInvalid            = -8
)

// WMessage wrapped TMessage
type WMessage struct {
	*TMessage                      // whole raw log
	ParsedLogs []*oplog.PartialLog // parsed log
}
type TMessage struct {
	Checksum   uint32
	Tag        uint32
	Shard      uint32
	Compress   uint32
	RawLogs    [][]byte
}

func (msg *TMessage) Crc32() uint32 {
	var value uint32
	for _, log := range msg.RawLogs {
		value ^= crc32.ChecksumIEEE(log)
	}
	return value
}

func (msg *TMessage) ToBytes(order binary.ByteOrder) []byte {
	buffer := bytes.Buffer{}
	binary.Write(&buffer, order, msg.Checksum)
	binary.Write(&buffer, order, msg.Tag)
	binary.Write(&buffer, order, msg.Shard)
	binary.Write(&buffer, order, msg.Compress)
	binary.Write(&buffer, order, uint32(len(msg.RawLogs)))
	for _, log := range msg.RawLogs {
		binary.Write(&buffer, order, uint32(len(log)))
		buffer.Write(log)
	}
	return buffer.Bytes()
}

func (msg *TMessage) FromBytes(buf []byte, order binary.ByteOrder) {
	buffer := bytes.NewBuffer(buf)
	binary.Read(buffer, order, &msg.Checksum)
	binary.Read(buffer, order, &msg.Tag)
	binary.Read(buffer, order, &msg.Shard)
	binary.Read(buffer, order, &msg.Compress)
	var n uint32
	binary.Read(buffer, order, &n)
	nimo.AssertTrue((buffer.Len() != 0 && msg.Tag&MsgProbe == 0) ||
		(buffer.Len() == 0 && msg.Tag&MsgProbe != 0),
		"message decode left bytes are empty")
	var start = uint32(len(buf) - buffer.Len())
	for n != 0 {
		tmp := bytes.NewBuffer(buf[start:])
		var length uint32
		binary.Read(tmp, order, &length)
		start += 4
		// total "n" number should be exactly correct. crash with
		// out of range with slice if we got dirty records
		nimo.AssertTrue(start+length <= uint32(len(buf)), "oplogs in msg offset is invalid")
		bytes := buf[start : start+length]
		start += length
		msg.RawLogs = append(msg.RawLogs, bytes)
		n--
	}
}

func (msg *TMessage) String() string {
	return fmt.Sprintf("[cksum:%d, tag:%d, shard:%d, compress:%d, logs_len:%d]",
		msg.Checksum, msg.Tag, msg.Shard, msg.Compress, len(msg.RawLogs))
}

func (msg *TMessage) ApproximateSize() uint64 {
	var size uint64 = 0
	for _, log := range msg.RawLogs {
		size += uint64(len(log))
	}
	return size
}

type Writer interface {
	/**
	 * Indicate weather this tunnel cares about ACK feedback value.
	 * Like RPC_TUNNEL (ack required is true), it's asynchronous and
	 * needs peer receiver has completely consumed the log entries
	 * and we can drop the reserved log entries only if the log entry
	 * ACK is confirmed
	 */
	AckRequired() bool

	/**
	 * prepare stage of the tunnel such as create the network connection or initialize
	 * something etc before the Send() invocation.
	 * return true on successful or false on failed
	 */
	Prepare() bool

	/**
	 * write the real tunnel message to tunnel.
	 *
	 * return the right ACK offset value with positive number. if AckRequired is set
	 * this ACk offset is used to purge buffered oplogs. Otherwise upper layer use
	 * the max oplog ts as ACK offset and discard the returned value (ACK offset).
	 * error on returning a negative number
	 */
	Send(message *WMessage) int64

	/**
	 * whether need parsed log or raw log
	 */
	ParsedLogsRequired() bool
}

type WriterFactory struct {
	Name string
}

// create specific Tunnel with tunnel name and pass connection
// or usefully meta
func (factory *WriterFactory) Create(address []string, workerId uint32) Writer {
	switch factory.Name {
	case "kafka":
		return &KafkaWriter{RemoteAddr: address[0]}
	case "tcp":
		return &TCPWriter{RemoteAddr: address[0]}
	case "rpc":
		return &RPCWriter{RemoteAddr: address[0]}
	case "mock":
		return &MockWriter{}
	case "file":
		return &FileWriter{Local: address[0]}
	case "direct":
		return &DirectWriter{RemoteAddrs: address, ReplayerId: workerId}
	default:
		LOG.Critical("Specific tunnel not found [%s]", factory.Name)
		return nil
	}
}

// create specific Tunnel with tunnel name and pass connection
// or usefully meta
func (factory *ReaderFactory) Create(address string) Reader {
	switch factory.Name {
	case "kafka":
		return &KafkaReader{address: address}
	case "tcp":
		return &TCPReader{listenAddress: address}
	case "rpc":
		return &RPCReader{address: address}
	case "mock":
		return &MockReader{}
	case "file":
		return &FileReader{File: address}
	default:
		LOG.Critical("Specific tunnel not found [%s]", factory.Name)
		return nil
	}
}

type Reader interface {
	/**
	 * Bridge of tunnel reader and aggregater(replayer)
	 *
	 */
	Link(aggregate []Replayer) error
}

type Replayer interface {
	/**
	 * Replay oplog entry with batched Oplog
	 *
	 */
	Sync(message *TMessage, completion func()) int64

	/**
	 * Ack offset value
	 *
	 */
	GetAcked() int64
}

type ReaderFactory struct {
	Name string
}
