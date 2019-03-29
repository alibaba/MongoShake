package tunnel

import (
	"bytes"
	"encoding/binary"

	"mongoshake/tunnel/kafka"

	LOG "github.com/vinllen/log4go"
)

type KafkaWriter struct {
	RemoteAddr string
	writer     *kafka.SyncWriter
}

func (tunnel *KafkaWriter) Prepare() bool {
	writer, err := kafka.NewSyncWriter(tunnel.RemoteAddr)
	if err != nil {
		LOG.Critical("KafkaWriter prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	if err := writer.Start(); err != nil {
		LOG.Critical("KafkaWriter prepare[%v] start writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	tunnel.writer = writer
	return true
}

func (tunnel *KafkaWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}

	message.Tag |= MsgPersistent

	byteBuffer := bytes.NewBuffer([]byte{})
	// checksum
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Checksum))
	// tag
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Tag))
	// shard
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Shard))
	// compressor
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Compress))
	// serialize log count
	binary.Write(byteBuffer, binary.BigEndian, uint32(len(message.RawLogs)))

	// serialize logs
	for _, log := range message.RawLogs {
		binary.Write(byteBuffer, binary.BigEndian, uint32(len(log)))
		binary.Write(byteBuffer, binary.BigEndian, log)
	}

	err := tunnel.writer.SimpleWrite(byteBuffer.Bytes())

	if err != nil {
		LOG.Error("KafkaWriter send[%v] error[%v]", tunnel.RemoteAddr, err)
		return ReplyError
	}

	// KafkaWriter.AckRequired() is always false, return 0 directly
	return 0
}

func (tunnel *KafkaWriter) AckRequired() bool {
	return false
}

func (tunnel *KafkaWriter) ParsedLogsRequired() bool {
	return false
}
