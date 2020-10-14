package tunnel

import (
	"bytes"
	"encoding/binary"

	"mongoshake/tunnel/kafka"

	LOG "github.com/vinllen/log4go"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"encoding/json"
	"strings"
)

type KafkaWriter struct {
	RemoteAddr string
	writer     *kafka.SyncWriter
}

func (tunnel *KafkaWriter) Name() string {
	return "kafka"
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

	switch conf.Options.TunnelMessage {
	case utils.VarTunnelMessageBson:
		// write the raw oplog directly
		for _, log := range message.RawLogs {
			if err := tunnel.writer.SimpleWrite(log); err != nil {
				LOG.Error("KafkaWriter send [%v] with type[%v] error[%v]", tunnel.RemoteAddr,
					conf.Options.TunnelMessage, err)
				return ReplyError
			}
		}
	case utils.VarTunnelMessageJson:
		for _, log := range message.ParsedLogs {
			// json marshal
			encode, err := json.Marshal(log.ParsedLog)
			if err != nil {
				if strings.Contains(err.Error(), "unsupported value:") {
					LOG.Error("KafkaWriter json marshal data[%v] meets unsupported value[%v], skip current oplog",
						log.ParsedLog, err)
					continue
				} else {
					LOG.Error("KafkaWriter json marshal data[%v] error[%v]", log.ParsedLog, err)
					return ReplyServerFault
				}
			}
			if err := tunnel.writer.SimpleWrite(encode); err != nil {
				LOG.Error("KafkaWriter send [%v] with type[%v] error[%v]", tunnel.RemoteAddr,
					conf.Options.TunnelMessage, err)
				return ReplyError
			}
		}
	case utils.VarTunnelMessageRaw:
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

		if err := tunnel.writer.SimpleWrite(byteBuffer.Bytes()); err != nil {
			LOG.Error("KafkaWriter send [%v] with type[%v] error[%v]", tunnel.RemoteAddr,
				conf.Options.TunnelMessage, err)
			return ReplyError
		}

	default:
		LOG.Crash("unknown tunnel.message type: ", conf.Options.TunnelMessage)
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
