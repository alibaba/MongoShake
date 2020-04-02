package replayer

import (
	"mongoshake/common"
	"mongoshake/modules"
	"mongoshake/oplog"
	"mongoshake/tunnel"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	PendingQueueCapacity = 256
)

type ExampleReplayer struct {
	Retransmit bool  // need re-transmit
	Ack        int64 // ack number

	// current compressor construct by TMessage
	// Compress field specific
	compressor module.Compress

	// pending queue, use to pass message
	pendingQueue chan *MessageWithCallback

	id int // current replayer id
}

type MessageWithCallback struct {
	message    *tunnel.TMessage
	completion func()
}

func NewExampleReplayer(id int) *ExampleReplayer {
	LOG.Info("ExampleReplayer start. pending queue capacity %d", PendingQueueCapacity)
	er := &ExampleReplayer{
		pendingQueue: make(chan *MessageWithCallback, PendingQueueCapacity),
		id:           id,
	}
	go er.handler()
	return er
}

/*
 * Receiver message and do the following steps:
 * 1. if we need re-transmit, this log will be discard
 * 2. validate the checksum
 * 3. decompress
 * 4. put message into channel
 * Generally speaking, do not modify this function.
 */
func (er *ExampleReplayer) Sync(message *tunnel.TMessage, completion func()) int64 {
	// tell collector we need re-trans all unacked oplogs first
	// this always happen on receiver restart !
	if er.Retransmit {
		// reject normal oplogs request
		if message.Tag&tunnel.MsgRetransmission == 0 {
			return tunnel.ReplyRetransmission
		}
		er.Retransmit = false
	}

	// validate the checksum value
	if message.Checksum != 0 {
		recalculated := message.Crc32()
		if recalculated != message.Checksum {
			// we need the peer to retransmission the current message
			er.Retransmit = true
			LOG.Critical("Tunnel message checksum bad. recalculated is 0x%x. origin is 0x%x", recalculated, message.Checksum)
			return tunnel.ReplyChecksumInvalid
		}
	}

	// decompress
	if message.Compress != module.NoCompress {
		// reuse current compressor handle
		var err error
		if er.compressor, err = module.GetCompressorById(message.Compress); err != nil {
			er.Retransmit = true
			LOG.Critical("Tunnel message compressor not support. is %d", message.Compress)
			return tunnel.ReplyCompressorNotSupported
		}
		var decompress [][]byte
		for _, toDecompress := range message.RawLogs {
			bits, err := er.compressor.Decompress(toDecompress)
			if err == nil {
				decompress = append(decompress, bits)
			}
		}
		if len(decompress) != len(message.RawLogs) {
			er.Retransmit = true
			LOG.Critical("Decompress result isn't equivalent. len(decompress) %d, len(Logs) %d", len(decompress), len(message.RawLogs))
			return tunnel.ReplyDecompressInvalid
		}

		message.RawLogs = decompress
	}

	er.pendingQueue <- &MessageWithCallback{message: message, completion: completion}
	return er.GetAcked()
}

func (er *ExampleReplayer) GetAcked() int64 {
	return er.Ack
}

/*
 * Users should modify this function according to different demands.
 */
func (er *ExampleReplayer) handler() {
	for msg := range er.pendingQueue {
		count := uint64(len(msg.message.RawLogs))
		if count == 0 {
			// probe request
			continue
		}

		// parse batched message
		oplogs := make([]oplog.ParsedLog, len(msg.message.RawLogs))
		for i, raw := range msg.message.RawLogs {
			oplogs[i] = oplog.ParsedLog{}
			if err := bson.Unmarshal(raw, &oplogs[i]); err != nil {
				// impossible switch, need panic and exit
				LOG.Crashf("unmarshal oplog[%v] failed[%v]", raw, err)
				return
			}
			LOG.Info(oplogs[i]) // just print for test, users can modify to fulfill different needs
			// fmt.Println(oplogs[i])
		}

		if callback := msg.completion; callback != nil {
			callback() // exec callback
		}

		// get the newest timestamp
		n := len(oplogs)
		lastTs := utils.TimestampToInt64(oplogs[n-1].Timestamp)
		er.Ack = lastTs

		LOG.Debug("handle ack[%v]", er.Ack)

		// add logical code below
	}
}
