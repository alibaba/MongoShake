package tunnel

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/tunnel/kafka"

	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"os"
	"strings"
)

const (
	inputChanSize  = 256
	outputChanSize = 4196
)

var (
	unitTestWriteKafkaFlag = false
	unitTestWriteKafkaChan chan []byte
)

type outputLog struct {
	isEnd bool
	log   []byte
}

type KafkaWriter struct {
	RemoteAddr  string
	PartitionId int               // write to which partition
	writer      *kafka.SyncWriter // writer
	state       int64             // state: ok, error
	encoderNr   int64             // how many encoder
	inputChan   []chan *WMessage  // each encoder has 1 inputChan
	outputChan  []chan outputLog  // output chan length
	pushIdx     int64             // push into which encoder
	popIdx      int64             // pop from which encoder
}

func (tunnel *KafkaWriter) Name() string {
	return "kafka"
}

func (tunnel *KafkaWriter) Prepare() bool {
	var writer *kafka.SyncWriter
	var err error
	if !unitTestWriteKafkaFlag && conf.Options.IncrSyncTunnelKafkaDebug == "" {
		writer, err = kafka.NewSyncWriter(conf.Options.TunnelMongoSslRootCaFile, tunnel.RemoteAddr, tunnel.PartitionId)
		if err != nil {
			LOG.Critical("KafkaWriter prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
		if err := writer.Start(); err != nil {
			LOG.Critical("KafkaWriter prepare[%v] start writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
	}

	tunnel.writer = writer
	tunnel.state = ReplyOK
	tunnel.encoderNr = int64(math.Max(float64(conf.Options.IncrSyncTunnelWriteThread/conf.Options.IncrSyncWorker), 1))
	tunnel.inputChan = make([]chan *WMessage, tunnel.encoderNr)
	tunnel.outputChan = make([]chan outputLog, tunnel.encoderNr)
	tunnel.pushIdx = 0
	tunnel.popIdx = 0

	LOG.Info("%s starts: writer_thread count[%v]", tunnel, tunnel.encoderNr)

	// start encoder
	for i := 0; i < int(tunnel.encoderNr); i++ {
		tunnel.inputChan[i] = make(chan *WMessage, inputChanSize)
		tunnel.outputChan[i] = make(chan outputLog, outputChanSize)
		go tunnel.encode(i)
	}

	// start kafkaWriter
	go tunnel.writeKafka()

	return true
}

func (tunnel *KafkaWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}

	encoderId := atomic.AddInt64(&tunnel.pushIdx, 1)
	tunnel.inputChan[encoderId%tunnel.encoderNr] <- message

	// for transfer() not into default branch and then endless loop
	return 0
}

// KafkaWriter.AckRequired() is always false, return 0 directly
func (tunnel *KafkaWriter) AckRequired() bool {
	return false
}

func (tunnel *KafkaWriter) ParsedLogsRequired() bool {
	return false
}

func (tunnel *KafkaWriter) String() string {
	return fmt.Sprintf("KafkaWriter[%v] with partitionId[%v]", tunnel.RemoteAddr, tunnel.PartitionId)
}

func (tunnel *KafkaWriter) encode(id int) {
	for message := range tunnel.inputChan[id] {
		message.Tag |= MsgPersistent

		switch conf.Options.TunnelMessage {
		case utils.VarTunnelMessageBson:
			// write the raw oplog directly
			for i, log := range message.RawLogs {
				tunnel.outputChan[id] <- outputLog{
					isEnd: i == len(log)-1,
					log:   log,
				}
			}
		case utils.VarTunnelMessageJson:
			for i, log := range message.ParsedLogs {
				// json marshal
				var encode []byte
				var err error
				if conf.Options.TunnelJsonFormat == "" {
					encode, err = json.Marshal(log.ParsedLog)
					if err != nil {
						if strings.Contains(err.Error(), "unsupported value:") {
							LOG.Error("%s json marshal data[%v] meets unsupported value[%v], skip current oplog",
								tunnel, log.ParsedLog, err)
							continue
						} else {
							// should panic
							LOG.Crashf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
							tunnel.state = ReplyServerFault
						}
					}
				} else if conf.Options.TunnelJsonFormat == "canonical_extended_json" {
					encode, err = bson.MarshalExtJSON(log.ParsedLog, true, true)
					if err != nil {
						// should panic
						LOG.Crashf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
						tunnel.state = ReplyServerFault
					}
				} else {
					LOG.Crashf("unknown tunnel.json.format[%v]", conf.Options.TunnelJsonFormat)
				}

				tunnel.outputChan[id] <- outputLog{
					isEnd: i == len(message.ParsedLogs)-1,
					log:   encode,
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
			for i, log := range message.RawLogs {
				binary.Write(byteBuffer, binary.BigEndian, uint32(len(log)))
				binary.Write(byteBuffer, binary.BigEndian, log)

				tunnel.outputChan[id] <- outputLog{
					isEnd: i == len(message.ParsedLogs)-1,
					log:   byteBuffer.Bytes(),
				}
			}

		default:
			LOG.Crash("%s unknown tunnel.message type: ", tunnel, conf.Options.TunnelMessage)
		}
	}
}

func (tunnel *KafkaWriter) writeKafka() {
	// debug
	var debugF *os.File
	var err error
	if conf.Options.IncrSyncTunnelKafkaDebug != "" {
		fileName := fmt.Sprintf("%s-%d", conf.Options.IncrSyncTunnelKafkaDebug, tunnel.PartitionId)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			if debugF, err = os.Create(fileName); err != nil {
				LOG.Crashf("%s create kafka debug file[%v] failed: %v", tunnel, fileName, err)
			}
		} else {
			if debugF, err = os.OpenFile(fileName, os.O_RDWR, 0666); err != nil {
				LOG.Crashf("%s open kafka debug file[%v] failed: %v", tunnel, fileName, err)
			}
		}
		defer debugF.Close()
	}

	for {
		tunnel.popIdx = (tunnel.popIdx + 1) % tunnel.encoderNr
		// read chan
		for data := range tunnel.outputChan[tunnel.popIdx] {
			if unitTestWriteKafkaFlag {
				// unit test only
				unitTestWriteKafkaChan <- data.log
			} else if conf.Options.IncrSyncTunnelKafkaDebug != "" {
				if _, err = debugF.Write(data.log); err != nil {
					LOG.Crashf("%s write to kafka debug file failed: %v, input data: %s", tunnel, err, data.log)
				}
				debugF.Write([]byte{10})
			} else {
				for {
					if err = tunnel.writer.SimpleWrite(data.log); err != nil {
						LOG.Error("%s send [%v] with type[%v] error[%v]", tunnel, tunnel.RemoteAddr,
							conf.Options.TunnelMessage, err)

						tunnel.state = ReplyError
						time.Sleep(time.Second)
					} else {
						break
					}
				}
			}

			if data.isEnd {
				break
			}
		}
	}
}
