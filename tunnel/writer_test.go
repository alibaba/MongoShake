package tunnel

import (
	"fmt"
	"github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// return $nr oplog inside
func generateWMessage(val, nr int) *WMessage {
	parsedLogs := make([]*oplog.PartialLog, 0, nr)
	for i := val; i < val+nr; i++ {
		parsedLogs = append(parsedLogs, &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "i",
				Object: bson.D{
					bson.E{"val", i},
				},
			},
		})
	}

	return &WMessage{
		TMessage: &TMessage{ // meaningless
			RawLogs: [][]byte{{123}},
			Tag:     0,
		},
		ParsedLogs: parsedLogs,
	}
}

func parseJsonValue(input []byte) (interface{}, error) {
	jsonParsedMap := make(map[string]interface{})
	err := bson.UnmarshalExtJSON(input, true, &jsonParsedMap)
	if err != nil {
		return 0, err
	}

	return jsonParsedMap["o"].(bson.A)[0].(map[string]interface{})["value"], nil
}

func TestKafkaWriter(t *testing.T) {
	// test KafkaWriter

	var nr int
	conf.Options.TunnelJsonFormat = "canonical_extended_json"

	// test flag
	unitTestWriteKafkaFlag = true
	unitTestWriteKafkaChan = make(chan []byte, 2048)

	// simple test, only write 1
	{
		fmt.Printf("TestKafkaWriter case %d.\n", nr)
		nr++

		utils.InitialLogger("", "", "info", true, 1)

		conf.Options.TunnelMessage = utils.VarTunnelMessageJson
		conf.Options.IncrSyncTunnelWriteThread = 1
		conf.Options.TunnelKafkaPartitionNumber = 1
		conf.Options.IncrSyncWorker = 8

		kafkaWriter := &KafkaWriter{
			RemoteAddr:  "shake-ut-test-kafka-addr",
			PartitionId: 0,
		}
		ok := kafkaWriter.Prepare()
		assert.Equal(t, true, ok, "should be equal")

		msg := generateWMessage(1, 1)

		val := kafkaWriter.Send(msg)
		assert.Equal(t, ReplyOK, val, "should be equal")

		data := <-unitTestWriteKafkaChan

		outVal, err := parseJsonValue(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int32(1), outVal, "should be equal")
	}

	// write some data
	{
		fmt.Printf("TestKafkaWriter case %d.\n", nr)
		nr++

		utils.InitialLogger("", "", "info", true, 1)

		conf.Options.TunnelMessage = utils.VarTunnelMessageJson
		conf.Options.IncrSyncTunnelWriteThread = 24
		conf.Options.TunnelKafkaPartitionNumber = 3
		conf.Options.IncrSyncWorker = 8

		kafkaWriter := &KafkaWriter{
			RemoteAddr:  "shake-ut-test-kafka-addr",
			PartitionId: 0,
		}
		ok := kafkaWriter.Prepare()
		assert.Equal(t, true, ok, "should be equal")

		batchSize := 5
		writeNr := 20
		for i := 0; i <= writeNr; i += batchSize {
			msg := generateWMessage(i, batchSize)

			val := kafkaWriter.Send(msg)
			assert.Equal(t, ReplyOK, val, "should be equal")
		}

		// pay attention: unitTestWriteKafkaChan may not be drain when run next case
		for i := 0; i < writeNr+batchSize; i++ {
			data := <-unitTestWriteKafkaChan

			outVal, err := parseJsonValue(data)
			assert.Equal(t, nil, err, "should be equal")
			assert.Equal(t, int32(i), outVal, "should be equal")
		}

		// drain all
	X:
		for {
			select {
			case <-unitTestWriteKafkaChan:
			default:
				break X
			}
		}
	}

	// write NaN value
	{
		fmt.Printf("TestKafkaWriter case %d.\n", nr)
		nr++

		utils.InitialLogger("", "", "info", true, 1)

		conf.Options.TunnelMessage = utils.VarTunnelMessageJson
		conf.Options.IncrSyncTunnelWriteThread = 1
		conf.Options.TunnelKafkaPartitionNumber = 1
		conf.Options.IncrSyncWorker = 8

		kafkaWriter := &KafkaWriter{
			RemoteAddr:  "shake-ut-test-kafka-addr",
			PartitionId: 0,
		}
		ok := kafkaWriter.Prepare()
		assert.Equal(t, true, ok, "should be equal")

		msg := &WMessage{
			TMessage: &TMessage{
				RawLogs: [][]byte{{123}},
				Tag:     0,
			},
			ParsedLogs: []*oplog.PartialLog{
				{
					ParsedLog: oplog.ParsedLog{
						Object: bson.D{
							bson.E{"$v", 1},
							bson.E{"$set", bson.M{
								"sale_qty":   0,
								"sale_value": math.NaN(),
							}},
						},
					},
				},
			},
		}

		val := kafkaWriter.Send(msg)
		assert.Equal(t, ReplyOK, val, "should be equal")

		data := <-unitTestWriteKafkaChan
		jsonParsedMap := make(map[string]interface{})
		err := bson.UnmarshalExtJSON(data, true, &jsonParsedMap)
		assert.Equal(t, nil, err, "should be equal")
		fmt.Println(jsonParsedMap)
		output := jsonParsedMap["o"].(bson.A)[1].(map[string]interface{})["value"].(map[string]interface{})
		assert.Equal(t, int32(0), output["sale_qty"], "should be equal")
		assert.Equal(t, true, math.IsNaN(output["sale_value"].(float64)), "should be equal")
	}
}
