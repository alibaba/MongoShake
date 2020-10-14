package tunnel

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"mongoshake/oplog"
	"math"
	"mongoshake/tunnel/kafka"
	"mongoshake/common"
	"mongoshake/collector/configure"
)

func TestKafkaWriter(t *testing.T) {
	// test KafkaWriter

	var nr int

	{
		fmt.Printf("TestKafkaWriter case %d.\n", nr)
		nr++

		utils.InitialLogger("", "", "info", true, true)

		msg := &WMessage{
			TMessage: &TMessage{
				RawLogs: [][]byte{{123}},
				Tag: 0,
			},
			ParsedLogs: []*oplog.PartialLog {
				{
					ParsedLog: oplog.ParsedLog {
						Object: bson.D{
							bson.DocElem{"$v", 1},
							bson.DocElem{"$set", bson.M{
								"sale_qty":   0,
								"sale_value": math.NaN(),
							}},
						},
					},
				},
			},
		}

		// fmt.Println(msg.ParsedLogs[0])

		conf.Options.TunnelMessage = utils.VarTunnelMessageJson
		w := &KafkaWriter{
			writer: new(kafka.SyncWriter),
		}
		val := w.Send(msg)
		assert.Equal(t, int64(0), val, "should be equal")
	}
}