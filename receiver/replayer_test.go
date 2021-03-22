package replayer

import (
	"fmt"
	"testing"

	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"

	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

func TestReplayer(t *testing.T) {
	// test Replayer

	var nr int
	{
		fmt.Printf("TestReplayer case %d.\n", nr)
		nr++

		data := &oplog.ParsedLog{
			Timestamp: 1234567,
			Operation: "o",
			Namespace: "a.b",
			Object: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "xxx",
				},
			},
			Query: bson.M{
				"what": "fff",
			},
		}

		out, err := bson.Marshal(data)
		assert.Equal(t, nil, err, "should be equal")

		r := NewExampleReplayer(0)
		ret := r.Sync(&tunnel.TMessage{
			RawLogs: [][]byte{out},
		}, nil)
		assert.Equal(t, int64(0), ret, "should be equal")

		time.Sleep(1 * time.Second)

		ret = r.Sync(&tunnel.TMessage{
			RawLogs: [][]byte{},
		}, nil)
		assert.Equal(t, int64(1234567), ret, "should be equal")
	}
}
