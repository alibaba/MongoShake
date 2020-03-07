package collector

import (
	"testing"
	"fmt"

	"mongoshake/oplog"
	"mongoshake/collector/configure"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
)

// mock oplog with different namespace
func mockLog(ns string, ts bson.MongoTimestamp) *oplog.ParsedLog {
	return &oplog.ParsedLog{
		Timestamp:     ts,
		Operation:     "i",
		Namespace:     ns,
		Object:        bson.D{},
		Query:         bson.M{},
		UniqueIndexes: bson.M{},
	}
}

func TestDeserializer(t *testing.T) {
	// test deserializer

	var nr int
	// normal
	{
		fmt.Printf("TestDeserializer case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog

		syncer := &OplogSyncer{}
		syncer.startDeserializer()

		log1 := mockLog("a.b", 1)
		data1, err := bson.Marshal(log1)
		assert.Equal(t, nil, err, "should be equal")

		log2 := mockLog("a.b", 2)
		data2, err := bson.Marshal(log2)
		assert.Equal(t, nil, err, "should be equal")

		syncer.PendingQueue[0] <-[][]byte{data1}
		syncer.PendingQueue[1] <-[][]byte{data2}

		out1 := <-syncer.logsQueue[0]
		out2 := <-syncer.logsQueue[1]
		assert.Equal(t, 1, len(out1), "should be equal")
		assert.Equal(t, *log1, out1[0].Parsed.ParsedLog, "should be equal")
		assert.Equal(t, 1, len(out2), "should be equal")
		assert.Equal(t, *log2, out2[0].Parsed.ParsedLog, "should be equal")
	}
}