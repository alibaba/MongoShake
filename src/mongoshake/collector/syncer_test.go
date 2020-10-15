package collector

import (
	"testing"
	"fmt"

	"mongoshake/oplog"
	"mongoshake/collector/configure"
	"mongoshake/common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

// mock oplog with different namespace
func mockLog(ns string, ts bson.MongoTimestamp, withDefault bool) *oplog.ParsedLog {
	switch withDefault {
	case true:
		return &oplog.ParsedLog{
			Timestamp:     ts,
			Operation:     "i",
			Namespace:     ns,
			Object:        bson.D{},
			Query:         bson.M{},
			UniqueIndexes: bson.M{},
			Lsid:          bson.M{},
		}
	case false:
		return &oplog.ParsedLog{
			Timestamp: ts,
			Operation: "i",
			Namespace: ns,
			Object:    bson.D{},
			Query:     bson.M{},
		}
	}
	return nil
}

// mock change stream event
func mockEvent(nsCollection string, ts bson.MongoTimestamp) *oplog.Event {
	return &oplog.Event{
		Ns: bson.M{
			"db":   "testDB",
			"coll": nsCollection,
		},
		OperationType: "insert",
		ClusterTime:   ts,
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

		log1 := mockLog("a.b", 1, false)
		data1, err := bson.Marshal(log1)
		assert.Equal(t, nil, err, "should be equal")

		log2 := mockLog("a.b", 2, false)
		data2, err := bson.Marshal(log2)
		assert.Equal(t, nil, err, "should be equal")

		syncer.PendingQueue[0] <- [][]byte{data1}
		syncer.PendingQueue[1] <- [][]byte{data2}

		out1 := <-syncer.logsQueue[0]
		out2 := <-syncer.logsQueue[1]
		assert.Equal(t, 1, len(out1), "should be equal")
		assert.Equal(t, *log1, out1[0].Parsed.ParsedLog, "should be equal")
		assert.Equal(t, 1, len(out2), "should be equal")
		assert.Equal(t, *log2, out2[0].Parsed.ParsedLog, "should be equal")
	}

	{
		fmt.Printf("TestDeserializer case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
		conf.Options.Tunnel = utils.VarTunnelRpc

		syncer := &OplogSyncer{}
		syncer.startDeserializer()

		event1 := mockEvent("b", 1)
		data1, err := bson.Marshal(event1)
		assert.Equal(t, nil, err, "should be equal")

		event2 := mockEvent("c", 2)
		data2, err := bson.Marshal(event2)
		assert.Equal(t, nil, err, "should be equal")

		syncer.PendingQueue[0] <- [][]byte{data1}
		syncer.PendingQueue[1] <- [][]byte{data2}

		out1 := <-syncer.logsQueue[0]
		out2 := <-syncer.logsQueue[1]

		log1 := mockLog("testDB.b", 1, false)
		log2 := mockLog("testDB.c", 2, false)

		assert.Equal(t, 1, len(out1), "should be equal")
		assert.Equal(t, *log1, out1[0].Parsed.ParsedLog, "should be equal")
		assert.Equal(t, 1, len(out2), "should be equal")
		assert.Equal(t, *log2, out2[0].Parsed.ParsedLog, "should be equal")

		// unmarshal the raw data in log and do comparison again
		rawParsed1 := new(oplog.ParsedLog)
		err = bson.Unmarshal(out1[0].Raw, rawParsed1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, *mockLog("testDB.b", 1, false), *rawParsed1, "should be equal")
		rawParsed2 := new(oplog.ParsedLog)
		err = bson.Unmarshal(out2[0].Raw, rawParsed2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, *mockLog("testDB.c", 2, false), *rawParsed2, "should be equal")
	}
}
