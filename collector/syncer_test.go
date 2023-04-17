package collector

import (
	"fmt"
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

// mock oplog with different namespace
func mockLog(ns string, ts int64, withDefault bool, gid string) *oplog.ParsedLog {
	switch withDefault {
	case true:
		return &oplog.ParsedLog{
			Timestamp:     utils.Int64ToTimestamp(ts),
			Operation:     "i",
			Namespace:     ns,
			Object:        bson.D{},
			Query:         bson.D{},
			UniqueIndexes: bson.M{},
			LSID:          bson.Raw{},
			Gid:           gid,
		}
	case false:
		return &oplog.ParsedLog{
			Timestamp: utils.Int64ToTimestamp(ts),
			Operation: "i",
			Namespace: ns,
			Object:    bson.D{},
			Query:     bson.D{},
			Gid:       gid,
		}
	}
	return nil
}

// mock change stream event
func mockEvent(nsCollection string, ts int64) *oplog.Event {
	return &oplog.Event{
		Ns: bson.M{
			"db":   "testDB",
			"coll": nsCollection,
		},
		OperationType: "insert",
		ClusterTime:   utils.Int64ToTimestamp(ts),
	}
}

func TestDeserializer(t *testing.T) {
	// test deserializer

	utils.InitialLogger("", "", "debug", true, 1)

	var nr int
	// normal
	{
		fmt.Printf("TestDeserializer case %d.\n", nr)
		nr++

		conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog

		syncer := &OplogSyncer{}
		syncer.startDeserializer()

		log1 := mockLog("a.b", 1, false, "")
		data1, err := bson.Marshal(log1)
		assert.Equal(t, nil, err, "should be equal")

		log2 := mockLog("a.b", 2, false, "")
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

		log1 := mockLog("testDB.b", 1, false, "")
		log2 := mockLog("testDB.c", 2, false, "")

		assert.Equal(t, 1, len(out1), "should be equal")
		assert.Equal(t, *log1, out1[0].Parsed.ParsedLog, "should be equal")
		assert.Equal(t, 1, len(out2), "should be equal")
		assert.Equal(t, *log2, out2[0].Parsed.ParsedLog, "should be equal")

		// unmarshal the raw data in log and do comparison again
		rawParsed1 := new(oplog.ParsedLog)
		err = bson.Unmarshal(out1[0].Raw, rawParsed1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, *mockLog("testDB.b", 1, false, ""), *rawParsed1, "should be equal")
		rawParsed2 := new(oplog.ParsedLog)
		err = bson.Unmarshal(out2[0].Raw, rawParsed2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, *mockLog("testDB.c", 2, false, ""), *rawParsed2, "should be equal")
	}
}

func TestFilterOplogGid(t *testing.T) {
	// test filterOplogGid

	utils.InitialLogger("", "", "info", true, 1)

	var nr int
	{
		fmt.Printf("TestFilterOplogGid case %d.\n", nr)
		nr++

		conf.Options.FilterOplogGids = true

		batchedLog := [][]*oplog.GenericOplog{
			[]*oplog.GenericOplog{
				{
					Parsed: &oplog.PartialLog{
						ParsedLog: *mockLog("test", 100, false, ""),
					},
				},
				{
					Parsed: &oplog.PartialLog{
						ParsedLog: *mockLog("test2", 123, false, "555"),
					},
				},
			},
			[]*oplog.GenericOplog{
				{
					Parsed: &oplog.PartialLog{
						ParsedLog: *mockLog("test3", 124, false, "666"),
					},
				},
			},
		}

		syncer := &OplogSyncer{}
		err := syncer.filterOplogGid(batchedLog)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "", batchedLog[0][0].Parsed.Gid, "should be equal")
		assert.Equal(t, "", batchedLog[0][1].Parsed.Gid, "should be equal")
		assert.Equal(t, "", batchedLog[1][0].Parsed.Gid, "should be equal")

		for i := range batchedLog {
			for j := range batchedLog[i] {
				if len(batchedLog[i][j].Raw) == 0 {
					continue
				}

				logRec := new(oplog.ParsedLog)
				fmt.Println(batchedLog[i][j].Raw)
				err = bson.Unmarshal(batchedLog[i][j].Raw, logRec)
				assert.Equal(t, nil, err, "should be equal")
				assert.Equal(t, *logRec, batchedLog[i][j].Parsed.ParsedLog, "should be equal")
			}
		}
	}
}
