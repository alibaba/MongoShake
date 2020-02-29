package tunnel

import (
	"math/rand"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"time"
	"fmt"
)

const (
	BatchSize = 64
	TableName = "mongoshake_mock.table"
)

var opDict = []string{"i", "d", "u", "n"}

type MockReader struct {
	generator []*FakeGenerator
}

type FakeGenerator struct {
	// not owned
	replayer []Replayer
	index    uint32
}

func (tunnel *MockReader) Link(replayer []Replayer) error {
	tunnel.generator = make([]*FakeGenerator, len(replayer))
	for i := 0; i != len(replayer); i++ {
		LOG.Info("mock receiver generator-%d start", i)
		tunnel.generator[i] = &FakeGenerator{replayer: replayer}
		tunnel.generator[i].index = uint32(i)
		go tunnel.generator[i].start()
	}

	return nil
}

func (generator *FakeGenerator) start() {
	existIds := make(map[string]bson.ObjectId, 10000000)
	for {
		var batch []*oplog.GenericOplog
		var partialLog *oplog.PartialLog

		for i := 0; i != BatchSize; i++ {
			partialLog = &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Timestamp: bson.MongoTimestamp(time.Now().Unix() << 32),
					Namespace: fmt.Sprintf("%s_%d", TableName, generator.index),
				},
			}
			switch nr := rand.Uint32(); {
			case nr%1000 == 0:
				// noop 0.1%
				partialLog.Operation = "n"
				partialLog.Gid = "mock-noop"
				partialLog.Object = bson.D{bson.DocElem{"mongoshake-mock", "ApsaraDB"}}
			case nr%100 == 0:
				// delete 1%
				for k, oid := range existIds {
					partialLog.Operation = "d"
					partialLog.Gid = "mock-delete"
					partialLog.Object = bson.D{bson.DocElem{"_id", oid}}
					delete(existIds, k)
					break
				}
			case nr%3 == 0:
				// update 30%
				for _, oid := range existIds {
					partialLog.Operation = "u"
					partialLog.Gid = "mock-update"
					// partialLog.Object = bson.M{"$set": bson.M{"updates": nr}}
					partialLog.Object = bson.D{
						bson.DocElem{
							Name: "$set",
							Value: bson.D{
								bson.DocElem{"updates", nr},
							},
						},
					}
					partialLog.Query = bson.M{"_id": oid}
					break
				}
			default:
				// insert 70%
				oid := bson.NewObjectId()
				partialLog.Operation = "i"
				partialLog.Gid = "mock-insert"
				partialLog.Object = bson.D{
					bson.DocElem{"_id", oid},
					bson.DocElem{"test", "1"},
					bson.DocElem{"abc", nr},
				}
				existIds[oid.Hex()] = oid
			}
			bytes, _ := bson.Marshal(partialLog)
			batch = append(batch, &oplog.GenericOplog{Raw: bytes})
		}
		generator.replayer[generator.index].Sync(&TMessage{
			Checksum: 0,
			Tag:      MsgRetransmission,
			Shard:    generator.index,
			Compress: 0,
			RawLogs:  oplog.LogEntryEncode(batch),
		}, nil)

		LOG.Info("mock generator-index-%d generate and apply logs %d", generator.index, len(batch))
	}
}
