package collector

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"sync/atomic"
	"testing"
)

const (
	repliset1 = "mgset-117"
	repliset2 = "mgset-118"
	repliset3 = "mgset-119"
)

var (
	obj   = bson.D{{"_id", bson.ObjectId("5d30567ea99c6e5beb1d8af3")}, {"a", 0}}
	query = bson.M{"_id": bson.ObjectId("5d30567ea99c6e5beb1d8af3")}
)

func mockMvckSyncer(replset string,
	mvckManager *MoveChunkManager) *OplogSyncer {
	syncer := &OplogSyncer{
		replset:     replset,
		mvckManager: mvckManager,
	}
	filterList := filter.OplogFilterChain{}
	syncer.batcher = NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
	return syncer
}

func mockMoveChunkManager() *MoveChunkManager {
	manager := NewMoveChunkManager()
	manager.addOplogSyncer(mockMvckSyncer(repliset1, manager))
	manager.addOplogSyncer(mockMvckSyncer(repliset2, manager))
	manager.addOplogSyncer(mockMvckSyncer(repliset3, manager))
	return manager
}

func mockMvckLog(ind int, op string, object bson.D, query bson.M, fromMigrate bool) *oplog.PartialLog {
	return &oplog.PartialLog{
		Timestamp:   bson.MongoTimestamp(ind),
		Namespace:   "db.tbl",
		Operation:   op,
		Object:      object,
		Query:       query,
		FromMigrate: fromMigrate,
	}
}

func mockTransfer(syncer *OplogSyncer, log *oplog.PartialLog) []interface{} {
	var result []interface{}
	barrier, tsUpdate, updateObj := syncer.mvckManager.BarrierOplog(syncer.replset, log)
	result = append(result, barrier)
	result = append(result, tsUpdate)
	if !barrier {
		result = append(result, updateObj)
	} else {
		result = append(result, nil)
	}

	worker := syncer.batcher.workerGroup[0]
	if !barrier {
		atomic.StoreInt64(&worker.unack, utils.TimestampToInt64(log.Timestamp))
		atomic.StoreInt64(&worker.ack, utils.TimestampToInt64(log.Timestamp))

		syncer.mvckManager.UpdateOfferTs(syncer.replset)
	}

	return result
}

func updateGenerator(value int) bson.D {
	return bson.D{{"$set", bson.D{{"aaa", value}}}}
}

func TransferLog(manager *MoveChunkManager, cmd string, ts int) []interface{} {
	syncer1 := manager.syncInfoMap[repliset1].syncer
	syncer2 := manager.syncInfoMap[repliset2].syncer
	syncer3 := manager.syncInfoMap[repliset3].syncer

	// 1 means insert, 2 means update, 3 means delete ...
	switch cmd {
	case "a1":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "i", obj, nil, false))
	case "a2":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "u", updateGenerator(111), query, false))
	case "a3":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "d", obj, nil, true))
	case "a4":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "i", obj, nil, true))
	case "a5":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "u", updateGenerator(555), query, false))
	case "b1":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "i", obj, nil, true))
	case "b2":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "u", updateGenerator(222), query, false))
	case "b3":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "d", obj, nil, true))
	case "b4":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "i", obj, nil, true))
	case "b5":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "u", updateGenerator(444), query, false))
	case "b6":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "d", obj, nil, true))
	case "c1":
		return mockTransfer(syncer3,
			mockMvckLog(ts, "i", obj, nil, true))
	case "c2":
		return mockTransfer(syncer3,
			mockMvckLog(ts, "u", updateGenerator(333), query, false))
	case "c3":
		return mockTransfer(syncer3,
			mockMvckLog(ts, "d", obj, nil, true))
	case "a0":
		return mockTransfer(syncer1,
			mockMvckLog(ts, "n", nil, nil, false))
	case "b0":
		return mockTransfer(syncer2,
			mockMvckLog(ts, "n", nil, nil, false))
	case "c0":
		return mockTransfer(syncer3,
			mockMvckLog(ts, "n", nil, nil, false))
	default:
		fmt.Println("ERROR cmd")
		return []interface{}{}
	}
}

func TestMoveChunkManager(t *testing.T) {
	// test MoveChunkManager
	var nr int
	conf.Options.MoveChunkInterval = 50
	{
		// move chunk order: a -> b -> c
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, 0, len(manager.moveChunkMap))
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 8), "should be equal")
		assert.Equal(t, 1, len(manager.moveChunkMap))
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 4), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b2", 5), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 7), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 5), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(333)},
			TransferLog(manager, "c2", 8), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}
	{
		// move chunk order: a -> b -> c
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 7), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 8), "should be equal")
		assert.Equal(t, 1, len(manager.moveChunkMap))
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 4), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b2", 5), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 8), "should be equal")
		assert.Equal(t, 1, len(manager.moveChunkMap))
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 5), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(333)},
			TransferLog(manager, "c2", 8), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}
	{
		// move chunk order: a -> b -> c
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 4), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 7), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 8), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 5), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(333)},
			TransferLog(manager, "c2", 8), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}

	{
		// move chunk order: a -> b -> c -> b -> a
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 3), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b2", 4), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 5), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 6), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 7), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 4), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 6), "should be equal")
		assert.Equal(t, []interface{}{true, true, nil},
			TransferLog(manager, "b4", 7), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(333)},
			TransferLog(manager, "c2", 6), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c3", 10), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b5", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a4", 9), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "a5", 10), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(444)},
			TransferLog(manager, "b5", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b6", 10), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(555)},
			TransferLog(manager, "a5", 10), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}
	{
		// move chunk order: a -> b -> c -> b -> a
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 7), "should be equal")
		assert.Equal(t, []interface{}{true, true, nil},
			TransferLog(manager, "a4", 9), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 3), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b2", 4), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 5), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 6), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 4), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 6), "should be equal")
		assert.Equal(t, []interface{}{true, true, nil},
			TransferLog(manager, "b4", 7), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b5", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(333)},
			TransferLog(manager, "c2", 6), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c3", 10), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{false, true, updateGenerator(444)},
			TransferLog(manager, "b5", 8), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b6", 10), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "a5", 10), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{false, true, updateGenerator(555)},
			TransferLog(manager, "a5", 10), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}

	{
		// move chunk from a to b failed, then repeat it
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 7), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 3), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 7), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 4), "should be equal")
		assert.Equal(t, []interface{}{true, true, nil},
			TransferLog(manager, "b1", 5), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "b2", 6), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
			TransferLog(manager, "b2", 6), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b3", 7), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 0, len(manager.moveChunkMap))
	}
	{
		// move chunk from a to b failed, then move from a to c, unsupported
		fmt.Printf("TestMoveChunkManager case %d.\n", nr)
		nr++
		manager := mockMoveChunkManager()
		manager.start()

		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a1", 1), "should be equal")
		assert.Equal(t, []interface{}{false, true, updateGenerator(111)},
			TransferLog(manager, "a2", 2), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "a3", 5), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "b1", 3), "should be equal")
		assert.Equal(t, []interface{}{false, true, nil},
			TransferLog(manager, "c1", 4), "should be equal")
		assert.Equal(t, []interface{}{true, false, nil},
			TransferLog(manager, "c2", 5), "should be equal")
		manager.eliminateBarrier()
		//assert.Equal(t, []interface{}{false, true, updateGenerator(222)},
		//	TransferLog(manager, "c2", 5), "should be equal")
		manager.eliminateBarrier()
		assert.Equal(t, 1, len(manager.moveChunkMap))
	}
}
