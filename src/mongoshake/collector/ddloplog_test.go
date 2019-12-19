package collector

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/oplog"
	"sync/atomic"
	"testing"
)

func mockDDLSyncer(replset string) *OplogSyncer {
	syncer := &OplogSyncer{
		replset: replset,
	}
	filterList := filter.OplogFilterChain{}
	syncer.batcher = NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})
	return syncer
}

func mockDDLManager() *DDLManager {
	manager := &DDLManager{
		ddlMap:       make(map[DDLKey]*DDLValue),
		syncMap:      make(map[string]*OplogSyncer),
		ToIsSharding: true,
	}
	manager.addOplogSyncer(mockDDLSyncer(repliset1))
	manager.addOplogSyncer(mockDDLSyncer(repliset2))
	manager.addOplogSyncer(mockDDLSyncer(repliset3))
	return manager
}

func mockOpLog(ind int, op string, object bson.D) *oplog.PartialLog {
	return &oplog.PartialLog{
		Timestamp:   bson.MongoTimestamp(ind),
		Namespace:   "db.$cmd",
		Operation:   op,
		Object:      object,
		FromMigrate: false,
	}
}

func TestDDLManager(t *testing.T) {
	// test TestDDLManager
	var nr int
	conf.Options.ReplayerDMLOnly = true
	{
		fmt.Printf("TestDDLManager case %d.\n", nr)
		nr++
		manager := mockDDLManager()
		manager.start()

		worker3 := manager.syncMap[repliset3].batcher.workerGroup[0]

		v1 := manager.addDDL(repliset1, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v1)
		v2 := manager.addDDL(repliset2, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v2)
		atomic.StoreInt64(&worker3.unack, 1)
		_, ok := <-v1.blockChan
		assert.Equal(t, true, ok)
	}
	{
		fmt.Printf("TestDDLManager case %d.\n", nr)
		nr++
		manager := mockDDLManager()
		manager.start()

		v1 := manager.addDDL(repliset1, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v1)
		v2 := manager.addDDL(repliset2, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v2)
		v3 := manager.addDDL(repliset3, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v3)
		_, ok1 := <-v1.blockChan
		assert.Equal(t, true, ok1)
		manager.UnBlockDDL(repliset1, mockOpLog(1, "c", bson.D{{"create", "tbl"}}))
		_, ok2 := <-v2.blockChan
		assert.Equal(t, false, ok2)
	}
	{
		fmt.Printf("TestDDLManager case %d.\n", nr)
		nr++
		manager := mockDDLManager()
		manager.start()

		worker1 := manager.syncMap[repliset1].batcher.workerGroup[0]

		v1 := manager.addDDL(repliset1, mockOpLog(1, "c", bson.D{{"create", "tmp"}}))
		assert.NotNil(t, v1)
		v2 := manager.addDDL(repliset2, mockOpLog(2, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v2)
		v3 := manager.addDDL(repliset3, mockOpLog(2, "c", bson.D{{"createIndexes", "tbl"}}))
		assert.NotNil(t, v3)
		_, ok1 := <-v1.blockChan
		assert.Equal(t, true, ok1)
		manager.UnBlockDDL(repliset1, mockOpLog(1, "c", bson.D{{"create", "tmp"}}))
		v4 := manager.addDDL(repliset1, mockOpLog(2, "c", bson.D{{"create", "tbl"}}))
		assert.NotNil(t, v4)
		_, ok2 := <-v2.blockChan
		assert.Equal(t, true, ok2)
		manager.UnBlockDDL(repliset2, mockOpLog(2, "c", bson.D{{"create", "tbl"}}))
		_, ok4 := <-v4.blockChan
		assert.Equal(t, false, ok4)
		v5 := manager.addDDL(repliset2, mockOpLog(3, "c", bson.D{{"createIndexes", "tbl"}}))
		assert.NotNil(t, v5)
		atomic.StoreInt64(&worker1.unack, 3)
		_, ok3 := <-v3.blockChan
		assert.Equal(t, true, ok3)
	}
}
