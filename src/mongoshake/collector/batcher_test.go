package collector

import (
	"fmt"
	"testing"

	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/oplog"
	"mongoshake/common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"time"
)

func mockSyncer() *OplogSyncer {
	length := 3
	syncer := &OplogSyncer{
		PendingQueue:           make([]chan [][]byte, length),
		logsQueue:              make([]chan []*oplog.GenericOplog, length),
		hasher:                 &oplog.PrimaryKeyHasher{},
		fullSyncFinishPosition: -3, // disable in current test
		replMetric:             utils.NewMetric("test", "",0),
	}
	for i := 0; i < length; i++ {
		syncer.logsQueue[i] = make(chan []*oplog.GenericOplog, 100)
	}
	return syncer
}

/*
 * return oplogs array with length=input length.
 * ddlGiven array marks the ddl.
 * noopGiven array marks the noop.
 * sameTsGiven array marks the index that ts is the same with before.
 */
func mockOplogs(length int, ddlGiven []int, noopGiven []int, sameTsGiven []int, startTs int64) []*oplog.GenericOplog {
	output := make([]*oplog.GenericOplog, length)
	ddlIndex := 0
	noopIndex := 0
	sameTsIndex := 0
	for i := 0; i < length; i++ {
		op := "u"
		if noopIndex < len(noopGiven) && noopGiven[noopIndex] == i {
			op = "n"
			noopIndex++
		} else if ddlIndex < len(ddlGiven) && ddlGiven[ddlIndex] == i {
			op = "c"
			ddlIndex++
		}
		output[i] = &oplog.GenericOplog{
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: "a.b",
					Operation: op,
					Timestamp: bson.MongoTimestamp(startTs + int64(i)) << 32,
				},
			},
		}
		if sameTsIndex < len(sameTsGiven) && i > 0 && sameTsGiven[sameTsIndex] == i {
			output[i].Parsed.Timestamp = output[i-1].Parsed.Timestamp
			sameTsIndex++
		}

		// fmt.Println(output[i].Parsed.Timestamp, output[i].Parsed.Operation)
	}
	// fmt.Println("--------------")
	return output
}

func TestBatchMore(t *testing.T) {
	// test BatchMore

	var nr int
	// normal
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 17, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		syncer.logsQueue[0] <- mockOplogs(1, nil, nil, nil, 300)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(300) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(300) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	// split by `conf.Options.IncrSyncAdaptiveBatchingMaxSize`
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(104) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(105) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{0, 1, 2, 3, 4}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{0, 1, 2, 3, 4, 5}, nil, nil, 100)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(105) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
	}

	// has ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(101) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, true, batcher.previousFlush, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(102) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 9, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	// has several ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, []int{3}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, []int{4, 5}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 15, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 3 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(101) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 2 in logsQ[1]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(102) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 4 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(204) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 5 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	// first one and last one are ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, []int{0}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, []int{6}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 18, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 16, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 79, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(378) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(379) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(379) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	// all ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(3, []int{0, 1, 2}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(1, []int{0}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(1, []int{0}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(100) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(200) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 79, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(378) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(379) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")


		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(379) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
	}

	// the edge of `IncrSyncAdaptiveBatchingMaxSize` is ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 8
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{5}, nil, nil, 100) // last is ddl
		syncer.logsQueue[2] <- mockOplogs(7, []int{3}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(104) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(105) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(202) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(205) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(206) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
	}

	// test all transaction
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(4, nil, nil, []int{1, 2, 3}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 1, len(batcher.transactionOplogs), "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 3, len(batcher.transactionOplogs), "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, true, batcher.transactionOplogs == nil, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.previousOplog.Parsed, "should be equal")
	}

	// test transaction only
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		// sameTs 3 == 4
		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, []int{4}, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		// at the end of queue
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, []int{5, 6}, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 13, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 13, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.previousOplog.Parsed, "should be equal")

		// inject more
		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, []int{1}, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(204) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(204) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// handle new before logs[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, "c", batchedOplog[0][0].Parsed.Operation, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0][0].Parsed.Object[0].Value.([]bson.M)), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(204) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.previousOplog.Parsed, "should be equal")

		// handle 1 one barrier (should be cut on the later version)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(204) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(300) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// handle new in logs[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(300) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.previousOplog.Parsed, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(303) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(304) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
	}

	// test transaction and filter(noop) mix
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		// sameTs 1 == 2, 5 == 6
		syncer.logsQueue[0] <- mockOplogs(9, nil, []int{3, 4, 7, 8}, []int{2, 6}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, 1, len(batcher.transactionOplogs), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, 1, len(batcher.transactionOplogs), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(7) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(8) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}

	// test transaction, DDL, filter(noop) mix
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, []int{0, 7}, []int{3, 4}, []int{2, 6}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 9, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// empty flush before 1
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit 6
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// before 7
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// 7
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(7) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, 0, len(batcher.transactionOplogs), "should be equal")

		// wait 8
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(7) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, int64(8) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// get 8
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(8) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.previousOplog.Parsed, "should be equal")
	}

	// complicate test!
	// test transaction, DDL, filter(noop) mix.
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, []int{5}, []int{0, 1, 2}, []int{4}, 0)
		syncer.logsQueue[1] <- mockOplogs(7, []int{0, 1}, []int{2, 3, 4, 5, 6}, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(8, []int{0}, []int{1, 7}, []int{4, 5, 6}, 200)

		// hit the 4 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, int64(3) << 32, int64( batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// after 4 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(3) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 5 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 0 in logsQ[1]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(100) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 1 in logsQ[1]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(101) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the end of logsQ[1]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(101) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(106) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// before 0 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(101) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(106) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 0 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(200) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(106) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 4 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(202) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(201) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// hit the 6 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, "c", batchedOplog[0][0].Parsed.Operation, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(203) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(207) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}

	// test simple case which run failed in sync test
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(4, []int{2}, []int{0, 1}, nil, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(1) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}

	// test DDL on the last
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, []int{5}, nil, nil, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(4) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}

	// test transaction on the last
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, nil, nil, []int{1, 2, 3, 4, 5}, 0)

		// before 0
		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// at the end of queue, but still no data
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastOplog.Parsed, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, 5, len(batcher.transactionOplogs), "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}

	// test empty
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		// syncer.logsQueue[0] <- mockOplogs(6, nil, nil, []int{1, 2, 3, 4, 5}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// all filtered
		syncer.logsQueue[0] <- mockOplogs(3, nil, []int{0, 1, 2}, nil, 0)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// inject one
		syncer.logsQueue[1] <- mockOplogs(5, nil, nil, nil, 100)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 4, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(103) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, int64(104) << 32, int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// get the last one
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(104) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		// the last == 2
		assert.Equal(t, int64(2) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// update the lastFilterOplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(104) << 32, int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		// the last == 104
		assert.Equal(t, int64(104) << 32, int64(batcher.lastFilterOplog.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
	}
}

func mockBatcher(nsWhite []string, nsBlack []string) *Batcher {
	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	// namespace filter
	if len(nsWhite) != 0 || len(nsBlack) != 0 {
		namespaceFilter := filter.NewNamespaceFilter(nsWhite, nsBlack)
		filterList = append(filterList, namespaceFilter)
	}
	return &Batcher{
		syncer: &OplogSyncer{
			fullSyncFinishPosition: 0,
		},
		filterList: filterList,
	}
}

/*
func mockFilterPartialLog(op, ns string, logObject bson.D) *oplog.PartialLog {
	// log.Timestamp > fullSyncFinishPosition
	return &oplog.PartialLog{
		Timestamp: bson.MongoTimestamp(1),
		Namespace: ns,
		Operation: op,
		RawSize:   1,
		Object:    logObject,
	}
}

func TestFilterPartialLog(t *testing.T) {
	// test filterPartialLog

	var nr int
	// normal
	{
		fmt.Printf("TestFilterPartialLog case %d.\n", nr)
		nr++

		batcher := mockBatcher([]string{"fdb1"}, []string{})
		log := mockFilterPartialLog("i", "fdb1.fcol1", bson.D{bson.DocElem{"a", 1}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{
			bson.DocElem{"create", "fcol1"},
			bson.DocElem{"idIndex", bson.D{
				bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
				bson.DocElem{"ns", "fdb1.fcol1"},
			}},
		})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{
			bson.DocElem{"renameCollection", "fdb1.fcol1"},
			bson.DocElem{"to", "fdb2.fcol2"}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
	}

	{
		fmt.Printf("TestFilterPartialLog case %d.\n", nr)
		nr++

		batcher := mockBatcher([]string{"fdb1.fcol1"}, []string{})
		log := mockFilterPartialLog("i", "fdb1.fcol1", bson.D{bson.DocElem{"a", 1}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{
			bson.DocElem{"create", "fcol1"},
			bson.DocElem{"idIndex", bson.D{
				bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
				bson.DocElem{"ns", "fdb1.fcol1"},
			}},
		})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		log = mockFilterPartialLog("c", "fdb1.$cmd", bson.D{
			bson.DocElem{"renameCollection", "fdb1.fcol1"},
			bson.DocElem{"to", "fdb2.fcol2"}})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
	}
}
*/


func TestDispatchBatches(t *testing.T) {
	// test dispatchBatches

	var nr int

	// 1. input array is empty
	{
		fmt.Printf("TestDispatchBatches case %d.\n", nr)
		nr++

		batcher := &Batcher{
			workerGroup: []*Worker{
				{
					queue: make(chan []*oplog.GenericOplog, 100),
				},
			},
		}
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 0
		ret := batcher.dispatchBatches(nil)
		assert.Equal(t, false, ret, "should be equal")
	}
}

func TestGetTargetDelay(t *testing.T) {
	var nr int

	{
		fmt.Printf("TestGetTargetDelay case %d.\n", nr)
		nr++

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10
		assert.Equal(t, int64(10), getTargetDelay(), "should be equal")
	}

	{
		fmt.Printf("TestGetTargetDelay case %d.\n", nr)
		nr++

		utils.IncrSentinelOptions.TargetDelay = 12
		conf.Options.IncrSyncTargetDelay = 10
		assert.Equal(t, int64(12), getTargetDelay(), "should be equal")
	}
}

func TestGetBatchWithDelay(t *testing.T) {
	// test getBatchWithDelay

	var nr int

	utils.InitialLogger("", "", "info", true, true)

	// reset to default
	utils.IncrSentinelOptions.TargetDelay = -1
	conf.Options.IncrSyncTargetDelay = 0

	// 1. input is nil
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = nil
		batcher.utBatchesDelay.delay = 0

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 0, len(ret), "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 2. normal case: input array is not empty
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = time.Now().Unix() + 1000000

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, int64(0), getTargetDelay(), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	return
	// 3. delay == 1s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = 0

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 1

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 4. delay == 10s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.ExitPoint = nowTs + 5
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 6, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay == 0, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}

	// 5. delay == 10s, but before fullSyncFinishPosition
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = bson.MongoTimestamp(time.Now().Unix() + 100) << 32

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.ExitPoint = nowTs + 5
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 6. no delay, exit at middle
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = nowTs + 5

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 0

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 6, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}

	// 7. delay == 60s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 60
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = nowTs + 50

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay > 11, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 8. delay == 1s, no delay
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 1
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = -1

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 9. delay == 60s, exit at an old time
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = 1

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 60
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = nowTs - 1000

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 0, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay == 0, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}
}