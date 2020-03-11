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
)

func mockSyncer() *OplogSyncer {
	length := 3
	syncer := &OplogSyncer{
		PendingQueue:           make([]chan [][]byte, 4),
		logsQueue:              make([]chan []*oplog.GenericOplog, length),
		hasher:                 &oplog.PrimaryKeyHasher{},
		fullSyncFinishPosition: -3, // disable in current test
		replMetric:             utils.NewMetric("test", 0),
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
func mockOplogs(length int, ddlGiven []int, noopGiven []int, sameTsGiven []int, startTs int) []*oplog.GenericOplog {
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
					Timestamp: bson.MongoTimestamp(startTs + i),
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
	// test batchMore

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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 17, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		syncer.logsQueue[0] <- mockOplogs(1, nil, nil, nil, 300)
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(300), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(300), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(104), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(105), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(206), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(101), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(102), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 9, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(206), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 15, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(2), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 3 in logsQ[0]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(3), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(101), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 2 in logsQ[1]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(102), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(203), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 4 in logsQ[2]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(204), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// 5 in logsQ[2]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(206), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 16, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(206), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 79, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(378), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(379), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(379), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(2), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(100), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(200), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 79, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(378), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(379), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")


		// test the last flush oplog
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
		assert.Equal(t, int64(379), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(104), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(105), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(202), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(203), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(205), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(206), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 4, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 13, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(3), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// inject more
		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, []int{1}, 300)
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(204), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(300), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(303), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, int64(304), int64(batcher.previousOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(5), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(1), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(7), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")
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
		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(3), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 5 in logsQ[0]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 0 in logsQ[1]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(100), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 1 in logsQ[1]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(101), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the end of logsQ[1]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, int64(101), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 0 in logsQ[2]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(200), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		// hit the 6 in logsQ[2]
		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, "c", batchedOplog[0][1].Parsed.Operation, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(203), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, int(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(4), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
		assert.Equal(t, fakeOplog, batcher.previousOplog, "should be equal")

		batchedOplog, barrier, allEmpty = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(5), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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

		batchedOplog, barrier, allEmpty := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, int64(0), int64(batcher.lastOplog.Parsed.Timestamp), "should be equal")
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
