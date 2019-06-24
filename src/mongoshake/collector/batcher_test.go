package collector

import (
	"fmt"
	"github.com/vinllen/mgo/bson"
	"testing"

	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
)

func mockSyncer() *OplogSyncer {
	length := 3
	syncer := &OplogSyncer{
		logsQueue: make([]chan []*oplog.GenericOplog, length),
		hasher:    &oplog.PrimaryKeyHasher{},
	}
	for i := 0; i < length; i++ {
		syncer.logsQueue[i] = make(chan []*oplog.GenericOplog, 100)
	}
	return syncer
}

// return oplogs array with length=input length, the ddlGiven array marks the ddl
func mockOplogs(length int, ddlGiven []int) []*oplog.GenericOplog {
	output := make([]*oplog.GenericOplog, length)
	j := 0
	for i := 0; i < length; i ++ {
		op := "u"
		if j < len(ddlGiven) && ddlGiven[j] == i {
			op = "c"
			j++
		}
		output[i] = &oplog.GenericOplog {
			Parsed: &oplog.PartialLog{
				Namespace: "a.b",
				Operation: op,
			},
		}
	}
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

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = true

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 18, len(batchedOplog[0]), "should be equal")
	}

	// split by `conf.Options.AdaptiveBatchingMaxSize`
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 10
		conf.Options.ReplayerDMLOnly = true

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
	}

	// has ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2})
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
	}

	// has several ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{3})
		syncer.logsQueue[1] <- mockOplogs(6, []int{2})
		syncer.logsQueue[2] <- mockOplogs(7, []int{4, 5})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")

		// 3 in logsQ[0]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")

		// 2 in logsQ[1]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")

		// 4 in logsQ[2]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		// 5 in logsQ[2]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
	}

	// first one and last one are ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{0})
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, []int{6})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 16, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil)

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(1), batcher.currentQueue(), "should be equal")
	}

	// all ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(3, []int{0, 1, 2})
		syncer.logsQueue[1] <- mockOplogs(1, []int{0})
		syncer.logsQueue[2] <- mockOplogs(1, []int{0})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil)

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(1), batcher.currentQueue(), "should be equal")
	}

	// the edge of `AdaptiveBatchingMaxSize` is ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 8
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, []int{5}) // last is ddl
		syncer.logsQueue[2] <- mockOplogs(7, []int{3})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(2), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(2), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
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
		syncer:      &OplogSyncer{
			fullSyncFinishPosition: 0,
		},
		filterList:  filterList,
	}
}

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

	{
		fmt.Printf("TestFilterPartialLog case %d.\n", nr)
		nr++
		batcher := mockBatcher([]string{"fdb1.fcol1"}, []string{})
		log := mockFilterPartialLog("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "fdb1.fcol1"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "fdb2"},
							bson.DocElem{"$id", "id2"},
						}},
					},
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "fdb1.fcol2"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "fdb2"},
							bson.DocElem{"$id", "id2"},
						}},
					},
				},
			},
		})
		assert.Equal(t, false, filterPartialLog(log, batcher), "should be equal")
		assert.Equal(t, mockFilterPartialLog("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "fdb1.fcol1"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "fdb2"},
							bson.DocElem{"$id", "id2"},
						}},
					},
				},
			},
		}), log, "should be equal")
	}

	{
		fmt.Printf("TestFilterPartialLog case %d.\n", nr)
		nr++
		batcher := mockBatcher([]string{"fdb2"}, []string{})
		log := mockFilterPartialLog("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "fdb1.fcol1"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "fdb2"},
							bson.DocElem{"$id", "id2"},
						}},
					},
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "fdb1.fcol2"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "fdb2"},
							bson.DocElem{"$id", "id2"},
						}},
					},
				},
			},
		})
		assert.Equal(t, true, filterPartialLog(log, batcher), "should be equal")
		assert.Equal(t, mockFilterPartialLog("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D(nil),
			},
		}), log, "should be equal")
	}
}