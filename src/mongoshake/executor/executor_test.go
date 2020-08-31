package executor

import (
	"fmt"
	"testing"

	"mongoshake/collector/transform"
	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"sync"
)

func mockLogs(op, ns string, size int, cb bool) *OplogRecord {
	callback := func() {}
	if !cb {
		callback = nil
	}

	return &OplogRecord{
		original: &PartialLogWithCallbak{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: ns,
					Operation: op,
				},
				RawSize:   size,
			},
			callback: nil,
		},
		wait: callback,
	}
}

func TestMergeToGroups(t *testing.T) {
	// test mergeToGroups

	var nr int
	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 1, len(groups), "should be equal")
		assert.Equal(t, 4, len(groups[0].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   3,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 2, len(groups), "should be equal")
		assert.Equal(t, 3, len(groups[0].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[1].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 5*1024*1024, false),
			mockLogs("op1", "ns1", 7*1024*1024, false),
			mockLogs("op1", "ns1", 8*1024*1024, false),
			mockLogs("op1", "ns1", 1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 2, len(groups), "should be equal")
		assert.Equal(t, 2, len(groups[0].oplogRecords), "should be equal")
		assert.Equal(t, 2, len(groups[1].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns1", 13*1024*1024, false),
			mockLogs("op1", "ns1", 8*1024*1024, false),
			mockLogs("op1", "ns1", 1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 3, len(groups), "should be equal")
		assert.Equal(t, 1, len(groups[0].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[1].oplogRecords), "should be equal")
		assert.Equal(t, 2, len(groups[2].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns2", 13*1024*1024, false),
			mockLogs("op1", "ns1", 8*1024*1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 7*1024*1024, false),
			mockLogs("op1", "ns1", 1*1024*1024, false),
			mockLogs("op1", "ns1", 1, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 4, len(groups), "should be equal")
		assert.Equal(t, 1, len(groups[0].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[1].oplogRecords), "should be equal")
		assert.Equal(t, 3, len(groups[2].oplogRecords), "should be equal")
		assert.Equal(t, 2, len(groups[3].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns2", 13*1024*1024, false),
			mockLogs("op1", "ns1", 8*1024*1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 7*1024*1024, false),
			mockLogs("op1", "ns3", 1*1024*1024, false),
			mockLogs("op1", "ns1", 1, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 5, len(groups), "should be equal")
		assert.Equal(t, 1, len(groups[0].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[1].oplogRecords), "should be equal")
		assert.Equal(t, 3, len(groups[2].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[3].oplogRecords), "should be equal")
		assert.Equal(t, 1, len(groups[4].oplogRecords), "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		combiner := LogsGroupCombiner{
			maxGroupNr:   10,
			maxGroupSize: 12 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns2", 16*1024*1024, false),
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns1", 16*1024*1024, false),
			mockLogs("op1", "ns3", 16*1024*1024, false),
			mockLogs("op1", "ns1", 16*1024*1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 7, len(groups), "should be equal")
	}
}

func mockTransLogs(op, ns string, logObject bson.D) *OplogRecord {
	return &OplogRecord{
		original: &PartialLogWithCallbak{
			partialLog: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: ns,
					Operation: op,
					Object:    logObject,
				},
				RawSize:   1,
			},
			callback: nil,
		},
		wait: nil,
	}
}

func TestTransformLog(t *testing.T) {
	// test TestTransformLog

	var nr int
	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1:fdb2"})

		logs := []*OplogRecord{
			mockTransLogs("i", "fdb1.tc1", bson.D{bson.DocElem{"a", 1}}),
		}
		logs = transformLogs(logs, nsTrans, false)
		assert.Equal(t, mockTransLogs("i", "fdb2.tc1", bson.D{bson.DocElem{"a", 1}}), logs[0], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1:tdb1"})

		logs := []*OplogRecord{
			mockTransLogs("i", "fdb1.fcol1", bson.D{bson.DocElem{"a", 1}}),
			mockTransLogs("i", "fdb2.fcol2", bson.D{
				bson.DocElem{"a", 1},
				bson.DocElem{"b", bson.D{
					bson.DocElem{"$ref", "fcol1"},
					bson.DocElem{"$id", "id1"},
					bson.DocElem{"$db", "fdb1"}}}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{
				bson.DocElem{"create", "fcol1"},
				bson.DocElem{"idIndex", bson.D{
					bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
					bson.DocElem{"ns", "fdb1.fcol1"},
				}},
			}),
		}
		logs = transformLogs(logs, nsTrans, false)
		assert.Equal(t, mockTransLogs("i", "tdb1.fcol1", bson.D{bson.DocElem{"a", 1}}), logs[0], "should be equal")
		assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.D{
			bson.DocElem{"a", 1},
			bson.DocElem{"b", bson.D{
				bson.DocElem{"$ref", "fcol1"},
				bson.DocElem{"$id", "id1"},
				bson.DocElem{"$db", "fdb1"},
			}},
		}), logs[1], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}), logs[2], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.fcol1", bson.D{
			bson.DocElem{"create", "fcol1"},
			bson.DocElem{"idIndex", bson.D{
				bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
				bson.DocElem{"ns", "tdb1.fcol1"},
			}},
		}), logs[3], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1:tdb1"})

		logs := []*OplogRecord{
			mockTransLogs("i", "fdb1.fcol1", bson.D{bson.DocElem{"a", 1}}),
			mockTransLogs("i", "fdb2.fcol2", bson.D{
				bson.DocElem{"a", 1},
				bson.DocElem{"b", bson.D{
					bson.DocElem{"$ref", "fcol1"},
					bson.DocElem{"$id", "id1"},
					bson.DocElem{"$db", "fdb1"}}}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{
				bson.DocElem{"create", "fcol1"},
				bson.DocElem{"idIndex", bson.D{
					bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
					bson.DocElem{"ns", "fdb1.fcol1"},
				}},
			}),
		}
		logs = transformLogs(logs, nsTrans, true)
		assert.Equal(t, mockTransLogs("i", "tdb1.fcol1", bson.D{bson.DocElem{"a", 1}}), logs[0], "should be equal")
		assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.D{
			bson.DocElem{"a", 1},
			bson.DocElem{"b", bson.D{
				bson.DocElem{"$ref", "fcol1"},
				bson.DocElem{"$id", "id1"},
				bson.DocElem{"$db", "tdb1"},
			}},
		}), logs[1], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}), logs[2], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.fcol1", bson.D{
			bson.DocElem{"create", "fcol1"},
			bson.DocElem{"idIndex", bson.D{
				bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
				bson.DocElem{"ns", "tdb1.fcol1"},
			}},
		}), logs[3], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1.fcol1:tdb1.tcol1", "fdb1:tdb2"})

		logs := []*OplogRecord{
			mockTransLogs("i", "fdb1.fcol1", bson.D{bson.DocElem{"a", 1}}),
			mockTransLogs("i", "fdb2.fcol2", bson.D{
				bson.DocElem{"a", 1},
				bson.DocElem{"b", bson.D{
					bson.DocElem{"$ref", "fcol1"},
					bson.DocElem{"$id", "id1"},
					bson.DocElem{"$db", "fdb1"}}}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}),
			mockTransLogs("c", "fdb1.$cmd", bson.D{
				bson.DocElem{"create", "fcol1"},
				bson.DocElem{"idIndex", bson.D{
					bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
					bson.DocElem{"ns", "fdb1.fcol1"}}}}),
		}
		logs = transformLogs(logs, nsTrans, true)
		assert.Equal(t, mockTransLogs("i", "tdb1.tcol1", bson.D{bson.DocElem{"a", 1}}), logs[0], "should be equal")
		assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.D{
			bson.DocElem{"a", 1},
			bson.DocElem{"b", bson.D{
				bson.DocElem{"$ref", "tcol1"},
				bson.DocElem{"$id", "id1"},
				bson.DocElem{"$db", "tdb1"},
			}},
		}), logs[1], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb2.$cmd", bson.D{bson.DocElem{"dropDatabase", 1}}), logs[2], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.tcol1", bson.D{
			bson.DocElem{"create", "tcol1"},
			bson.DocElem{"idIndex", bson.D{
				bson.DocElem{"key", bson.D{bson.DocElem{"a", 1}}},
				bson.DocElem{"ns", "tdb1.tcol1"},
			}},
		}), logs[3], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1.fcol1:tdb1.tcol1", "fdb1:tdb2"})

		logs := []*OplogRecord{
			mockTransLogs("c", "admin.$cmd", bson.D{
				bson.DocElem{
					Name: "applyOps",
					Value: []bson.D{
						{
							bson.DocElem{"op", "i"},
							bson.DocElem{"ns", "fdb1.fcol1"},
							bson.DocElem{"o", bson.D{bson.DocElem{"a", 1}}},
						},
						{
							bson.DocElem{"op", "i"},
							bson.DocElem{"ns", "fdb1.fcol2"},
							bson.DocElem{"o", bson.D{bson.DocElem{"a", 1}}},
						},
					},
				},
			}),
			mockTransLogs("c", "admin.$cmd", bson.D{
				bson.DocElem{
					Name: "applyOps",
					Value: []bson.D{
						{
							bson.DocElem{"op", "i"},
							bson.DocElem{"ns", "fdb1.fcol1"},
							bson.DocElem{"o", bson.D{bson.DocElem{"b", 1}}},
						},
						{
							bson.DocElem{"op", "i"},
							bson.DocElem{"ns", "fdb1.fcol2"},
							bson.DocElem{"o", bson.D{
								bson.DocElem{"$ref", "fcol1"},
								bson.DocElem{"$id", "id1"},
								bson.DocElem{"$db", "fdb1"},
							}},
						},
					},
				},
			}),
		}

		// fmt.Println(logs[0].original.partialLog)
		logs = transformLogs(logs, nsTrans, true)
		// fmt.Println(logs[0].original.partialLog)
		assert.Equal(t, mockTransLogs("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "tdb1.tcol1"},
						bson.DocElem{"o", bson.D{bson.DocElem{"a", 1}}},
					},
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "tdb2.fcol2"},
						bson.DocElem{"o", bson.D{bson.DocElem{"a", 1}}},
					},
				},
			},
		}), logs[0], "should be equal")
		assert.Equal(t, mockTransLogs("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "tdb1.tcol1"},
						bson.DocElem{"o", bson.D{bson.DocElem{"b", 1}}},
					},
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "tdb2.fcol2"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "tcol1"},
							bson.DocElem{"$id", "id1"},
							bson.DocElem{"$db", "tdb1"},
						}},
					},
				},
			},
		}), logs[1], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"fdb1.fcol1:tdb1.tcol1", "fdb2.fcol2:tdb2.tcol2"})

		logs := []*OplogRecord{
			mockTransLogs("c", "fdb1.$cmd", bson.D{
				bson.DocElem{"renameCollection", "fdb1.fcol1"},
				bson.DocElem{"to", "fdb2.fcol2"}}),
		}
		logs = transformLogs(logs, nsTrans, true)
		assert.Equal(t,
			mockTransLogs("c", "tdb1.tcol1", bson.D{
				bson.DocElem{"renameCollection", "tdb1.tcol1"},
				bson.DocElem{"to", "tdb2.tcol2"}}), logs[0], "should be equal")
	}

	{
		fmt.Printf("TestTransformLog case %d.\n", nr)
		nr++
		nsTrans := transform.NewNamespaceTransform([]string{"a.b:c.d", "a:fff"})

		logs := []*OplogRecord{
			mockTransLogs("c", "admin.$cmd", bson.D{
				bson.DocElem{
					Name: "applyOps",
					Value: []bson.D{
						{
							bson.DocElem{"op", "i"},
							bson.DocElem{"ns", "a.b"},
							bson.DocElem{"o", bson.D{
								bson.DocElem{"$ref", "e"},
								bson.DocElem{"$id", "id1"},
							}},
						},
					},
				},
			}),
		}

		logs = transformLogs(logs, nsTrans, true)
		assert.Equal(t, mockTransLogs("c", "admin.$cmd", bson.D{
			bson.DocElem{
				Name: "applyOps",
				Value: []bson.D{
					{
						bson.DocElem{"op", "i"},
						bson.DocElem{"ns", "c.d"},
						bson.DocElem{"o", bson.D{
							bson.DocElem{"$ref", "e"},
							bson.DocElem{"$id", "id1"},
							bson.DocElem{"$db", "fff"},
						}},
					},
				},
			},
		}), logs[0], "should be equal")
	}
}

func TestCalculateTop3(t *testing.T) {
	// test TestCalculateTop3

	var nr int
	{
		fmt.Printf("TestCalculateTop3 case %d.\n", nr)
		nr++

		var mp sync.Map

		ret := calculateTop3(mp)
		assert.Equal(t, 0, len(ret), "should be equal")
	}

	{
		fmt.Printf("TestCalculateTop3 case %d.\n", nr)
		nr++

		var mp sync.Map
		val1 := uint64(10)
		mp.Store("test1", &val1)

		ret := calculateTop3(mp)
		assert.Equal(t, []Item{
			{
				Key: "test1",
				Val: uint64(10),
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestCalculateTop3 case %d.\n", nr)
		nr++

		var mp sync.Map
		val1 := uint64(10)
		val2 := uint64(5)
		mp.Store("test1", &val1)
		mp.Store("test2", &val2)

		ret := calculateTop3(mp)
		assert.Equal(t, []Item{
			{
				Key: "test1",
				Val: uint64(10),
			},
			{
				Key: "test2",
				Val: uint64(5),
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestCalculateTop3 case %d.\n", nr)
		nr++

		var mp sync.Map
		val1 := uint64(10)
		val2 := uint64(5)
		val4 := uint64(20000)
		val5 := uint64(40000)
		mp.Store("test1", &val1)
		mp.Store("test2", &val2)
		mp.Store("test4", &val4)
		mp.Store("test5", &val5)


		ret := calculateTop3(mp)
		assert.Equal(t, []Item{
			{
				Key: "test5",
				Val: uint64(40000),
			},
			{
				Key: "test4",
				Val: uint64(20000),
			},
			{
				Key: "test1",
				Val: uint64(10),
			},
		}, ret, "should be equal")
	}
}