package executor

import (
	"fmt"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/transform"
	"testing"

	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
)

func mockLogs(op, ns string, size int, cb bool) *OplogRecord {
	callback := func() {}
	if !cb {
		callback = nil
	}

	return &OplogRecord{
		original: &PartialLogWithCallbak{
			partialLog: &oplog.PartialLog{
				Namespace: ns,
				Operation: op,
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
				Namespace: ns,
				Operation: op,
				RawSize:   1,
				Object:    logObject,
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
		assert.Equal(t, mockTransLogs("i", "tdb1.fcol1", bson.M{"a": 1}), logs[0], "should be equal")
		assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.M{"a": 1, "b": bson.M{"$ref": "fcol1", "$id": "id1", "$db": "fdb1"}}), logs[1], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.$cmd", bson.M{"dropDatabase": 1}), logs[2], "should be equal")
		assert.Equal(t, mockTransLogs("c", "tdb1.fcol1", bson.M{"create": "fcol1", "idIndex": bson.M{"key": bson.M{"a": 1}, "ns": "tdb1.fcol1"}}), logs[3], "should be equal")
	}
	//{
	//	fmt.Printf("TestTransformLog case %d.\n", nr)
	//	nr++
	//	nsTrans := transform.NewNamespaceTransform([]string{"fdb1:tdb1"})
	//
	//	logs := []*OplogRecord{
	//		mockTransLogs("i", "fdb1.fcol1", bson.M{"a": 1}),
	//		mockTransLogs("i", "fdb2.fcol2", bson.M{"a": 1, "b": bson.M{"$ref": "fcol1", "$id": "id1", "$db": "fdb1"}}),
	//		mockTransLogs("c", "fdb1.$cmd", bson.M{"dropDatabase": 1}),
	//		mockTransLogs("c", "fdb1.$cmd", bson.M{"create": "fcol1", "idIndex": bson.M{"key": bson.M{"a": 1}, "ns": "fdb1.fcol1"}}),
	//	}
	//	logs = transformLogs(logs, nsTrans, true)
	//	assert.Equal(t, mockTransLogs("i", "tdb1.fcol1", bson.M{"a": 1}), logs[0], "should be equal")
	//	assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.M{"a": 1, "b": mgo.DBRef{Collection: "fcol1", Id: "id1", Database: "tdb1"}}), logs[1], "should be equal")
	//	assert.Equal(t, mockTransLogs("c", "tdb1.$cmd", bson.M{"dropDatabase": 1}), logs[2], "should be equal")
	//	assert.Equal(t, mockTransLogs("c", "tdb1.fcol1", bson.M{"create": "fcol1", "idIndex": bson.M{"key": bson.M{"a": 1}, "ns": "tdb1.fcol1"}}), logs[3], "should be equal")
	//}
	//{
	//	fmt.Printf("TestTransformLog case %d.\n", nr)
	//	nr++
	//	nsTrans := transform.NewNamespaceTransform([]string{"fdb1.fcol1:tdb1.tcol1", "fdb1:tdb2"})
	//
	//	logs := []*OplogRecord{
	//		mockTransLogs("i", "fdb1.fcol1", bson.M{"a": 1}),
	//		mockTransLogs("i", "fdb2.fcol2", bson.M{"a": 1, "b": bson.M{"$ref": "fcol1", "$id": "id1", "$db": "fdb1"}}),
	//		mockTransLogs("c", "fdb1.$cmd", bson.M{"dropDatabase": 1}),
	//		mockTransLogs("c", "fdb1.$cmd", bson.M{"create": "fcol1", "idIndex": bson.M{"key": bson.M{"a": 1}, "ns": "fdb1.fcol1"}}),
	//	}
	//	logs = transformLogs(logs, nsTrans, true)
	//	assert.Equal(t, mockTransLogs("i", "tdb1.tcol1", bson.M{"a": 1}), logs[0], "should be equal")
	//	assert.Equal(t, mockTransLogs("i", "fdb2.fcol2", bson.M{"a": 1, "b": mgo.DBRef{Collection: "tcol1", Id: "id1", Database: "tdb1"}}), logs[1], "should be equal")
	//	assert.Equal(t, mockTransLogs("c", "tdb2.$cmd", bson.M{"dropDatabase": 1}), logs[2], "should be equal")
	//	assert.Equal(t, mockTransLogs("c", "tdb1.tcol1", bson.M{"create": "tcol1", "idIndex": bson.M{"key": bson.M{"a": 1}, "ns": "tdb1.tcol1"}}), logs[3], "should be equal")
	//}
	//{
	//	fmt.Printf("TestTransformLog case %d.\n", nr)
	//	nr++
	//	nsTrans := transform.NewNamespaceTransform([]string{"fdb1.fcol1:tdb1.tcol1", "fdb1:tdb2"})
	//
	//	logs := []*OplogRecord{
	//		mockTransLogs("c", "admin.$cmd", bson.M{"applyOps": []interface{}{
	//			bson.M{"op": "i", "ns": "fdb1.fcol1", "o": bson.M{"a": 1}},
	//			bson.M{"op": "i", "ns": "fdb1.fcol2", "o": bson.M{"a": 1}}}}),
	//		mockTransLogs("c", "admin.$cmd", bson.M{"applyOps": []interface{}{
	//			bson.M{"op": "i", "ns": "fdb1.fcol1", "o": bson.M{"b": 1}},
	//			bson.M{"op": "i", "ns": "fdb1.fcol2", "o": bson.M{"b": bson.M{"$ref": "fcol1", "$id": "id1", "$db": "fdb1"}}}}}),
	//	}
	//	logs = transformLogs(logs, nsTrans, true)
	//	assert.Equal(t, mockTransLogs("c", "admin.$cmd", bson.M{"applyOps": []interface{}{
	//		bson.M{"op": "i", "ns": "tdb1.tcol1", "o": bson.M{"a": 1}, "ts": bson.MongoTimestamp(0), "g": "", "o2": bson.M(nil), "uk": bson.M(nil), "lsid": nil, "fromMigrate": false},
	//		bson.M{"op": "i", "ns": "tdb2.fcol2", "o": bson.M{"a": 1}, "ts": bson.MongoTimestamp(0), "g": "", "o2": bson.M(nil), "uk": bson.M(nil), "lsid": nil, "fromMigrate": false}}}), logs[0], "should be equal")
	//	assert.Equal(t, mockTransLogs("c", "admin.$cmd", bson.M{"applyOps": []interface{}{
	//		bson.M{"op": "i", "ns": "tdb1.tcol1", "o": bson.M{"b": 1}, "ts": bson.MongoTimestamp(0), "g": "", "o2": bson.M(nil), "uk": bson.M(nil), "lsid": nil, "fromMigrate": false},
	//		bson.M{"op": "i", "ns": "tdb2.fcol2", "o": bson.M{"b": mgo.DBRef{Collection: "tcol1", Id: "id1", Database: "tdb1"}}, "ts": bson.MongoTimestamp(0), "g": "", "o2": bson.M(nil), "uk": bson.M(nil), "lsid": nil, "fromMigrate": false}}}), logs[1], "should be equal")
	//}
}
