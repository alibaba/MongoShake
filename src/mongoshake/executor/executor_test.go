package executor

import (
	"testing"

	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
	"fmt"
)

func mockLogs(op, ns string, size int, cb bool) *OplogRecord {
	callback := func(){}
	if !cb {
		callback = nil
	}

	return &OplogRecord{
		original: &PartialLogWithCallbak{
			partialLog: &oplog.PartialLog{
				Namespace: ns,
				Operation: op,
				RawSize: size,
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
			maxGroupNr: 10,
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
			maxGroupNr: 3,
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
			maxGroupNr: 10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 5 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 7 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 8 * 1024 * 1024, false),
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
			maxGroupNr: 10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 13 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 8 * 1024 * 1024, false),
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
			maxGroupNr: 10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns2", 13 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 8 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 7 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 1 * 1024 * 1024, false),
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
			maxGroupNr: 10,
			maxGroupSize: 16 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns2", 13 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 8 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 1024, false),
			mockLogs("op1", "ns1", 7 * 1024 * 1024, false),
			mockLogs("op1", "ns3", 1 * 1024 * 1024, false),
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
			maxGroupNr: 10,
			maxGroupSize: 12 * 1024 * 1024,
		}

		logs := []*OplogRecord{
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns2", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns3", 16 * 1024 * 1024, false),
			mockLogs("op1", "ns1", 16 * 1024 * 1024, false),
		}
		groups := combiner.mergeToGroups(logs)
		assert.Equal(t, 7, len(groups), "should be equal")
	}
}