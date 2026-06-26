package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

var (
	testMongoAddressCs = unit_test_common.TestUrlConfigServer
)

func TestChangeStreamConn(t *testing.T) {
	// test ChangeStreamConn

	_ = InitialLogger("", "", "info", true, 1)

	var nr int
	// normal
	{
		fmt.Printf("TestChangeStreamConn case %d.\n", nr)
		nr++

		cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			true,
			"",
			nil,
			nil,
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")
		optionStr := printCsOption(cs.Ops)
		assert.Equal(t, " BatchSize[1024] FullDocument[updateLookup] MaxAwaitTime[24h0m0s]", optionStr, "should be equal")

		cs.Close()
	}

	// StartAtOperationTime
	{
		fmt.Printf("TestChangeStreamConn case %d.\n", nr)
		nr++

		newest, err := GetNewestTimestampByUrl(testMongoAddressCs, false, "")
		tsStr := fmt.Sprintf("{%v %v}", ExtractMongoTimestamp(newest), ExtractMongoTimestampCounter(newest))

		cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			true,
			"",
			nil,
			newest,
			8192,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr := printCsOption(cs.Ops)
		expect := fmt.Sprintf(" BatchSize[8192] FullDocument[updateLookup] MaxAwaitTime[24h0m0s] StartAtOperationTime[%s]", tsStr)
		assert.Equal(t, expect, optionStr, "should be equal")

		cs.Close()
	}

	// StartAtOperationTime && StartAfter
	{
		fmt.Printf("TestChangeStreamConn case %d.\n", nr)
		nr++

		newest, err := GetNewestTimestampByUrl(testMongoAddressCs, false, "")
		tsStr := fmt.Sprintf("{%v %v}", ExtractMongoTimestamp(newest), ExtractMongoTimestampCounter(newest))

		cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			true,
			"",
			nil,
			newest,
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr := printCsOption(cs.Ops)
		expect := fmt.Sprintf(" BatchSize[1024] FullDocument[updateLookup] MaxAwaitTime[24h0m0s] StartAtOperationTime[%s]", tsStr)
		assert.Equal(t, expect, optionStr, "should be equal")

		// trigger update ResumeToken
		cs.TryNext()

		token := cs.ResumeToken()
		fmt.Printf("ResumeToken: %v\n", token)
		cs.Close()

		// create new one
		cs2, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			true,
			"",
			nil,
			token,
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr2 := printCsOption(cs2.Ops)
		expect2 := fmt.Sprintf(" BatchSize[1024] FullDocument[updateLookup] MaxAwaitTime[24h0m0s] StartAfter[%s]", token)
		assert.Equal(t, expect2, optionStr2, "should be equal")
	}
}
