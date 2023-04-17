package utils

import (
	"fmt"
	"testing"

	"github.com/alibaba/MongoShake/v2/unit_test_common"
	"github.com/stretchr/testify/assert"
)

const (
	testMongoAddressCs = unit_test_common.TestUrlConfigServer
)

func TestChangeStreamConn(t *testing.T) {
	// test ChangeStreamConn

	InitialLogger("", "", "info", true, 1)

	var nr int
	// normal
	{
		fmt.Printf("TestChangeStreamConn case %d.\n", nr)
		nr++

		cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			false,
			"",
			nil,
			nil,
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")
		optionStr := printCsOption(cs.Ops)
		assert.Equal(t, " BatchSize[1024] MaxAwaitTime[24h0m0s]", optionStr, "should be equal")

		cs.Close()
	}

	// StartAtOperationTime
	{
		fmt.Printf("TestChangeStreamConn case %d.\n", nr)
		nr++

		newest, err := GetNewestTimestampByUrl(testMongoAddressCs, false, "")
		tsStr := fmt.Sprintf("{%v %v}", ExtractMongoTimestamp(newest), ExtractMongoTimestampCounter(newest))

		cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			false,
			"",
			nil,
			int64(newest),
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr := printCsOption(cs.Ops)
		expect := fmt.Sprintf(" BatchSize[1024] MaxAwaitTime[24h0m0s] StartAtOperationTime[%s]", tsStr)
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
			false,
			"",
			nil,
			int64(newest),
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr := printCsOption(cs.Ops)
		expect := fmt.Sprintf(" BatchSize[1024] MaxAwaitTime[24h0m0s] StartAtOperationTime[%s]", tsStr)
		assert.Equal(t, expect, optionStr, "should be equal")

		// trigger update ResumeToken
		cs.TryNext()

		token := cs.ResumeToken()
		fmt.Printf("ResumeToken: %v\n", token)
		cs.Close()

		// create new one
		cs2, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
			false,
			"",
			nil,
			token,
			1024,
			"4.2.0",
			"")
		assert.Equal(t, nil, err, "should be equal")

		optionStr2 := printCsOption(cs2.Ops)
		expect2 := fmt.Sprintf(" BatchSize[1024] MaxAwaitTime[24h0m0s] StartAfter[%s]", token)
		assert.Equal(t, expect2, optionStr2, "should be equal")
	}

	// TODO(jianyou) deprecate AliyunServerless
	//{
	//	fmt.Printf("TestChangeStreamConn case %d.\n", nr)
	//	nr++
	//
	//	conn, err := NewMongoCommunityConn(testMongoAddressCs, VarMongoConnectModePrimary, true,
	//		ReadWriteConcernLocal, ReadWriteConcernDefault, "")
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	// drop all databases
	//	dbs, err := conn.Client.ListDatabaseNames(nil, bson.M{})
	//	assert.Equal(t, nil, err, "should be equal")
	//	for _, db := range dbs {
	//		if db == "admin" || db == "local" || db == "config" {
	//			continue
	//		}
	//
	//		err = conn.Client.Database(db).Drop(nil)
	//		assert.Equal(t, nil, err, "should be equal")
	//	}
	//	conn.Client.Database("db1").Collection("c1").InsertOne(nil, bson.M{"x": 1})
	//	conn.Client.Database("db1").Collection("c2").InsertOne(nil, bson.M{"x": 1})
	//	conn.Client.Database("db1").Collection("c3").InsertOne(nil, bson.M{"x": 1})
	//
	//	newest, err := GetNewestTimestampByUrl(testMongoAddressCs, false, "")
	//	tsStr := fmt.Sprintf("{%v %v}", ExtractMongoTimestamp(newest), ExtractMongoTimestampCounter(newest))
	//
	//	cs, err := NewChangeStreamConn(testMongoAddressCs, VarMongoConnectModePrimary,
	//		false,
	//		VarSpecialSourceDBFlagAliyunServerless,
	//		func(name string) bool {
	//			list := strings.Split(name, ".")
	//			if len(list) > 0 && (list[0] == "admin" || list[0] == "config" || list[0] == "local") {
	//				return true
	//			}
	//			return false
	//		},
	//		int64(newest),
	//		1024,
	//		"4.2.0",
	//		"")
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	optionStr := printCsOption(cs.Ops)
	//	expect := fmt.Sprintf(" BatchSize[1024] MaxAwaitTime[24h0m0s] StartAtOperationTime[%s] MultiDbSelections[(db1|db2)]", tsStr)
	//	assert.Equal(t, expect, optionStr, "should be equal")
	//	cs.Close()
	//}
}
