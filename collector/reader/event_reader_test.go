package sourceReader

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"time"

	"github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"

	"github.com/stretchr/testify/assert"
)

const (
	testMongoAddressCs = unit_test_common.TestUrlServerlessTenant
	//testMongoAddressCs = "mongodb://tenant1:password@100.81.164.181:25230/admin"
	//testMongoAddressCs = "mongodb://100.81.164.181:36203"
)

func TestEventReader(t *testing.T) {
	// test EventReader

	utils.InitialLogger("", "", "all", true, 1)

	var nr int
	// TODO(jianyou) deprecate serverless（serverless  only watch tables that already exits, new create table(like dbxxx) not watch）
	// normal: test 2 db
	//{
	//	fmt.Printf("TestEventReader case %d.\n", nr)
	//	nr++
	//
	//	cnt := 30
	//	conn, err := utils.NewMongoCommunityConn(testMongoAddressCs, "primary", true,
	//		"", "", "")
	//	assert.Equal(t, nil, err, "should be equal")
	//	err = conn.Client.Database("db1").Drop(nil)
	//	assert.Equal(t, nil, err, "should be equal")
	//	err = conn.Client.Database("db2").Drop(nil)
	//	assert.Equal(t, nil, err, "should be equal")
	//	err = conn.Client.Database("dbxxx").Drop(nil)
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	// test two databases
	//	_, err = conn.Client.Database("db1").Collection("c1").InsertOne(nil, bson.M{"yy": 1})
	//	assert.Equal(t, nil, err, "should be equal")
	//	_, err = conn.Client.Database("db2").Collection("c1").InsertOne(nil, bson.M{"yy": 1})
	//	assert.Equal(t, nil, err, "should be equal")
	//
	//	er := NewEventReader(testMongoAddressCs, "ut_event_reader")
	//	conf.Options.SpecialSourceDBFlag = utils.VarSpecialSourceDBFlagAliyunServerless
	//	er.StartFetcher()
	//	time.Sleep(5 * time.Second) // wait fetcher start
	//
	//	flag := false
	//	startIndex := int32(0)
	//	var wg sync.WaitGroup
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		for {
	//			in, err := er.Next()
	//			if err == TimeoutError {
	//				time.Sleep(2 * time.Second)
	//				if er.client != nil {
	//					fmt.Printf("timeout, resumeToken: %v\n", er.client.ResumeToken())
	//				} else {
	//					fmt.Printf("timeout, er.client == nil\n")
	//				}
	//
	//				continue
	//			}
	//			assert.Equal(t, nil, err, "should be equal")
	//
	//			var val oplog.Event
	//			err = bson.Unmarshal(in, &val)
	//			//fmt.Printf("%v\n", val)
	//			assert.Equal(t, nil, err, "should be equal")
	//
	//			if !flag {
	//				if !((val.Ns["db"] == "db1" || val.Ns["db"] == "db2") && val.Ns["coll"] == "c1") {
	//					fmt.Printf("timeout because of unexpect ns[%v] event[%v]\n", val.Ns, val)
	//					time.Sleep(2 * time.Second)
	//					continue
	//				} else {
	//					flag = true
	//					m, _ := oplog.ConvertBsonD2M(val.FullDocument)
	//					startIndex = m["x"].(int32)
	//					fmt.Printf("startIndex:%v\n", startIndex)
	//					if startIndex < 1 {
	//						fmt.Printf("timeout because of unexpect x[%v] fullDocument[%v]\n", startIndex, m)
	//						time.Sleep(2 * time.Second)
	//						continue
	//					}
	//				}
	//			}
	//
	//			fmt.Printf("%v\n", val)
	//
	//			m, _ := oplog.ConvertBsonD2M(val.FullDocument)
	//			innerVal := m["x"].(int32)
	//			assert.Equal(t, startIndex, innerVal, "should be equal")
	//			fmt.Printf("match x[%v] value:%v\n", startIndex, innerVal)
	//			startIndex++
	//			if startIndex >= int32(cnt/3*2) {
	//				break
	//			}
	//		}
	//	}()
	//
	//	time.Sleep(10 * time.Second)
	//	nr := 0
	//	for i := 1; i <= cnt; i++ {
	//		var db string
	//		switch i % 3 {
	//		case 0:
	//			db = "dbxxx"
	//		case 1:
	//			db = "db1"
	//		case 2:
	//			db = "db2"
	//		}
	//		if db == "db1" || db == "db2" {
	//			nr++
	//			_, err = conn.Client.Database(db).Collection("c1").InsertOne(nil, bson.M{"x": nr})
	//			assert.Equal(t, nil, err, "should be equal")
	//		} else {
	//			_, err = conn.Client.Database(db).Collection("c1").InsertOne(nil, bson.M{"x": -1})
	//			assert.Equal(t, nil, err, "should be equal")
	//		}
	//	}
	//	wg.Wait()
	//}

	// normal: test 1 db
	{
		fmt.Printf("TestEventReader case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddressCs, "primary", true, "", "", "")

		// drop all databases
		dbs, err := conn.Client.ListDatabaseNames(nil, bson.M{})
		assert.Equal(t, nil, err, "should be equal")
		for _, db := range dbs {
			if db != "admin" && db != "local" && db != "config" {
				err = conn.Client.Database(db).Drop(nil)
				assert.Equal(t, nil, err, "should be equal")
			}
		}

		// test one database
		_, err = conn.Client.Database("db1").Collection("c1").InsertOne(nil, bson.M{"yy": 1})
		assert.Equal(t, nil, err, "should be equal")

		er := NewEventReader(testMongoAddressCs, "ut_event_reader")
		er.StartFetcher()
		time.Sleep(3 * time.Second) // wait fetcher start

		flag := false
		startIndex := int32(0)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				in, err := er.Next()
				if err == TimeoutError {
					time.Sleep(2 * time.Second)
					fmt.Printf("timeout, resumeToken: %v\n", er.client.ResumeToken())
					continue
				}
				assert.Equal(t, nil, err, "should be equal")

				var val oplog.Event
				err = bson.Unmarshal(in, &val)
				fmt.Printf("%v\n", val)
				assert.Equal(t, nil, err, "should be equal")

				if !flag {
					if val.Ns["db"] != "db1" && val.Ns["coll"] != "c1" {
						fmt.Printf("timeout because of unexpect ns[%v] event[%v]\n", val.Ns, val)
						time.Sleep(2 * time.Second)
						continue
					} else {
						flag = true
						m, _ := oplog.ConvertBsonD2M(val.FullDocument)
						startIndex = m["x"].(int32)
						if startIndex < 1 {
							fmt.Printf("timeout because of unexpect x[%v] fullDocument[%v]\n", startIndex, m)
							time.Sleep(2 * time.Second)
							continue
						}
					}
				}

				m, _ := oplog.ConvertBsonD2M(val.FullDocument)
				innerVal := m["x"].(int32)
				assert.Equal(t, startIndex, innerVal, "should be equal")
				fmt.Printf("match x[%v]\n", startIndex)
				startIndex++
				if startIndex >= 100 {
					break
				}
			}
		}()

		time.Sleep(10 * time.Second)
		for i := 1; i <= 100; i++ {
			db := "db1"
			_, err = conn.Client.Database(db).Collection("c1").InsertOne(nil, bson.M{"x": i})
			assert.Equal(t, nil, err, "should be equal")
		}
		wg.Wait()
	}
}
