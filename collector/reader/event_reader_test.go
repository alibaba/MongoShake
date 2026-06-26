package sourceReader

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

var (
	testMongoAddressCs = unit_test_common.TestUrlServerlessTenant
)

const (
	testInsertTimes = 10
)

func TestEventReader(t *testing.T) {
	// test EventReader

	_ = utils.InitialLogger("", "", "all", true, 1)

	var nr int

	// normal: test 1 db
	{
		fmt.Printf("TestEventReader case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoCommunityConn(testMongoAddressCs, "primary", true, "", "", "")
		assert.NoError(t, err, "should be no error")

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

		conf.Options.IncrSyncReaderFetchBatchSize = 8192
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
				if err != nil && err.Error() == TimeoutError.Error() {
					time.Sleep(2 * time.Second)
					fmt.Printf("timeout, resumeToken: %v\n", er.client.ResumeToken())
					continue
				}
				assert.Equal(t, nil, err, "should be equal")

				var val oplog.Event
				err = bson.Unmarshal(in, &val)
				if err != nil {
					fmt.Printf("unmarshal error: %v", err)
				}
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
				if startIndex >= testInsertTimes {
					break
				}
			}
		}()

		time.Sleep(10 * time.Second)
		for i := 1; i <= testInsertTimes; i++ {
			db := "db1"
			_, err = conn.Client.Database(db).Collection("c1").InsertOne(nil, bson.M{"x": i})
			assert.Equal(t, nil, err, "should be equal")
		}
		wg.Wait()
	}
}
