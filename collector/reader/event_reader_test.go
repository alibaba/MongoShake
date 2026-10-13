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

	// A database and collection no other package uses. The reader watches the whole
	// cluster, so everybody else's namespaces are visible to it and the filter below
	// has to be able to tell this test's own events apart from theirs.
	testDB   = "ut_event_reader_db"
	testColl = "ut_event_reader_coll"

	// The reader loop only stops once it has seen every insert, so without a deadline
	// anything that keeps those events from arriving hangs the whole package. Observed
	// runtime is 27-33s on an idle server; this is loose enough to survive a parallel
	// go test ./... and still an order of magnitude short of the package timeout.
	testReadDeadline = 180 * time.Second
)

// eventReaderOwnNamespace reports whether a change stream event belongs to this test.
// The watch is cluster-wide, so every other database on the server is visible and has to
// be skipped by hand. Both halves must match: testing either one alone lets a
// same-named collection in somebody else's database through.
func eventReaderOwnNamespace(val *oplog.Event) bool {
	return val.Ns["db"] == testDB && val.Ns["coll"] == testColl
}

func TestEventReaderOwnNamespace(t *testing.T) {
	cases := []struct {
		name string
		ns   bson.M
		want bool
	}{
		{"this test's own namespace", bson.M{"db": testDB, "coll": testColl}, true},
		{"same collection name in another database", bson.M{"db": "somewhere_else", "coll": testColl}, false},
		{"another collection in this database", bson.M{"db": testDB, "coll": "c1"}, false},
		{"unrelated namespace", bson.M{"db": "db1", "coll": "c1"}, false},
		{"the shared test database", bson.M{"db": "test", "coll": "test"}, false},
		{"missing ns entirely", bson.M{}, false},
	}

	for _, c := range cases {
		assert.Equal(t, c.want, eventReaderOwnNamespace(&oplog.Event{Ns: c.ns}), c.name)
	}
}

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
		defer conn.Close()

		// Drop only this test's own database. Every package in the repo points at the
		// same server, so dropping all of them deleted other packages' fixtures while
		// they were mid-test.
		assert.Equal(t, nil, conn.Client.Database(testDB).Drop(nil), "should be equal")

		// test one database. Inserted before the reader starts, so the stream -- which
		// begins at the current cluster time -- never sees it.
		_, err = conn.Client.Database(testDB).Collection(testColl).InsertOne(nil, bson.M{"yy": 1})
		assert.Equal(t, nil, err, "should be equal")

		conf.Options.IncrSyncReaderFetchBatchSize = 8192
		// incr_sync.reader.buffer_time is in neither the conf template nor
		// DefaultConfiguration(), so it is 0 here and get()'s time.After(0) is
		// immediately ready. select then picks between a buffered event and the
		// timeout at random, which made each iteration a coin flip and cost this
		// test 113s under parallel load instead of 33s on its own.
		conf.Options.IncrSyncReaderBufferTime = 1
		er := NewEventReader(testMongoAddressCs, "ut_event_reader")
		er.StartFetcher()
		time.Sleep(3 * time.Second) // wait fetcher start

		flag := false
		startIndex := int32(0)
		deadline := time.Now().Add(testReadDeadline)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if time.Now().After(deadline) {
					t.Errorf("saw x up to %d, want %d inserts within %v",
						startIndex, testInsertTimes, testReadDeadline)
					return
				}

				in, err := er.Next()
				if err != nil && err.Error() == TimeoutError.Error() {
					// Next() already blocked for incr_sync.reader.buffer_time, so
					// there is nothing to wait for here.
					continue
				}
				assert.Equal(t, nil, err, "should be equal")

				var val oplog.Event
				err = bson.Unmarshal(in, &val)
				if err != nil {
					fmt.Printf("unmarshal error: %v", err)
				}
				assert.Equal(t, nil, err, "should be equal")

				if !eventReaderOwnNamespace(&val) {
					continue
				}

				m, _ := oplog.ConvertBsonD2M(val.FullDocument)
				x, ok := m["x"].(int32)
				if !ok {
					continue
				}

				if !flag {
					flag = true
					startIndex = x
					if startIndex < 1 {
						t.Errorf("first observed x is %d, want at least 1", startIndex)
						return
					}
				}

				assert.Equal(t, startIndex, x, "should be equal")
				startIndex++
				if startIndex > testInsertTimes {
					break
				}
			}
		}()

		time.Sleep(10 * time.Second)
		for i := 1; i <= testInsertTimes; i++ {
			_, err = conn.Client.Database(testDB).Collection(testColl).InsertOne(nil, bson.M{"x": i})
			assert.Equal(t, nil, err, "should be equal")
		}
		wg.Wait()

		// The old break condition stopped after the ninth insert, so the tenth was
		// never checked and the test passed on a short stream.
		assert.Equal(t, int32(testInsertTimes)+1, startIndex, "should be equal")
	}
}
