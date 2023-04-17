package collector

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/oplog"

	"github.com/stretchr/testify/assert"
)

func mockOplogsBinary() []byte {
	oplog := oplog.ParsedLog{
		Namespace: "a.b",
	}

	ret, err := bson.Marshal(&oplog)
	if err != nil {
		panic(err)
	}
	return ret
}

func TestInject(t *testing.T) {
	// test Inject

	var nr int
	// normal
	{
		fmt.Printf("TestInject case %d.\n", nr)
		nr++

		conf.Options.IncrSyncFetcherBufferCapacity = 5
		conf.Options.FullSyncReaderOplogStoreDisk = false

		syncer := mockSyncer()
		syncer.startDeserializer()
		persister := NewPersister("test-replica", syncer)

		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)
		persister.Inject(nil)
		persister.Inject(nil)
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)

		mergeBatch := <-syncer.logsQueue[0]
		assert.Equal(t, 5, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[1]
		assert.Equal(t, 2, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[2]
		assert.Equal(t, 1, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[3]
		assert.Equal(t, 1, len(mergeBatch), "should be equal")
	}
}
