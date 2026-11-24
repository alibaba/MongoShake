package sourceReader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestOplogReaderNameAndString(t *testing.T) {
	or := &OplogReader{src: "mongodb://u:p@127.0.0.1:27017", replset: "rs0"}
	assert.Equal(t, utils.VarIncrSyncMongoFetchMethodOplog, or.Name())
	s := or.String()
	assert.Contains(t, s, "rs0")
	assert.NotContains(t, s, ":p@", "password must be masked")
}

func TestOplogReaderQueryTimestamp(t *testing.T) {
	or := &OplogReader{query: bson.M{}}
	or.SetQueryTimestampOnEmpty(int64(1234567890))
	assert.Equal(t, int64(1234567890), or.getQueryTimestamp())

	// once set, SetQueryTimestampOnEmpty must not overwrite
	or.SetQueryTimestampOnEmpty(int64(999))
	assert.Equal(t, int64(1234567890), or.getQueryTimestamp())

	// explicit update does advance
	or.UpdateQueryTimestamp(42)
	assert.Equal(t, int64(42), or.getQueryTimestamp())
	assert.Equal(t, bson.M{QueryOpGT: utils.Int64ToTimestamp(42)}, or.query[QueryTs])
}

func TestOplogReaderFetchNewestTimestampNotImplemented(t *testing.T) {
	or := &OplogReader{}
	v, err := or.FetchNewestTimestamp()
	assert.Nil(t, v)
	require.Error(t, err)
}

func TestEventReaderNameAndQueryTimestamp(t *testing.T) {
	er := &EventReader{}
	assert.Equal(t, utils.VarIncrSyncMongoFetchMethodChangeStream, er.Name())

	er.SetQueryTimestampOnEmpty(int64(777))
	assert.Equal(t, int64(777), er.getQueryTimestamp())
	// already set: no overwrite
	er.SetQueryTimestampOnEmpty(int64(888))
	assert.Equal(t, int64(777), er.getQueryTimestamp())

	er.UpdateQueryTimestamp(999)
	assert.Equal(t, int64(999), er.getQueryTimestamp())

	// a non-int64 resume token is stored verbatim
	er2 := &EventReader{}
	token := bson.M{"_data": "8261"}
	er2.SetQueryTimestampOnEmpty(token)
	assert.Equal(t, token, er2.startAtOperationTime)
}
