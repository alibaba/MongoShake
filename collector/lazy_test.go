package collector

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestLazyOplogParserSwitch(t *testing.T) {
	for _, op := range []string{"i", "u", "d"} {
		raw, err := bson.Marshal(bson.D{{"op", op}, {"ns", "db.coll"},
			{"o", bson.D{{"_id", int64(42)}, {"nested", bson.D{{"value", "payload"}}}}},
			{"o2", bson.D{{"_id", int64(42)}}}})
		require.NoError(t, err)
		eager, err := newOplogParser(false)(raw)
		require.NoError(t, err)
		lazy, err := newOplogParser(true)(raw)
		require.NoError(t, err)
		require.NotNil(t, eager.Object)
		require.NotNil(t, eager.Query)
		require.Nil(t, lazy.Object)
		require.Nil(t, lazy.Query)
		require.Equal(t, oplog.GetIdOrNSFromOplog(eager), oplog.GetIdOrNSFromOplog(lazy))
		eagerBytes, err := bson.Marshal(eager)
		require.NoError(t, err)
		lazyBytes, err := bson.Marshal(lazy)
		require.NoError(t, err)
		require.Equal(t, eagerBytes, lazyBytes)
	}
}
