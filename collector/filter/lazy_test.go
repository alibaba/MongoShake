package filter

import (
	"testing"

	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestLazyLegacyIndexFiltering(t *testing.T) {
	for _, ns := range []string{"source.mysystem.indexes", "source.foo.system.indexes", "source.system.indexes"} {
		raw, err := bson.Marshal(bson.D{{"op", "i"}, {"ns", ns}, {"o", bson.D{{"_id", 1}}}})
		require.NoError(t, err)
		log, err := oplog.ParseRaw(raw)
		require.NoError(t, err)
		require.NotPanics(t, func() { require.False(t, NewNamespaceFilter(nil, nil).Filter(log)) })
		require.Equal(t, ns, log.Namespace)
		require.Equal(t, ns == "source.system.indexes", (&DDLFilter{}).Filter(log))
	}
	raw, err := bson.Marshal(bson.D{{"op", "i"}, {"ns", "source.system.indexes"}, {"o", bson.D{{"ns", "source.blocked"}}}})
	require.NoError(t, err)
	log, err := oplog.ParseRaw(raw)
	require.NoError(t, err)
	require.True(t, NewNamespaceFilter(nil, []string{"source.blocked"}).Filter(log))
	require.Equal(t, "source.system.indexes", log.Namespace)
}
