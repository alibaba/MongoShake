package sourceReader

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Use a dedicated hidden secondary with replication paused after seeding oplog
// entries, then advance the primary. Discovery excludes the hidden member, so
// the old secondaryPreferred check deterministically reads the wrong bounds.
func TestGetAllTimestampStandaloneMatchesReaderLive(t *testing.T) {
	uri := os.Getenv("MONGOSHAKE_TIMESTAMP_HIDDEN_SOURCE")
	if uri == "" {
		t.Skip("requires a dedicated, seeded, paused hidden secondary")
	}
	u, err := url.Parse(uri)
	require.NoError(t, err)
	ip := net.ParseIP(u.Hostname())
	require.True(t, ip != nil && ip.IsLoopback(), "only dedicated loopback nodes are allowed")
	require.Empty(t, u.User)
	require.NotContains(t, u.Host, ",")
	oldOptions, oldLogger := conf.Options, l.Logger
	t.Cleanup(func() { conf.Options, l.Logger = oldOptions, oldLogger })
	conf.Options.MongoConnectMode = utils.VarMongoConnectModeStandalone
	conf.Options.IncrSyncReaderFetchBatchSize = 32
	conf.Options.MongoSslRootCaFile = ""
	dir := t.TempDir()
	require.NoError(t, l.New("info", dir, "timestamp.log", true, 20, 1, 0))

	for _, direct := range []string{"", "false", "true"} {
		t.Run("direct_"+direct, func(t *testing.T) {
			q := u.Query()
			q.Del("directConnection")
			if direct != "" {
				q.Set("directConnection", direct)
			}
			u.RawQuery = q.Encode()
			source := &utils.MongoSource{URL: u.String(), ReplicaName: "timestamp_source"}
			// This is the same direct client that the real reader constructs.
			r := NewOplogReader(source.URL, source.ReplicaName)
			conn, err := utils.NewMongoCommunityConn(source.URL, conf.Options.MongoConnectMode, true, "", "", "")
			require.NoError(t, err)
			defer conn.Close()
			var hello struct {
				Secondary bool `bson:"secondary"`
				Hidden    bool `bson:"hidden"`
			}
			require.NoError(t, conn.Client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello))
			require.True(t, hello.Secondary && hello.Hidden, "fixture must be a hidden secondary")
			oldest, err := utils.GetOldestTimestampByConn(conn)
			require.NoError(t, err)
			newest, err := utils.GetNewestTimestampByConn(conn)
			require.NoError(t, err)
			require.Greater(t, newest, oldest, "fixture needs seeded oplog entries")
			bounds, maxNew, minNew, maxOld, minOld, err := utils.GetAllTimestamp([]*utils.MongoSource{source}, conf.Options.MongoConnectMode, "")
			require.NoError(t, err)
			require.Equal(t, utils.TimestampNode{Oldest: oldest, Newest: newest}, bounds[source.ReplicaName], "timestamp check must use reader's hidden node, not a discovered member")
			require.Equal(t, []int64{newest, newest, oldest, oldest}, []int64{maxNew, minNew, maxOld, minOld})
			r.SetQueryTimestampOnEmpty(oldest)
			require.NoError(t, r.EnsureNetwork())
			defer r.conn.Close()
			defer r.releaseCursor()
			require.Equal(t, oldest, r.getOldestTimestamp())
			require.Equal(t, newest, r.getNewestTimestamp())
			require.True(t, r.oplogsCursor.Next(context.Background()))
			want, err := conn.Client.Database("local").Collection(utils.OplogNS).FindOne(context.Background(),
				bson.M{"ts": bson.M{"$gt": utils.Int64ToTimestamp(oldest)}}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}})).DecodeBytes()
			require.NoError(t, err)
			require.Equal(t, []byte(want), []byte(r.oplogsCursor.Current), "real incremental cursor must return the same complete BSON entry")
		})
	}
	data, err := os.ReadFile(filepath.Join(dir, "timestamp.log"))
	require.NoError(t, err)
	var reads int
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "Read oplog timestamp") {
			reads++
			require.Contains(t, line, "actualConnection["+u.Host+"[")
			require.Contains(t, line, "connectMode[standalone]")
			t.Log(line)
		}
	}
	require.Equal(t, 6, reads, "each successful oldest/newest read must identify its actual connection")
}
