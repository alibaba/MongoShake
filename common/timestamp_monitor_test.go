package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
)

func TestTimestampQueryMonitorActualConnection(t *testing.T) {
	oldLogger := l.Logger
	t.Cleanup(func() { l.Logger = oldLogger })
	dir := t.TempDir()
	require.NoError(t, l.New("info", dir, "timestamp.log", true, 20, 1, 0))
	monitor := timestampQueryMonitor("mongodb://user:secret@seed:27017", VarMongoConnectModeStandalone)
	reply, err := bson.Marshal(bson.M{"cursor": bson.M{
		"ns": "local.oplog.rs", "firstBatch": bson.A{bson.M{"ts": primitive.Timestamp{T: 123, I: 7}}},
	}})
	require.NoError(t, err)
	e := &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{
		CommandName: "find", DatabaseName: "local", ConnectionID: "actual-secondary:27018[-3]",
	}, Reply: reply}
	monitor.Succeeded(context.Background(), e)
	data, err := os.ReadFile(filepath.Join(dir, "timestamp.log"))
	require.NoError(t, err)
	require.Contains(t, string(data), "actualConnection[actual-secondary:27018[-3]]")
	require.Contains(t, string(data), "source[mongodb://user:***@seed:27017]")
	require.Contains(t, string(data), "ts[{123 7}]")
	require.NotContains(t, string(data), "secret")

	// Unrelated queries, empty results and malformed timestamps cannot claim a read.
	for _, reply := range []bson.M{
		{"cursor": bson.M{"ns": "local.other", "firstBatch": bson.A{bson.M{"ts": primitive.Timestamp{T: 1}}}}},
		{"cursor": bson.M{"ns": "local.oplog.rs", "firstBatch": bson.A{}}},
		{"cursor": bson.M{"ns": "local.oplog.rs", "firstBatch": bson.A{bson.M{"ts": "invalid"}}}},
	} {
		e.Reply, err = bson.Marshal(reply)
		require.NoError(t, err)
		monitor.Succeeded(context.Background(), e)
	}
	after, err := os.ReadFile(filepath.Join(dir, "timestamp.log"))
	require.NoError(t, err)
	require.Equal(t, data, after)
}
