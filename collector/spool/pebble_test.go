package spool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestPebbleSpoolPutReadAdvance(t *testing.T) {
	ps := openTestSpool(t, t.TempDir(), "rs-spool-basic")
	defer ps.Delete()

	data := makeTestOplogs(t, 1, 2, 3)
	for _, item := range data {
		require.NoError(t, ps.Put(item))
	}

	stats := ps.Stats()
	assert.Equal(t, uint64(1), stats.ReadSeq, "should be equal")
	assert.Equal(t, uint64(3), stats.WriteSeq, "should be equal")
	assert.Equal(t, uint64(3), stats.Depth, "should be equal")

	batch, err := ps.ReadBatch(2)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	assert.Equal(t, data[0], batch[0], "should be equal")
	assert.Equal(t, data[1], batch[1], "should be equal")

	require.NoError(t, ps.Advance(2))
	depth, err := ps.Depth()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), depth, "should be equal")

	batch, err = ps.ReadBatch(10)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	assert.Equal(t, data[2], batch[0], "should be equal")
}

func TestPebbleSpoolPutBatchPersistsAfterCrash(t *testing.T) {
	memFS := vfs.NewStrictMem()
	logDir := "/logs"
	name := "rs-spool-crash"
	require.NoError(t, memFS.MkdirAll(logDir, 0o755))
	root, err := memFS.OpenDir("/")
	require.NoError(t, err)
	require.NoError(t, root.Sync())
	require.NoError(t, root.Close())

	ps, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: true,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
		FS:              memFS,
	})
	require.NoError(t, err)

	data := makeTestOplogs(t, 1, 2, 3)
	require.NoError(t, ps.PutBatch(data))

	memFS.SetIgnoreSyncs(true)
	require.NoError(t, ps.db.Close())
	ps.db = nil
	ps.closed = true
	memFS.ResetToSyncedState()
	memFS.SetIgnoreSyncs(false)

	reopened, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
		FS:              memFS,
	})
	require.NoError(t, err)
	defer reopened.Delete()

	rows, err := reopened.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, data, rows, "should be equal")
}

func TestPebbleSpoolAcceptNoopWithoutNamespace(t *testing.T) {
	ps := openTestSpool(t, t.TempDir(), "rs-spool-noop")
	defer ps.Delete()

	data, err := bson.Marshal(&oplog.ParsedLog{
		Timestamp: utils.TimeToTimestamp(11),
		Operation: "n",
	})
	require.NoError(t, err)

	require.NoError(t, ps.Put(data))
	last, err := ps.LastWriteTimestamp()
	require.NoError(t, err)
	assert.Equal(t, utils.TimeStampToInt64(utils.TimeToTimestamp(11)), last, "should be equal")

	rows, err := ps.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, data, rows[0], "should be equal")
}

func TestParseOplogTimestampRejectsMissingTimestamp(t *testing.T) {
	data, err := bson.Marshal(bson.D{{Key: "op", Value: "n"}})
	require.NoError(t, err)

	_, err = parseOplogTimestamp(data)
	require.ErrorContains(t, err, "timestamp field \"ts\" not found")
}

func TestPebbleSpoolReopen(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-reopen"
	ps := openTestSpool(t, logDir, name)

	data := makeTestOplogs(t, 11, 12, 13)
	for _, item := range data {
		require.NoError(t, ps.Put(item))
	}
	require.NoError(t, ps.Advance(2))
	require.NoError(t, ps.Close())

	reopened, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.NoError(t, err)
	defer reopened.Delete()

	stats := reopened.Stats()
	assert.Equal(t, uint64(1), stats.ReadSeq, "should be equal")
	assert.Equal(t, uint64(3), stats.WriteSeq, "should be equal")
	assert.Equal(t, uint64(3), stats.Depth, "should be equal")

	last, err := reopened.LastWriteTimestamp()
	require.NoError(t, err)
	assert.Equal(t, utils.TimeStampToInt64(utils.TimeToTimestamp(13)), last, "should be equal")

	remaining, err := reopened.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, data, remaining, "should be equal")
}

func TestPebbleSpoolRejectPutAfterMaxBytes(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-max"
	ps, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: true,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
		MaxBytesMB:      1,
	})
	require.NoError(t, err)
	defer ps.Delete()

	require.NoError(t, ps.Put(makeTestOplogs(t, 1)[0]))

	err = ps.Put(makeLargeTestOplog(t, 2, int(utils.MB)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max size", "should be equal")

	stats := ps.Stats()
	assert.Equal(t, uint64(1), stats.WriteSeq, "should be equal")
	assert.Equal(t, uint64(1), stats.Depth, "should be equal")
	assert.Greater(t, stats.Bytes, uint64(0), "should be equal")
}

func TestPebbleSpoolStatsUsesPebbleDiskUsage(t *testing.T) {
	memFS := vfs.NewMem()
	name := "rs-spool-size"
	ps, err := Open(OpenOptions{
		Name:            name,
		LogDir:          "/logs",
		CreateIfMissing: true,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
		FS:              memFS,
	})
	require.NoError(t, err)
	defer ps.Delete()

	require.NoError(t, ps.Put(makeTestOplogs(t, 1)[0]))
	expected := ps.db.Metrics().DiskSpaceUsage()
	assert.Greater(t, expected, uint64(0), "should be greater")
	assert.Equal(t, expected, ps.Stats().Bytes, "should be equal")
}

func TestPebbleSpoolDelete(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-delete"
	ps := openTestSpool(t, logDir, name)
	require.NoError(t, ps.Put(makeTestOplogs(t, 1)[0]))

	path := Path(logDir, name)
	require.NoError(t, ps.Delete())
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "should be equal")
}

func TestDeletePathRejectsUnsafeName(t *testing.T) {
	logDir := t.TempDir()
	marker := filepath.Join(logDir, "keep", "marker")
	require.NoError(t, os.MkdirAll(filepath.Dir(marker), 0o755))
	require.NoError(t, os.WriteFile(marker, []byte("keep"), 0o644))

	for _, name := range []string{"", ".", "..", "../keep", `..\keep`, "nested/spool"} {
		err := DeletePath(logDir, name)
		require.Error(t, err, name)
	}
	_, err := os.Stat(marker)
	require.NoError(t, err)
}

func TestPebbleSpoolOpenLockErrorIsNotLegacy(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-lock"
	ps := openTestSpool(t, logDir, name)
	defer ps.Delete()

	_, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open pebble oplog spool", "should be equal")
	assert.NotContains(t, err.Error(), "Legacy go-diskqueue", "should be equal")
}

func TestPebbleSpoolRejectNonPebbleDir(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-legacy"
	path := Path(logDir, name)
	require.NoError(t, os.MkdirAll(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "diskqueue.dat"), []byte("legacy"), 0o644))

	_, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a Pebble spool", "should be equal")
	assert.Contains(t, err.Error(), "Legacy go-diskqueue files are not supported", "should be equal")
}

func TestPebbleSpoolRejectLegacyFilesInLogDir(t *testing.T) {
	logDir := t.TempDir()
	name := "diskqueue-rs-legacy"
	require.NoError(t, os.WriteFile(filepath.Join(logDir, name+".dat"), []byte("legacy"), 0o644))

	_, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a Pebble spool", "should be equal")
	assert.Contains(t, err.Error(), "Legacy go-diskqueue files are not supported", "should be equal")
}

func TestPebbleSpoolRejectsSchemaV1(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-schema"
	ps := openTestSpool(t, logDir, name)
	require.NoError(t, ps.Close())

	db, err := pebble.Open(Path(logDir, name), &pebble.Options{ErrorIfNotExists: true})
	require.NoError(t, err)
	require.NoError(t, db.Set(metaVersionKey, encodeUint32(1), pebble.Sync))
	require.NoError(t, db.Close())

	_, err = Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema version mismatch", "should be equal")
	assert.Contains(t, err.Error(), "got 1 want 2", "should be equal")
}

func TestPebbleSpoolMissingDataKey(t *testing.T) {
	logDir := t.TempDir()
	name := "rs-spool-missing"
	ps := openTestSpool(t, logDir, name)
	data := makeTestOplogs(t, 1, 2, 3)
	for _, item := range data {
		require.NoError(t, ps.Put(item))
	}
	require.NoError(t, ps.Close())

	db, err := pebble.Open(Path(logDir, name), &pebble.Options{ErrorIfNotExists: true})
	require.NoError(t, err)
	require.NoError(t, db.Delete(dataKey(2), pebble.NoSync))
	require.NoError(t, db.Close())

	reopened, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: false,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.NoError(t, err)
	defer reopened.Delete()

	_, err = reopened.ReadBatch(10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing data key for seq 2", "should be equal")
}

func TestPebbleSpoolMetricsNoPanic(t *testing.T) {
	ps := openTestSpool(t, t.TempDir(), "rs-spool-metrics")
	defer ps.Delete()

	require.NoError(t, ps.Put(makeTestOplogs(t, 1)[0]))
	rows, err := ps.ReadBatch(1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NoError(t, ps.Advance(1))
	require.NoError(t, ps.Delete())
}

func openTestSpool(t *testing.T, logDir, name string) *PebbleSpool {
	t.Helper()

	ps, err := Open(OpenOptions{
		Name:            name,
		LogDir:          logDir,
		CreateIfMissing: true,
		MetricName:      name,
		MetricStage:     utils.TypeIncr,
	})
	require.NoError(t, err)
	return ps
}

func makeTestOplogs(t *testing.T, timestamps ...int64) [][]byte {
	t.Helper()

	out := make([][]byte, 0, len(timestamps))
	for _, ts := range timestamps {
		data, err := bson.Marshal(&oplog.ParsedLog{
			Timestamp: utils.TimeToTimestamp(ts),
			Operation: "i",
			Namespace: "a.b",
			Object:    bson.D{{Key: "_id", Value: ts}},
		})
		require.NoError(t, err)
		out = append(out, data)
	}
	return out
}

func makeLargeTestOplog(t *testing.T, ts int64, payloadSize int) []byte {
	t.Helper()

	data, err := bson.Marshal(&oplog.ParsedLog{
		Timestamp: utils.TimeToTimestamp(ts),
		Operation: "i",
		Namespace: "a.b",
		Object: bson.D{
			{Key: "_id", Value: ts},
			{Key: "payload", Value: strings.Repeat("x", payloadSize)},
		},
	})
	require.NoError(t, err)
	return data
}
