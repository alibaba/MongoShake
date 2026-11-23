package tunnel

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockWriterBasics(t *testing.T) {
	w := &MockWriter{}
	assert.Equal(t, "mock", w.Name())
	assert.True(t, w.Prepare())
	assert.False(t, w.AckRequired())
	assert.False(t, w.ParsedLogsRequired())
	assert.Equal(t, int64(0), w.Send(&WMessage{TMessage: &TMessage{RawLogs: [][]byte{{1}}}}))
}

func TestMockReaderLinkWithoutReplayers(t *testing.T) {
	r := &MockReader{}
	require.NoError(t, r.Link([]Replayer{}))
	assert.Empty(t, r.generator)
}

func TestFileWriterBasics(t *testing.T) {
	w := &FileWriter{}
	assert.Equal(t, "file", w.Name())
	assert.False(t, w.AckRequired())
	assert.False(t, w.ParsedLogsRequired())
}

func TestFileOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	f, ok := _Open(path)
	require.True(t, ok)
	require.NotNil(t, f)
	_ = f.Close()

	// a missing parent directory cannot be created by OpenFile
	f, ok = _Open(filepath.Join(t.TempDir(), "no-such-dir", "data"))
	assert.False(t, ok)
	assert.Nil(t, f)
}

func TestFileHeaderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "header")
	f, ok := _Open(path)
	require.True(t, ok)
	defer f.Close()

	df := &DataFile{filehandle: f}
	df.WriteHeader()

	// WriteHeader seeks past the 32-byte header; rewind to read it back.
	_, err := f.Seek(0, 0)
	require.NoError(t, err)
	header := df.ReadHeader()
	assert.Equal(t, FILE_MAGIC_NUMBER, header.Magic)
	assert.Equal(t, FILE_PROTOCOL_NUMBER, header.Protocol)
	assert.Equal(t, uint32(0), header.Checksum)

	info, err := f.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(32), info.Size())
}

// FileWriter.Prepare installs package-global state and starts SyncToDisk, which
// never returns and re-reads the global oplogMessage channel every iteration.
// Producing more than one file would leave several goroutines competing for that
// channel, so the whole package shares a single production.
var (
	oplogFileOnce sync.Once
	oplogFilePath string
	oplogFileLogs = [][]byte{[]byte("abc"), []byte("de")}
	// 32-byte file header + 24-byte block header + (4+3) + (4+2) payload
	oplogFileSize = int64(32 + 24 + 7 + 6)
)

func ensureOplogFile(t *testing.T) string {
	t.Helper()
	oplogFileOnce.Do(func() {
		globalInitializer = 0
		dir, err := os.MkdirTemp("", "mshake-file-tunnel")
		if err != nil {
			t.Fatal(err)
		}
		oplogFilePath = filepath.Join(dir, "oplog")
		w := &FileWriter{Local: oplogFilePath}
		if !w.Prepare() {
			t.Fatal("FileWriter.Prepare failed")
		}
		// probe messages must not be persisted
		w.Send(&WMessage{TMessage: &TMessage{RawLogs: [][]byte{{1}}, Tag: MsgProbe}})
		time.Sleep(300 * time.Millisecond)
		if info, err := os.Stat(oplogFilePath); err != nil || info.Size() != 32 {
			t.Fatalf("probe message was persisted: %v", err)
		}
		w.Send(&WMessage{TMessage: &TMessage{
			RawLogs: oplogFileLogs, Checksum: 7, Shard: 3, Compress: 1,
		}})
		// SyncToDisk writes the block header and payload in two Write calls, so
		// waiting for "grew past 32" could observe a half-written block.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if info, err := os.Stat(oplogFilePath); err == nil && info.Size() >= oplogFileSize {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("SyncToDisk never wrote the complete block")
	})
	return oplogFilePath
}

func TestFileWriterPrepareSendAndSync(t *testing.T) {
	path := ensureOplogFile(t)

	// a second Prepare is a no-op: the CAS already fired
	w := &FileWriter{Local: path}
	assert.True(t, w.Prepare())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), int(oplogFileSize))
	block := data[32:]
	assert.Equal(t, uint32(7), binary.BigEndian.Uint32(block[0:4]))
	assert.Equal(t, uint32(MsgPersistent|MsgStorageBackend), binary.BigEndian.Uint32(block[4:8]))
	assert.Equal(t, uint32(3), binary.BigEndian.Uint32(block[8:12]))
	assert.Equal(t, uint32(1), binary.BigEndian.Uint32(block[12:16]))
	assert.Equal(t, uint32(0xeeeeeeee), binary.BigEndian.Uint32(block[16:20]))

	payloadLen := binary.BigEndian.Uint32(block[20:24])
	assert.Equal(t, uint32(4+3+4+2), payloadLen)
	payload := block[24:]
	require.Len(t, payload, int(payloadLen))
	assert.Equal(t, uint32(3), binary.BigEndian.Uint32(payload[0:4]))
	assert.Equal(t, "abc", string(payload[4:7]))
	assert.Equal(t, uint32(2), binary.BigEndian.Uint32(payload[7:11]))
	assert.Equal(t, "de", string(payload[11:13]))
}

// Prepare's failure path returns before starting SyncToDisk, so it leaks nothing.
// Leave globalInitializer at 1 afterwards: resetting it would let a later Prepare
// re-initialise and O_TRUNC the shared file, which breaks -count > 1.
func TestFileWriterPrepareFailsOnBadPath(t *testing.T) {
	globalInitializer = 0
	w := &FileWriter{Local: filepath.Join(t.TempDir(), "missing-dir", "oplog")}
	assert.False(t, w.Prepare())
	assert.Nil(t, w.dataFile)
}
