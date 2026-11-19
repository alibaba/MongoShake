package tunnel

import (
	"path/filepath"
	"testing"

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

// Prepare's failure path returns before starting SyncToDisk, so it leaks nothing.
// The success path is deliberately not covered here: SyncToDisk never returns and
// logs on a 1s ticker, and pkg/log.InitialLogger reassigns the global Logger
// without synchronisation, so a leaked goroutine makes `go test -race ./tunnel/`
// fail against the existing TestKafkaWriter. Covering it needs that race fixed
// first.
func TestFileWriterPrepareFailsOnBadPath(t *testing.T) {
	globalInitializer = 0
	w := &FileWriter{Local: filepath.Join(t.TempDir(), "missing-dir", "oplog")}
	assert.False(t, w.Prepare())
	assert.Nil(t, w.dataFile)
}
