package tunnel

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureReplayer struct {
	mu    sync.Mutex
	got   []*TMessage
	reply int64
}

func (r *captureReplayer) Sync(message *TMessage, completion func()) int64 {
	r.mu.Lock()
	r.got = append(r.got, message)
	r.mu.Unlock()
	if completion != nil {
		completion()
	}
	return r.reply
}

func (r *captureReplayer) GetAcked() int64 { return 0 }

func (r *captureReplayer) snapshot() []*TMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*TMessage(nil), r.got...)
}

func waitReplay(t *testing.T, r *captureReplayer, n int) []*TMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(r.snapshot()) < n {
		time.Sleep(50 * time.Millisecond)
	}
	return r.snapshot()
}

// copyOplogFile reuses the bytes the real FileWriter produced, so the reader is
// tested against the product's own format without starting a second writer
// goroutine on the shared global channel.
func copyOplogFile(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(ensureOplogFile(t))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "oplog")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func TestFileReaderRoundTrip(t *testing.T) {
	path := copyOplogFile(t)
	// the written block carries shard 3; two replayers fold it to 1
	reps := []*captureReplayer{{reply: ReplyOK}, {reply: ReplyOK}}
	r := &FileReader{File: path}
	require.NoError(t, r.Link([]Replayer{reps[0], reps[1]}))

	got := waitReplay(t, reps[1], 1)
	require.Len(t, got, 1)
	assert.Empty(t, reps[0].snapshot())
	assert.Equal(t, uint32(7), got[0].Checksum)
	assert.Equal(t, uint32(1), got[0].Shard)
	assert.Equal(t, uint32(1), got[0].Compress)
	assert.Equal(t, oplogFileLogs, got[0].RawLogs)
	// the reader marks replayed blocks for retransmission
	assert.NotZero(t, got[0].Tag&MsgRetransmission)
}

func TestFileReaderReplayerRejections(t *testing.T) {
	// Verifies the block is still delivered when the replayer rejects it (the
	// reader must not crash or drop it). The per-code logging arms themselves are
	// not asserted: a rejection makes recvTransfer retry forever, so observing a
	// second delivery would hot-spin; delivery-once is the safe observable.
	for _, reply := range []int64{ReplyChecksumInvalid, ReplyRetransmission,
		ReplyCompressorNotSupported, ReplyNetworkOpFail, ReplyError, ReplyServerFault} {
		path := copyOplogFile(t)
		rep := &captureReplayer{reply: reply}
		r := &FileReader{File: path}
		require.NoError(t, r.Link([]Replayer{rep}))
		assert.Len(t, waitReplay(t, rep, 1), 1, "reply %d", reply)
	}
}

func TestFileReaderLinkErrors(t *testing.T) {
	r := &FileReader{File: filepath.Join(t.TempDir(), "absent")}
	assert.Error(t, r.Link([]Replayer{&captureReplayer{}}))

	bad := filepath.Join(t.TempDir(), "bad-magic")
	require.NoError(t, os.WriteFile(bad, make([]byte, 64), 0o644))
	r2 := &FileReader{File: bad}
	err := r2.Link([]Replayer{&captureReplayer{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "magic number or protocol number is invalid")
}

func TestFileReaderResharding(t *testing.T) {
	path := copyOplogFile(t)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// block header starts at 32; shard is the third uint32. Rewrite 3 -> 7 so
	// three replayers fold it to 1 rather than the original 0.
	shard := data[32+8 : 32+12]
	shard[0], shard[1], shard[2], shard[3] = 0, 0, 0, 7
	require.NoError(t, os.WriteFile(path, data, 0o644))

	reps := []*captureReplayer{{reply: ReplyOK}, {reply: ReplyOK}, {reply: ReplyOK}}
	r := &FileReader{File: path}
	require.NoError(t, r.Link([]Replayer{reps[0], reps[1], reps[2]}))
	got := waitReplay(t, reps[1], 1)
	require.Len(t, got, 1)
	assert.Equal(t, uint32(1), got[0].Shard, "7 %% 3 replayers must fold to 1")
	assert.Empty(t, reps[0].snapshot())
	assert.Empty(t, reps[2].snapshot())
}
