package module

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/MongoShake/v2/tunnel"
)

func TestChecksumCalculatorRegistration(t *testing.T) {
	coder := &ChecksumCalculator{}
	assert.True(t, coder.IsRegistered())
	assert.True(t, coder.Install())
}

func TestChecksumCalculatorHandleWithoutLogs(t *testing.T) {
	coder := &ChecksumCalculator{}
	msg := &tunnel.WMessage{TMessage: &tunnel.TMessage{Checksum: 0xdeadbeef}}
	assert.Equal(t, tunnel.ReplyOK, coder.Handle(msg))
	// nothing to checksum: the existing value must be left alone
	assert.Equal(t, uint32(0xdeadbeef), msg.Checksum)
}

func TestChecksumCalculatorHandleSetsCrc32(t *testing.T) {
	coder := &ChecksumCalculator{}
	logs := [][]byte{[]byte("entry one"), []byte("entry two")}
	msg := &tunnel.WMessage{TMessage: &tunnel.TMessage{RawLogs: logs}}
	require.Equal(t, tunnel.ReplyOK, coder.Handle(msg))
	assert.Equal(t, msg.Crc32(), msg.Checksum)
	assert.NotZero(t, msg.Checksum)

	// Crc32 XORs per-entry checksums, so order does not matter but content does.
	reordered := &tunnel.WMessage{TMessage: &tunnel.TMessage{RawLogs: [][]byte{logs[1], logs[0]}}}
	require.Equal(t, tunnel.ReplyOK, coder.Handle(reordered))
	assert.Equal(t, msg.Checksum, reordered.Checksum)

	changed := &tunnel.WMessage{TMessage: &tunnel.TMessage{RawLogs: [][]byte{[]byte("entry one"), []byte("entry TWO")}}}
	require.Equal(t, tunnel.ReplyOK, coder.Handle(changed))
	assert.NotEqual(t, msg.Checksum, changed.Checksum)
}
