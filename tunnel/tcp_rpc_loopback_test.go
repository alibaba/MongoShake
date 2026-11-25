package tunnel

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketCodecRoundTrip(t *testing.T) {
	payload := []byte("packet-payload")
	p := NewPacketV1(PacketWrite, payload)
	encoded := p.encode()
	require.Len(t, encoded, HeaderLen+len(payload))

	var q Packet
	require.True(t, q.decodeHeader(encoded[:HeaderLen]))
	assert.Equal(t, PacketWrite, q.typeOf)
	assert.Equal(t, uint32(len(payload)), q.length)
	q.setPayload(encoded[HeaderLen:])
	assert.Equal(t, payload, q.payload)
	assert.Contains(t, p.String(), "magic:")

	// a corrupt magic / version / type must not validate
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 0x00 },                // magic
		func(b []byte) { b[2] = 0x7f },                // version
		func(b []byte) { b[3] = UndefinedPacketType }, // type
	} {
		bad := append([]byte(nil), encoded[:HeaderLen]...)
		mutate(bad)
		var r Packet
		assert.False(t, r.decodeHeader(bad))
	}
}

// freeTCPBasePort returns a port P such that P and P+1 are both bindable, which
// is what TCPReader needs (transfer + ack channels).
func freeTCPBasePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 20; i++ {
		l1, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		p := l1.Addr().(*net.TCPAddr).Port
		l1.Close()
		l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+1))
		if err == nil {
			l2.Close()
			return p
		}
	}
	t.Fatal("no adjacent free tcp port pair")
	return 0
}

func TestTCPLoopbackRoundTrip(t *testing.T) {
	port := freeTCPBasePort(t)
	rep := &captureReplayer{reply: ReplyOK}
	reader := &TCPReader{listenAddress: fmt.Sprintf("127.0.0.1:%d", port)}
	require.NoError(t, reader.Link([]Replayer{rep}))
	// Do NOT close the listeners in cleanup: Link's acceptors run in
	// nimo.GoRoutineInLoop (a for{} with no backoff), so a closed listener makes
	// AcceptTCP fail instantly and the loop hot-spins, flooding the log. Leaving
	// them open blocks in Accept instead; the test binary's exit reclaims them.

	writer := &TCPWriter{RemoteAddr: fmt.Sprintf("127.0.0.1:%d", port)}
	require.True(t, writer.Prepare())
	assert.True(t, writer.AckRequired())
	assert.False(t, writer.ParsedLogsRequired())

	logs := [][]byte{[]byte("tcp-entry")}
	writer.Send(&WMessage{TMessage: &TMessage{RawLogs: logs, Checksum: 9, Shard: 0}})

	got := waitReplay(t, rep, 1)
	require.Len(t, got, 1)
	assert.Equal(t, logs, got[0].RawLogs)
	assert.Equal(t, uint32(9), got[0].Checksum)
}

func TestTCPUnreachableReturnsNetworkFail(t *testing.T) {
	writer := &TCPWriter{RemoteAddr: "127.0.0.1:1"}
	require.True(t, writer.Prepare()) // InitialStageChecking is false, so no dial yet
	assert.Equal(t, ReplyNetworkOpFail, writer.Send(&WMessage{TMessage: &TMessage{RawLogs: [][]byte{{1}}}}))
}

func TestRPCLoopbackRoundTrip(t *testing.T) {
	port := freeTCPBasePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	rep := &captureReplayer{reply: ReplyOK}
	reader := &RPCReader{address: addr}
	require.NoError(t, reader.Link([]Replayer{rep}))

	writer := &RPCWriter{RemoteAddr: addr}
	require.True(t, writer.Prepare())
	assert.True(t, writer.AckRequired())

	logs := [][]byte{[]byte("rpc-entry")}
	reply := writer.Send(&WMessage{TMessage: &TMessage{RawLogs: logs, Checksum: 5, Shard: 0}})
	assert.Equal(t, int64(ReplyOK), reply)

	got := waitReplay(t, rep, 1)
	require.Len(t, got, 1)
	assert.Equal(t, logs, got[0].RawLogs)
	assert.Equal(t, uint32(5), got[0].Checksum)

	if writer.rpcClient != nil {
		_ = writer.rpcClient.Close()
	}
}

func TestRPCUnreachableReturnsNetworkFail(t *testing.T) {
	writer := &RPCWriter{RemoteAddr: "127.0.0.1:1"}
	require.True(t, writer.Prepare())
	assert.Equal(t, ReplyNetworkOpFail, writer.Send(&WMessage{TMessage: &TMessage{RawLogs: [][]byte{{1}}}}))
}
