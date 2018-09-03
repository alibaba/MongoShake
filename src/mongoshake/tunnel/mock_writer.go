package tunnel

import (
	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

type MockWriter struct {
}

func (tunnel *MockWriter) Send(message *WMessage) int64 {
	nimo.AssertTrue(len(message.RawLogs) > 0,
		"ack is not required. we should never receive empty messages")
	LOG.Debug("MockTunnel received message length %d, shard %d ", len(message.RawLogs), message.Shard)

	return 0
}

func (tunnel *MockWriter) Prepare() bool {
	return true
}

func (tunnel *MockWriter) AckRequired() bool {
	return false
}

func (tunnel *MockWriter) ParsedLogsRequired() bool {
	return false
}
