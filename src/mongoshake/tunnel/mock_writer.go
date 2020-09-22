package tunnel

import (
	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

type MockWriter struct {
}

func (tunnel *MockWriter) Name() string {
	return "mock"
}

func (tunnel *MockWriter) Send(message *WMessage) int64 {
	nimo.AssertTrue(len(message.RawLogs) > 0,
		"ack is not required. we should never receive empty messages")
	LOG.Info("MockTunnel received message length %d, shard %d, message: %v ",
		len(message.RawLogs), message.Shard, message.ParsedLogs)

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
