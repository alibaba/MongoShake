package tunnel

import (
	"mongoshake/executor"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"mongoshake/common"
)

type DirectWriter struct {
	RemoteAddrs   []string
	ReplayerId    uint32 // equal to worker-id
	batchExecutor *executor.BatchGroupExecutor
}

func (writer *DirectWriter) Prepare() bool {
	nimo.AssertTrue(len(writer.RemoteAddrs) > 0, "RemoteAddrs must > 0")

	first := writer.RemoteAddrs[0]
	if _, err := utils.NewMongoConn(first, utils.VarMongoConnectModeSecondaryPreferred, true); err != nil {
		LOG.Critical("target mongo server[%s] connect failed: %s", first, err.Error())
		return false
	}

	urlChoose := writer.ReplayerId % uint32(len(writer.RemoteAddrs))
	writer.batchExecutor = &executor.BatchGroupExecutor{
		ReplayerId: writer.ReplayerId,
		MongoUrl:   writer.RemoteAddrs[urlChoose],
	}
	// writer.batchExecutor.RestAPI()
	writer.batchExecutor.Start()
	return true
}

func (writer *DirectWriter) Send(message *WMessage) int64 {
	// won't return when Sync has been finished which is a synchronous operation.
	writer.batchExecutor.Sync(message.ParsedLogs, nil)
	return 0
}

func (writer *DirectWriter) AckRequired() bool {
	return false
}

func (writer *DirectWriter) ParsedLogsRequired() bool {
	return true
}
