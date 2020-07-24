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
	BatchExecutor *executor.BatchGroupExecutor
}

func (writer *DirectWriter) Name() string {
	return "direct"
}

func (writer *DirectWriter) Prepare() bool {
	nimo.AssertTrue(len(writer.RemoteAddrs) > 0, "RemoteAddrs must > 0")

	first := writer.RemoteAddrs[0]
	if _, err := utils.NewMongoConn(first, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); err != nil {
		LOG.Critical("target mongo server[%s] connect failed: %s", first, err.Error())
		return false
	}

	urlChoose := writer.ReplayerId % uint32(len(writer.RemoteAddrs))
	writer.BatchExecutor = &executor.BatchGroupExecutor{
		ReplayerId: writer.ReplayerId,
		MongoUrl:   writer.RemoteAddrs[urlChoose],
	}
	// writer.batchExecutor.RestAPI()
	writer.BatchExecutor.Start()
	return true
}

func (writer *DirectWriter) Send(message *WMessage) int64 {
	// won't return when Sync has been finished which is a synchronous operation.
	writer.BatchExecutor.Sync(message.ParsedLogs, nil)
	return 0
}

func (writer *DirectWriter) AckRequired() bool {
	return false
}

func (writer *DirectWriter) ParsedLogsRequired() bool {
	return true
}
