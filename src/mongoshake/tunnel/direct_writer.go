package tunnel

import (
	"mongoshake/dbpool"
	"mongoshake/executor"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

type DirectWriter struct {
	RemoteAddrs   []string
	ReplayerId    uint32 // equal to worker-id
	batchExecutor *executor.BatchGroupExecutor
}

func (writer *DirectWriter) Prepare() bool {
	nimo.AssertTrue(len(writer.RemoteAddrs) > 0, "RemoteAddrs must > 0")

	first := writer.RemoteAddrs[0]
	if _, err := dbpool.NewMongoConn(first, false); err != nil {
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
	writer.batchExecutor.Sync(message.ParsedLogs, nil)
	return 0
}

func (writer *DirectWriter) AckRequired() bool {
	return false
}

func (writer *DirectWriter) ParsedLogsRequired() bool {
	return true
}
