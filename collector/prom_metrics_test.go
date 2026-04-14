package collector

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestPrometheusHandlerExposesQueueMetrics(t *testing.T) {
	syncer := &OplogSyncer{
		Replset:      "rs-queue-metrics",
		PendingQueue: []chan [][]byte{make(chan [][]byte, 2)},
		logsQueue:    []chan []*oplog.GenericOplog{make(chan []*oplog.GenericOplog, 2)},
	}
	syncer.PendingQueue[0] <- [][]byte{[]byte("a")}
	syncer.logsQueue[0] <- []*oplog.GenericOplog{{}}
	syncer.updatePendingQueueMetric(0)
	syncer.updateLogsQueueMetric(0)

	persister := &Persister{
		replset: syncer.Replset,
		sync:    syncer,
		Buffer:  [][]byte{[]byte("a")},
	}
	persister.updateBufferUsedMetric()

	worker := &Worker{
		syncer: syncer,
		id:     7,
		queue:  make(chan []*oplog.GenericOplog, 2),
	}
	worker.queue <- []*oplog.GenericOplog{{}}
	worker.listUnACK = []*oplog.GenericOplog{{}}
	atomic.StoreInt64(&worker.unackSize, 1)
	worker.updateJobsQueuedMetric()
	worker.updateUnackBufferMetric()

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	utils.PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `pending_queue_used{name="rs-queue-metrics",queue_id="0",stage="incr"} 1`, "should be equal")
	assert.Contains(t, body, `logs_queue_used{name="rs-queue-metrics",queue_id="0",stage="incr"} 1`, "should be equal")
	assert.Contains(t, body, `worker_jobs_queued{name="rs-queue-metrics",stage="incr",worker_id="7"} 1`, "should be equal")
	assert.Contains(t, body, `worker_unack_buffer_used{name="rs-queue-metrics",stage="incr",worker_id="7"} 1`, "should be equal")
	assert.Contains(t, body, `persister_buffer_used{name="rs-queue-metrics",stage="incr"} 1`, "should be equal")
}
