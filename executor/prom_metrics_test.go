package executor

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestPrometheusHandlerExposesExecutorOperationMetrics(t *testing.T) {
	exec := &Executor{
		batchExecutor: &BatchGroupExecutor{
			MetricName: "rs-exec-metrics",
		},
	}

	exec.addOperationMetric("insert", 3)
	exec.addOperationMetric("error", 1)

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	utils.PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `executor_op_total{name="rs-exec-metrics",op="insert",stage="incr"} 3`, "should be equal")
	assert.Contains(t, body, `executor_op_total{name="rs-exec-metrics",op="error",stage="incr"} 1`, "should be equal")
}
