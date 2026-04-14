package utils

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestPrometheusHandlerExposesMetrics(t *testing.T) {
	metric := NewMetric("rs-prom", TypeIncr, METRIC_LSN)
	defer metric.Close()

	metric.SetLSN(123)
	metric.SetOplogGetDelay(456)

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `lsn{name="rs-prom",stage="incr"} 123`, "should be equal")
	assert.Contains(t, body, `oplog_get_delay{name="rs-prom",stage="incr"} 456`, "should be equal")
}

func TestPrometheusHandlerExposesStatusAndLagMetrics(t *testing.T) {
	metric := NewMetric("rs-prom-derived", TypeIncr, METRIC_LSN)
	defer metric.Close()

	metric.SetReplStatus(FetchBad)
	metric.SetLSN(TimeStampToInt64(primitive.Timestamp{T: 120, I: 1}))
	metric.SetLSNACK(TimeStampToInt64(primitive.Timestamp{T: 111, I: 1}))
	metric.SetLSNCheckpoint(TimeStampToInt64(primitive.Timestamp{T: 101, I: 1}))

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `repl_status_code{name="rs-prom-derived",stage="incr"} 2`, "should be equal")
	assert.Contains(t, body, `lsn_ack_lag_seconds{name="rs-prom-derived",stage="incr"} 9`, "should be equal")
	assert.Contains(t, body, `lsn_checkpoint_lag_seconds{name="rs-prom-derived",stage="incr"} 19`, "should be equal")
}

func TestPrometheusHandlerExposesZeroValueMetricsForFreshIncrMetric(t *testing.T) {
	metric := NewMetric("rs-prom-zero", TypeIncr, 0)
	defer metric.Close()

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `oplog_fail_total{name="rs-prom-zero",stage="incr"} 0`, "should be equal")
	assert.Contains(t, body, `retransmission_total{name="rs-prom-zero",stage="incr"} 0`, "should be equal")
	assert.Contains(t, body, `checkpoint_times_total{name="rs-prom-zero",stage="incr"} 0`, "should be equal")
}

func TestPrometheusHandlerExposesRuntimeCollectors(t *testing.T) {
	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `go_info`, "should be equal")
	assert.Contains(t, body, `go_goroutines`, "should be equal")
}
