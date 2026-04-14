package docsyncer

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestPrometheusHandlerExposesFullSyncProgressMetrics(t *testing.T) {
	syncer := NewDBSyncer(1, "mongodb://from", "rs-full-metrics", "mongodb://to", nil, nil, nil, false)
	defer syncer.replMetric.Close()

	atomic.StoreInt64(&syncer.totalCollections, 2)
	atomic.StoreInt64(&syncer.waitingCollections, 2)
	syncer.updateCollectionProgressMetrics()

	first := NewCollectionMetric()
	second := NewCollectionMetric()
	syncer.markCollectionProcessing(first)
	syncer.markCollectionFinished(first)
	syncer.markCollectionProcessing(second)

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	utils.PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `full_sync_collections_total{name="rs-full-metrics",stage="full"} 2`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_finished{name="rs-full-metrics",stage="full"} 1`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_processing{name="rs-full-metrics",stage="full"} 1`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_waiting{name="rs-full-metrics",stage="full"} 0`, "should be equal")
}
