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

	firstNS := utils.NS{Database: "db1", Collection: "users"}
	secondNS := utils.NS{Database: "db1", Collection: "orders"}
	first := NewCollectionMetric()
	second := NewCollectionMetric()
	atomic.StoreUint64(&first.TotalCount, 100)
	syncer.updateSingleCollectionProgressMetric(firstNS, first)
	syncer.updateSingleCollectionProgressMetric(secondNS, second)
	syncer.markCollectionProcessing(firstNS, first)
	syncer.addCollectionFinishedDocs(firstNS, first, 25)
	syncer.markCollectionFinished(firstNS, first)
	syncer.markCollectionProcessing(secondNS, second)

	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	utils.PrometheusHandler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	assert.Equal(t, 200, recorder.Code, "should be equal")
	assert.Contains(t, body, `full_sync_collections_total{name="rs-full-metrics",stage="full"} 2`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_finished{name="rs-full-metrics",stage="full"} 1`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_processing{name="rs-full-metrics",stage="full"} 1`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_waiting{name="rs-full-metrics",stage="full"} 0`, "should be equal")
	assert.Contains(t, body, `full_sync_collections_progress_ratio{name="rs-full-metrics",stage="full"} 0.5`, "should be equal")
	assert.Contains(t, body, `full_sync_collection_status{collection="users",db="db1",name="rs-full-metrics",stage="full"} 2`, "should be equal")
	assert.Contains(t, body, `full_sync_collection_docs_total{collection="users",db="db1",name="rs-full-metrics",stage="full"} 100`, "should be equal")
	assert.Contains(t, body, `full_sync_collection_docs_finished{collection="users",db="db1",name="rs-full-metrics",stage="full"} 25`, "should be equal")
	assert.Contains(t, body, `full_sync_collection_progress_ratio{collection="users",db="db1",name="rs-full-metrics",stage="full"} 1`, "should be equal")
	assert.Contains(t, body, `full_sync_collection_status{collection="orders",db="db1",name="rs-full-metrics",stage="full"} 1`, "should be equal")
}
