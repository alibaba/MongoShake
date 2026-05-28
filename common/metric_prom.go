package utils

import (
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	prometheusRegistry  = prometheus.NewRegistry()
	prometheusInitOnce  sync.Once
	prometheusStartTime = time.Now()
)

var OplogFilterProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_filter_total",
	Help: "Total number of filtered oplogs.",
}, []string{"name", "stage"})

var OplogGetProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_get_total",
	Help: "Total number of fetched oplogs.",
}, []string{"name", "stage"})

var OplogConsumeProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_consume_total",
	Help: "Total number of oplogs consumed by workers.",
}, []string{"name", "stage"})

var OplogApplyProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_apply_total",
	Help: "Total number of oplogs sent to downstream apply.",
}, []string{"name", "stage"})

var OplogSuccessProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_success_total",
	Help: "Total number of successfully acknowledged oplogs.",
}, []string{"name", "stage"})

var OplogSuccessTpsProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_success_tps",
	Help: "Current successful oplog throughput per second, matching the internal metric delta.",
}, []string{"name", "stage"})

var OplogFailProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_fail_total",
	Help: "Total number of failed oplog transfers.",
}, []string{"name", "stage"})

var OplogWriteFailProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_write_fail_total",
	Help: "Total number of failed full-sync writes.",
}, []string{"name", "stage"})

var CheckpointTimesProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "checkpoint_times_total",
	Help: "Total number of checkpoint updates.",
}, []string{"name", "stage"})

var RetransmissionProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "retransmission_total",
	Help: "Total number of retransmissions.",
}, []string{"name", "stage"})

var TunnelTrafficProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "tunnel_traffic_total",
	Help: "Total downstream tunnel traffic in bytes.",
}, []string{"name", "stage"})

var LSNProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn",
	Help: "Current latest oplog timestamp encoded as int64.",
}, []string{"name", "stage"})

var LSNAckProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_ack",
	Help: "Current acknowledged oplog timestamp encoded as int64.",
}, []string{"name", "stage"})

var LSNCheckpointProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_checkpoint",
	Help: "Current checkpoint oplog timestamp encoded as int64.",
}, []string{"name", "stage"})

var OplogMaxSizeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_max_size",
	Help: "Observed maximum oplog payload size in bytes.",
}, []string{"name", "stage"})

var OplogAvgSizeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_avg_size",
	Help: "Observed average oplog payload size in bytes.",
}, []string{"name", "stage"})

var TableOperationsProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "table_operations_total",
	Help: "Total number of operations grouped by namespace.",
}, []string{"name", "stage", "collection"})

var OplogGetDelayProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_get_delay",
	Help: "Delay in milliseconds between now and the latest fetched source wall/clusterTime.",
}, []string{"name", "stage"})

var OplogPutDelayProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_put_delay",
	Help: "Delay in milliseconds between now and the latest successfully applied or acknowledged source wall/clusterTime.",
}, []string{"name", "stage"})

var ReplStatusCodeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "repl_status_code",
	Help: "Current replication status code.",
}, []string{"name", "stage"})

var LSNAckLagSecondsProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_ack_lag_seconds",
	Help: "Lag in seconds between latest fetched LSN and acknowledged LSN.",
}, []string{"name", "stage"})

var LSNCheckpointLagSecondsProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_checkpoint_lag_seconds",
	Help: "Lag in seconds between latest fetched LSN and checkpoint LSN.",
}, []string{"name", "stage"})

var PendingQueueUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pending_queue_used",
	Help: "Current used size of the pending queue.",
}, []string{"name", "stage", "queue_id"})

var PendingQueueCapacityProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pending_queue_capacity",
	Help: "Capacity of the pending queue.",
}, []string{"name", "stage", "queue_id"})

var PendingQueueUsedRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pending_queue_used_ratio",
	Help: "Current used ratio of the pending queue, from 0 to 1.",
}, []string{"name", "stage", "queue_id"})

var LogsQueueUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "logs_queue_used",
	Help: "Current used size of the logs queue.",
}, []string{"name", "stage", "queue_id"})

var LogsQueueCapacityProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "logs_queue_capacity",
	Help: "Capacity of the logs queue.",
}, []string{"name", "stage", "queue_id"})

var LogsQueueUsedRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "logs_queue_used_ratio",
	Help: "Current used ratio of the logs queue, from 0 to 1.",
}, []string{"name", "stage", "queue_id"})

var WorkerJobsQueuedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_jobs_queued",
	Help: "Current number of jobs queued in a worker.",
}, []string{"name", "stage", "worker_id"})

var WorkerJobsQueueCapacityProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_jobs_queue_capacity",
	Help: "Capacity of a worker jobs queue.",
}, []string{"name", "stage", "worker_id"})

var WorkerJobsQueueUsedRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_jobs_queue_used_ratio",
	Help: "Current used ratio of a worker jobs queue, from 0 to 1.",
}, []string{"name", "stage", "worker_id"})

var WorkerUnackBufferUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_unack_buffer_used",
	Help: "Current number of oplogs held in the worker unack buffer.",
}, []string{"name", "stage", "worker_id"})

var PersisterBufferUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "persister_buffer_used",
	Help: "Current number of oplogs buffered inside the persister.",
}, []string{"name", "stage"})

var PersisterBufferCapacityProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "persister_buffer_capacity",
	Help: "Capacity of the persister in-memory buffer.",
}, []string{"name", "stage"})

var PersisterBufferUsedRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "persister_buffer_used_ratio",
	Help: "Current used ratio of the persister in-memory buffer, from 0 to 1.",
}, []string{"name", "stage"})

var FullSyncCollectionsTotalProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collections_total",
	Help: "Total number of collections discovered for full sync.",
}, []string{"name", "stage"})

var FullSyncCollectionsFinishedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collections_finished",
	Help: "Number of collections finished during full sync.",
}, []string{"name", "stage"})

var FullSyncCollectionsProcessingProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collections_processing",
	Help: "Number of collections currently processing during full sync.",
}, []string{"name", "stage"})

var FullSyncCollectionsWaitingProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collections_waiting",
	Help: "Number of collections waiting to start during full sync.",
}, []string{"name", "stage"})

var FullSyncCollectionsProgressRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collections_progress_ratio",
	Help: "Overall full sync collection progress ratio, from 0 to 1.",
}, []string{"name", "stage"})

var FullSyncCollectionStatusProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collection_status",
	Help: "Full sync collection status code: 0 wait start, 1 processing, 2 finish.",
}, []string{"name", "stage", "db", "collection"})

var FullSyncCollectionDocsTotalProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collection_docs_total",
	Help: "Total number of documents discovered for a collection during full sync.",
}, []string{"name", "stage", "db", "collection"})

var FullSyncCollectionDocsFinishedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collection_docs_finished",
	Help: "Number of documents finished for a collection during full sync.",
}, []string{"name", "stage", "db", "collection"})

var FullSyncCollectionProgressRatioProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_sync_collection_progress_ratio",
	Help: "Full sync collection document progress ratio, from 0 to 1.",
}, []string{"name", "stage", "db", "collection"})

var MongoShakeSyncStageProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mongoshake_sync_stage",
	Help: "Current MongoShake sync stage. 1 means active, 0 means inactive.",
}, []string{"name", "stage"})

var MongoShakeSyncStageStartTimeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mongoshake_sync_stage_start_time_seconds",
	Help: "Unix timestamp when the current sync stage started.",
}, []string{"name", "stage"})

var MongoShakeSyncStateCodeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mongoshake_sync_state_code",
	Help: "MongoShake sync state code: 0 init, 1 running, 2 done, 3 error, 4 stopped.",
}, []string{"name", "stage"})

var MongoShakeInfoProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mongoshake_info",
	Help: "MongoShake build and runtime information.",
}, []string{"name", "version", "go_version"})

var MongoShakeUptimeSecondsProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mongoshake_uptime_seconds",
	Help: "MongoShake process uptime in seconds observed by the Prometheus handler.",
}, []string{"name"})

var ExecutorOperationsProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "executor_op_total",
	Help: "Total number of direct executor operations grouped by operation type.",
}, []string{"name", "stage", "op"})

func InitPrometheus() {
	prometheusInitOnce.Do(func() {
		prometheusRegistry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
			OplogFilterProm,
			OplogGetProm,
			OplogConsumeProm,
			OplogApplyProm,
			OplogSuccessProm,
			OplogSuccessTpsProm,
			OplogFailProm,
			OplogWriteFailProm,
			CheckpointTimesProm,
			RetransmissionProm,
			TunnelTrafficProm,
			LSNProm,
			LSNAckProm,
			LSNCheckpointProm,
			OplogMaxSizeProm,
			OplogAvgSizeProm,
			TableOperationsProm,
			OplogGetDelayProm,
			OplogPutDelayProm,
			ReplStatusCodeProm,
			LSNAckLagSecondsProm,
			LSNCheckpointLagSecondsProm,
			PendingQueueUsedProm,
			PendingQueueCapacityProm,
			PendingQueueUsedRatioProm,
			LogsQueueUsedProm,
			LogsQueueCapacityProm,
			LogsQueueUsedRatioProm,
			WorkerJobsQueuedProm,
			WorkerJobsQueueCapacityProm,
			WorkerJobsQueueUsedRatioProm,
			WorkerUnackBufferUsedProm,
			PersisterBufferUsedProm,
			PersisterBufferCapacityProm,
			PersisterBufferUsedRatioProm,
			FullSyncCollectionsTotalProm,
			FullSyncCollectionsFinishedProm,
			FullSyncCollectionsProcessingProm,
			FullSyncCollectionsWaitingProm,
			FullSyncCollectionsProgressRatioProm,
			FullSyncCollectionStatusProm,
			FullSyncCollectionDocsTotalProm,
			FullSyncCollectionDocsFinishedProm,
			FullSyncCollectionProgressRatioProm,
			ExecutorOperationsProm,
			MongoShakeSyncStageProm,
			MongoShakeSyncStageStartTimeProm,
			MongoShakeSyncStateCodeProm,
			MongoShakeInfoProm,
			MongoShakeUptimeSecondsProm,
		)
	})
}

func PrometheusHandler() http.Handler {
	return PrometheusHandlerWithName("collector")
}

func PrometheusHandlerWithName(name string) http.Handler {
	InitPrometheus()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ObserveMongoShakeUptime(name)
		promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
}

const (
	SyncStateInit uint64 = iota
	SyncStateRunning
	SyncStateDone
	SyncStateError
	SyncStateStopped
)

func SetMongoShakeSyncStage(name string, activeStage string) {
	now := float64(time.Now().Unix())
	for _, stage := range []string{TypeFull, TypeIncr} {
		value := 0.0
		if stage == activeStage {
			value = 1
			MongoShakeSyncStageStartTimeProm.WithLabelValues(name, stage).Set(now)
		}
		MongoShakeSyncStageProm.WithLabelValues(name, stage).Set(value)
	}
}

func SetMongoShakeSyncState(name, stage string, state uint64) {
	MongoShakeSyncStateCodeProm.WithLabelValues(name, stage).Set(float64(state))
}

func SetMongoShakeInfo(name string) {
	MongoShakeInfoProm.WithLabelValues(name, BRANCH, runtime.Version()).Set(1)
}

func ObserveMongoShakeUptime(name string) {
	MongoShakeUptimeSecondsProm.WithLabelValues(name).Set(time.Since(prometheusStartTime).Seconds())
}

func QueueUsedRatio(used, capacity int) float64 {
	if capacity <= 0 {
		return 0
	}
	return float64(used) / float64(capacity)
}
