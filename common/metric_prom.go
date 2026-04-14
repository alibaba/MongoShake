package utils

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	prometheusRegistry = prometheus.NewRegistry()
	prometheusInitOnce sync.Once
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

var LogsQueueUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "logs_queue_used",
	Help: "Current used size of the logs queue.",
}, []string{"name", "stage", "queue_id"})

var WorkerJobsQueuedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_jobs_queued",
	Help: "Current number of jobs queued in a worker.",
}, []string{"name", "stage", "worker_id"})

var WorkerUnackBufferUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_unack_buffer_used",
	Help: "Current number of oplogs held in the worker unack buffer.",
}, []string{"name", "stage", "worker_id"})

var PersisterBufferUsedProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "persister_buffer_used",
	Help: "Current number of oplogs buffered inside the persister.",
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

var ExecutorOperationsProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "executor_op_total",
	Help: "Total number of direct executor operations grouped by operation type.",
}, []string{"name", "stage", "op"})

func InitPrometheus() {
	prometheusInitOnce.Do(func() {
		prometheusRegistry.MustRegister(
			collectors.NewGoCollector(),
			OplogFilterProm,
			OplogGetProm,
			OplogConsumeProm,
			OplogApplyProm,
			OplogSuccessProm,
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
			LogsQueueUsedProm,
			WorkerJobsQueuedProm,
			WorkerUnackBufferUsedProm,
			PersisterBufferUsedProm,
			FullSyncCollectionsTotalProm,
			FullSyncCollectionsFinishedProm,
			FullSyncCollectionsProcessingProm,
			FullSyncCollectionsWaitingProm,
			ExecutorOperationsProm,
		)
	})
}

func PrometheusHandler() http.Handler {
	InitPrometheus()
	return promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{})
}
