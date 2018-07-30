package utils

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	LOG "github.com/vinllen/log4go"
)

const (
	FrequentInSeconds        = 5
	TimeFormat        string = "2006-01-02 15:04:05"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
)

// struct used to mark the metric delta.
// Value: current value
// Delta: the difference between current value and previous value
// previous: store the previous value
type MetricDelta struct {
	Value    uint64
	Delta    uint64
	previous uint64
}

func (o *MetricDelta) Update() {
	current := atomic.LoadUint64(&o.Value)
	o.Delta, o.previous = current - o.previous, current
}

type ReplicationStatus uint64

type ReplicationMetric struct {
	NAME      string
	SUBSCRIBE uint64

	OplogFilter     MetricDelta
	OplogGet        MetricDelta
	OplogConsume    MetricDelta
	OplogApply      MetricDelta
	OplogSuccess    MetricDelta
	OplogFail       MetricDelta
	CheckpointTimes uint64
	Retransmission  uint64
	TunnelTraffic   uint64
	LSN             int64
	LSNAck          int64
	LSNCheckpoint   int64

	OplogMaxSize  int64
	OplogAvgSize  int64

	TableOperations *TableOps

	// replication status
	ReplStatus ReplicationStatus
}

//var Metric *ReplicationMetric

func NewMetric(name string, subscribe uint64) *ReplicationMetric {
	metric := &ReplicationMetric{}
	metric.NAME = name
	metric.SUBSCRIBE = subscribe
	metric.startup()
	return metric
}

const (
	METRIC_NONE            = 0x0000000000000000
	METRIC_CKPT_TIMES      = 0x0000000000000001
	METRIC_TUNNEL_TRAFFIC  = 0x0000000000000010
	METRIC_LSN_CKPT        = 0x0000000000000100
	METRIC_RETRANSIMISSION = 0x0000000000001000
	METRIC_TPS             = 0x0000000000010000
	METRIC_SUCCESS         = 0x0000000000100000
)

func (metric *ReplicationMetric) init() {
	metric.TableOperations = NewTableOps()
}

func (metric *ReplicationMetric) resetEverySecond(items []*MetricDelta) {
	for _, item := range items {
		item.Update()
	}
}

func (metric *ReplicationMetric) startup() {
	metric.init()
	go func() {
		tick := 0
		// items that need be reset
		resetItems := []*MetricDelta{&metric.OplogSuccess}
		for range time.NewTicker(1 * time.Second).C {
			tick++
			metric.resetEverySecond(resetItems)
			if tick%FrequentInSeconds != 0 {
				continue
			}

			ckpt := atomic.LoadUint64(&metric.CheckpointTimes)
			lsnCkpt := atomic.LoadInt64(&metric.LSNCheckpoint)
			retransimission := atomic.LoadUint64(&metric.Retransmission)
			tps := atomic.LoadUint64(&metric.OplogSuccess.Delta)
			success := atomic.LoadUint64(&metric.OplogSuccess.Value)
			verbose := "[name=%s, filter=%d, get=%d, consume=%d, apply=%d, failed_times=%d"
			if metric.SUBSCRIBE&METRIC_SUCCESS != 0 {
				verbose += fmt.Sprintf(", success=%d", success)
			}
			if metric.SUBSCRIBE&METRIC_TPS != 0 {
				verbose += fmt.Sprintf(", tps=%d", tps)
			}
			if metric.SUBSCRIBE&METRIC_CKPT_TIMES != 0 {
				verbose += fmt.Sprintf(", ckpt_times=%d", ckpt)
			}
			if metric.SUBSCRIBE&METRIC_RETRANSIMISSION != 0 {
				verbose += fmt.Sprintf(", retransimit_times=%d", retransimission)
			}
			if metric.SUBSCRIBE&METRIC_TUNNEL_TRAFFIC != 0 {
				verbose += fmt.Sprintf(", tunnel_traffic=%s", metric.getTunnelTraffic())
			}
			if metric.SUBSCRIBE&METRIC_LSN_CKPT != 0 {
				verbose += fmt.Sprintf(", lsn_ckpt={%d,%s}", ExtractMongoTimestamp(lsnCkpt), TimestampToString(ExtractMongoTimestamp(lsnCkpt)))
			}
			verbose += ", lsn_ack={%d,%s}]"

			LOG.Info(verbose, metric.NAME,
				atomic.LoadUint64(&metric.OplogFilter.Value),
				atomic.LoadUint64(&metric.OplogGet.Value),
				atomic.LoadUint64(&metric.OplogConsume.Value),
				atomic.LoadUint64(&metric.OplogApply.Value),
				atomic.LoadUint64(&metric.OplogFail.Value),
				ExtractMongoTimestamp(atomic.LoadInt64(&metric.LSNAck)),
				TimestampToString(ExtractMongoTimestamp(atomic.LoadInt64(&metric.LSNAck))))
		}
	}()
}

func (metric *ReplicationMetric) getTunnelTraffic() string {
	traffic := atomic.LoadUint64(&metric.TunnelTraffic)
	switch {
	case traffic > PB:
		return fmt.Sprintf("%dPB", traffic/PB)
	case traffic > TB:
		return fmt.Sprintf("%dTB", traffic/TB)
	case traffic > GB:
		return fmt.Sprintf("%dGB", traffic/GB)
	case traffic > MB:
		return fmt.Sprintf("%dMB", traffic/MB)
	case traffic > KB:
		return fmt.Sprintf("%dKB", traffic/KB)
	default:
		return fmt.Sprintf("%dB", traffic)
	}
}

func (metric *ReplicationMetric) Get() uint64 {
	return atomic.LoadUint64(&metric.OplogGet.Value)
}

func (metric *ReplicationMetric) Apply() uint64 {
	return atomic.LoadUint64(&metric.OplogApply.Value)
}

func (metric *ReplicationMetric) Success() uint64 {
	return atomic.LoadUint64(&metric.OplogSuccess.Value)
}

func (metric *ReplicationMetric) AddSuccess(incr uint64) {
	atomic.AddUint64(&metric.OplogSuccess.Value, incr)
}

func (metric *ReplicationMetric) AddGet(incr uint64) {
	atomic.AddUint64(&metric.OplogGet.Value, incr)
}

func (metric *ReplicationMetric) AddCheckpoint(number uint64) {
	atomic.AddUint64(&metric.CheckpointTimes, number)
}

func (metric *ReplicationMetric) AddRetransmission(number uint64) {
	atomic.AddUint64(&metric.Retransmission, number)
}

func (metric *ReplicationMetric) AddTunnelTraffic(number uint64) {
	atomic.AddUint64(&metric.TunnelTraffic, number)
}

func (metric *ReplicationMetric) AddFilter(incr uint64) {
	atomic.AddUint64(&metric.OplogFilter.Value, incr)
}

func (metric *ReplicationMetric) AddApply(incr uint64) {
	atomic.AddUint64(&metric.OplogApply.Value, incr)
}

func (metric *ReplicationMetric) AddFailed(incr uint64) {
	atomic.AddUint64(&metric.OplogFail.Value, incr)
}

func (metric *ReplicationMetric) AddConsume(incr uint64) {
	atomic.AddUint64(&metric.OplogConsume.Value, incr)
}

func (metric *ReplicationMetric) SetOplogMax(max int64) {
	forwardCas(&metric.OplogMaxSize, max)
}

func (metric *ReplicationMetric) SetOplogAvg(size int64) {
	// not atomic update ! acceptable
	avg := (atomic.LoadInt64(&metric.OplogAvgSize) + size) / 2
	atomic.StoreInt64(&metric.OplogAvgSize, avg)
}

func (metric *ReplicationMetric) SetLSNCheckpoint(ckpt int64) {
	forwardCas(&metric.LSNCheckpoint, ckpt)
}

func (metric *ReplicationMetric) SetLSN(lsn int64) {
	forwardCas(&metric.LSN, lsn)
}

func (metric *ReplicationMetric) SetLSNACK(ack int64) {
	forwardCas(&metric.LSNAck, ack)
}

func (metric *ReplicationMetric) AddTableOps(table string, n uint64) {
	metric.TableOperations.Incr(table, n)
}

func (metric *ReplicationMetric) TableOps() map[string]uint64 {
	return metric.TableOperations.MakeCopy()
}

func forwardCas(v *int64, new int64) {
	var current int64
	for current = atomic.LoadInt64(v); new > current; {
		if atomic.CompareAndSwapInt64(v, current, new) {
			break
		}
		current = atomic.LoadInt64(v)
	}
}

func (status *ReplicationStatus) Update(s uint64) {
	atomic.StoreUint64((*uint64)(status), s)
}

func (status *ReplicationStatus) Clear(s uint64) {
	atomic.CompareAndSwapUint64((*uint64)(status), s, WorkGood)
}

func (status *ReplicationStatus) GetStatusString() string {
	return RunStatusMessage(uint64(*status))
}

func (status *ReplicationStatus) IsGood() bool {
	return uint64(*status) == WorkGood || uint64(*status) == GetReady
}

// TableOps, count collection operations
type TableOps struct {
	sync.Mutex
	ops map[string]uint64
}

func NewTableOps() *TableOps {
	return &TableOps{ops: make(map[string]uint64)}
}

func (t *TableOps) Incr(table string, n uint64) {
	t.Lock()
	defer t.Unlock()
	t.ops[table] += n
}

func (t *TableOps) MakeCopy() map[string]uint64 {
	t.Lock()
	defer t.Unlock()
	c := make(map[string]uint64, len(t.ops))
	for k, v := range t.ops {
		c[k] = v
	}
	return c
}
