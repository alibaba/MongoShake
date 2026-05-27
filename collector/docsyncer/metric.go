package docsyncer

import (
	"fmt"
	"sync/atomic"
)

type Status int32

const (
	StatusWaitStart Status = iota
	StatusProcessing
	StatusFinish
)

type CollectionMetric struct {
	CollectionStatus int32
	TotalCount       uint64
	FinishCount      uint64
}

func NewCollectionMetric() *CollectionMetric {
	return &CollectionMetric{
		CollectionStatus: int32(StatusWaitStart),
	}
}

func (cm *CollectionMetric) Status() Status {
	return Status(atomic.LoadInt32(&cm.CollectionStatus))
}

func (cm *CollectionMetric) SetStatus(status Status) {
	atomic.StoreInt32(&cm.CollectionStatus, int32(status))
}

func (cm *CollectionMetric) String() string {
	totalCount := atomic.LoadUint64(&cm.TotalCount)
	finishCount := atomic.LoadUint64(&cm.FinishCount)
	status := cm.Status()
	if status == StatusWaitStart {
		return fmt.Sprintf("-")
	}

	if totalCount == 0 {
		return fmt.Sprintf("100%% (%v/%v)", finishCount, totalCount)
	} else {
		return fmt.Sprintf("%.2f%% (%v/%v)", float64(finishCount)/float64(totalCount)*100,
			finishCount, totalCount)
	}
}

func (cm *CollectionMetric) StatusCode() float64 {
	switch cm.Status() {
	case StatusProcessing:
		return 1
	case StatusFinish:
		return 2
	default:
		return 0
	}
}

func (cm *CollectionMetric) ProgressRatio() float64 {
	totalCount := atomic.LoadUint64(&cm.TotalCount)
	finishCount := atomic.LoadUint64(&cm.FinishCount)
	status := cm.Status()
	if status == StatusWaitStart {
		return 0
	}
	if status == StatusFinish || totalCount == 0 {
		return 1
	}
	return float64(finishCount) / float64(totalCount)
}
