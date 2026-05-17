package docsyncer

import (
	"fmt"
	"sync/atomic"
)

type Status string

const (
	StatusWaitStart  Status = "wait start"
	StatusProcessing Status = "in processing"
	StatusFinish     Status = "finish"
)

type CollectionMetric struct {
	CollectionStatus Status
	TotalCount       uint64
	FinishCount      uint64
}

func NewCollectionMetric() *CollectionMetric {
	return &CollectionMetric{
		CollectionStatus: StatusWaitStart,
	}
}

func (cm *CollectionMetric) String() string {
	totalCount := atomic.LoadUint64(&cm.TotalCount)
	finishCount := atomic.LoadUint64(&cm.FinishCount)
	if cm.CollectionStatus == StatusWaitStart {
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
	switch cm.CollectionStatus {
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
	if cm.CollectionStatus == StatusWaitStart {
		return 0
	}
	if cm.CollectionStatus == StatusFinish || totalCount == 0 {
		return 1
	}
	return float64(finishCount) / float64(totalCount)
}
