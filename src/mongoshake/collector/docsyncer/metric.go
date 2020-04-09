package docsyncer

import "fmt"

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
	if cm.CollectionStatus == StatusWaitStart {
		return fmt.Sprintf("-")
	}

	if cm.TotalCount == 0 {
		return fmt.Sprintf("100%% (%v/%v)", cm.FinishCount, cm.TotalCount)
	} else {
		return fmt.Sprintf("%.2f%% (%v/%v)", float64(cm.FinishCount) / float64(cm.TotalCount) * 100,
			cm.FinishCount, cm.TotalCount)
	}
}