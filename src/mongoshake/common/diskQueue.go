package utils

import "fmt"

const (
	FetchStageStoreUnknown     int32 = 0
	FetchStageStoreDiskNoApply int32 = 1
	FetchStageStoreDiskApply   int32 = 2
	FetchStageStoreMemoryApply int32 = 3
)

func LogFetchStage(stage int32) string {
	switch stage {
	case FetchStageStoreDiskNoApply:
		return "store disk and no apply"
	case FetchStageStoreDiskApply:
		return "store disk and apply"
	case FetchStageStoreMemoryApply:
		return "store memory and apply"
	default:
		return fmt.Sprintf("invalid[%v]", stage)
	}
}