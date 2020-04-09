package utils

import (
	"github.com/gugemichael/nimo4go"
)

var (
	FullSyncHttpApi *nimo.HttpRestProvider
	IncrSyncHttpApi *nimo.HttpRestProvider
)

func FullSyncInitHttpApi(port int) {
	FullSyncHttpApi = nimo.NewHttpRestProvider(port)
}

func IncrSyncInitHttpApi(port int) {
	IncrSyncHttpApi = nimo.NewHttpRestProvider(port)
}
