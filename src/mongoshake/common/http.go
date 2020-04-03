package utils

import (
	"github.com/gugemichael/nimo4go"
)

var (
	FullSyncHttpApi *nimo.HttpRestProvider
	IncrSyncHttpApi *nimo.HttpRestProvider
)

func FullSyncInitHttpApi(port int) {
	FullSyncHttpApi = nimo.NewHttpRestProvdier(port)
}

func IncrSyncInitHttpApi(port int) {
	IncrSyncHttpApi = nimo.NewHttpRestProvdier(port)
}
