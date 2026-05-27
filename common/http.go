package utils

import (
	"fmt"
	"net/http"

	nimo "github.com/gugemichael/nimo4go"
)

var (
	FullSyncHttpApi   *nimo.HttpRestProvider
	IncrSyncHttpApi   *nimo.HttpRestProvider
	PrometheusHttpApi *PrometheusProvider
)

func FullSyncInitHttpApi(port int) {
	FullSyncHttpApi = nimo.NewHttpRestProvider(port)
}

func IncrSyncInitHttpApi(port int) {
	IncrSyncHttpApi = nimo.NewHttpRestProvider(port)
}

type PrometheusProvider struct {
	port    int
	handler http.Handler
}

func PrometheusInitHttpApi(port int) {
	PrometheusInitHttpApiWithName(port, "collector")
}

func PrometheusInitHttpApiWithName(port int, name string) {
	PrometheusHttpApi = &PrometheusProvider{
		port:    port,
		handler: PrometheusHandlerWithName(name),
	}
}

func (provider *PrometheusProvider) Listen() error {
	if provider == nil {
		return nil
	}
	return http.ListenAndServe(fmt.Sprintf(":%d", provider.port), provider.handler)
}

func IsHTTPPortEnabled(port int) bool {
	return port > 0
}
