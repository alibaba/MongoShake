package nimo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	HttpGet    = "GET"
	HttpPost   = "POST"
	HttpUpdate = "UPDATE"
)

type HttpRestProvider struct {
	controller map[HttpURL][]HandlerFunc
	port       int
	serverMux  *http.ServeMux
}

type HandlerFunc func(body []byte) interface{}

type HttpURL struct {
	Uri    string
	Method string
}

func NewHttpRestProvider(port int) *HttpRestProvider {
	return &HttpRestProvider{
		controller: make(map[HttpURL][]HandlerFunc, 512),
		port:       port,
		serverMux:  http.NewServeMux(),
	}
}

func (rest *HttpRestProvider) Listen() error {
	for web, handlerList := range rest.controller {
		rest.register(web, handlerList)
	}
	// list overall registered http resource and uri
	rest.register(HttpURL{"/", HttpGet}, []HandlerFunc{func(body []byte) interface{} {
		var maps []HttpURL
		for contr := range rest.controller {
			maps = append(maps, contr)
		}
		return maps
	}})

	return http.ListenAndServe(fmt.Sprintf(":%d", rest.port), rest.serverMux)
}

func (rest *HttpRestProvider) RegisterAPI(url string, method string, handler func(body []byte) interface{}) {
	// http root url is responsible for show registered handlers
	if len(url) == 0 || url == "/" {
		return
	}
	identifier := HttpURL{Uri: url, Method: method}

	if _, exist := rest.controller[identifier]; exist {
		rest.controller[identifier] = append(rest.controller[identifier], handler)
	} else {
		rest.controller[identifier] = []HandlerFunc{handler}
	}
}

func (rest *HttpRestProvider) register(web HttpURL, handlerList []HandlerFunc) {
	rest.serverMux.HandleFunc(web.Uri, func(w http.ResponseWriter, req *http.Request) {
		if strings.ToUpper(req.Method) != strings.ToUpper(web.Method) {
			return
		}
		// read full body content
		body, _ := ioutil.ReadAll(req.Body)

		var v []byte
		var response interface{}
		if len(handlerList) == 1 {
			response = handlerList[0](body)
		} else {
			var results []interface{}
			// aggregate all results if multi-controller register
			for _, handler := range handlerList {
				results = append(results, handler(body))
			}
			response = results
		}
		v, _ = json.Marshal(response)
		w.Write(v)
	})
}
