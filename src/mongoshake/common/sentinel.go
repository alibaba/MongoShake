package utils

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

// SentinelOptions. option's value type should be
// String or Bool or Int64
var SentinelOptions struct {
	OplogDump      int64
	DuplicatedDump bool
	Pause          bool
	TPS            int64
}

type Sentinel struct {
}

func (sentinel *Sentinel) Register() {
	HttpApi.RegisterAPI("/sentinel", nimo.HttpGet, func([]byte) interface{} {
		return SentinelOptions
	})

	HttpApi.RegisterAPI("/sentinel/options", nimo.HttpPost, func(body []byte) interface{} {
		// check the exist of every option. options will be configured only
		// if all the header kv pair are exist! this means that we ensure the
		// operation consistency
		kv := make(map[string]interface{})
		if err := json.Unmarshal(body, &kv); err != nil {
			LOG.Info("Register set options wrong format : %v", err)
			return map[string]string{"sentinel": "request json options wrong format"}
		}
		for name := range kv {
			if !reflect.ValueOf(&SentinelOptions).Elem().FieldByName(name).IsValid() {
				return map[string]string{"sentinel": fmt.Sprintf("%s is not exist", name)}
			}
		}

		for name, value := range kv {
			field := reflect.ValueOf(&SentinelOptions).Elem().FieldByName(name)
			switch field.Kind() {
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					field.SetBool(v)
					continue
				}
			case reflect.Int64:
				//fmt.Printf("%v, %s", value, reflect.TypeOf(value).String())
				if v, ok := value.(float64); ok {
					field.SetInt(int64(v))
					continue
				}
			case reflect.String:
				if v, ok := value.(string); ok {
					field.SetString(v)
					continue
				}
			default:
			}
			return map[string]string{"sentinel": fmt.Sprintf("%s option isn't corret", name)}
		}
		return map[string]string{"sentinel": "success"}
	})
}
