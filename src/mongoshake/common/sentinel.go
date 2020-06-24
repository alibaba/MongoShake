package utils

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

const (
	TypeFull = "full"
	TypeIncr = "incr"
)

// IncrSentinelOptions. option's value type should be
// String or Bool or Int64
// only used in incremental sync.
var IncrSentinelOptions struct {
	OplogDump      int64
	DuplicatedDump bool
	Pause          bool
	TPS            int64
	TargetDelay    int64
	ExitPoint      int64 // 32 bits timestamp
	Shutdown       bool  // close shake
}

// only used in full sync.
var FullSentinelOptions struct {
	TPS int64
}

func init() {
	IncrSentinelOptions.TargetDelay = -1
	IncrSentinelOptions.ExitPoint = -1
}

type Sentinel struct {
	tp string
}

func NewSentinel(tp string) *Sentinel {
	return &Sentinel{
		tp: tp,
	}
}

func (sentinel *Sentinel) getOptions() interface{} {
	switch sentinel.tp {
	case TypeFull:
		return &FullSentinelOptions
	case TypeIncr:
		return &IncrSentinelOptions
	}

	return nil
}

func (sentinel *Sentinel) getProvider() *nimo.HttpRestProvider {
	switch sentinel.tp {
	case TypeFull:
		return FullSyncHttpApi
	case TypeIncr:
		return IncrSyncHttpApi
	}

	return nil
}

func (sentinel *Sentinel) Register() {
	provider := sentinel.getProvider()

	provider.RegisterAPI("/sentinel", nimo.HttpGet, func([]byte) interface{} {
		return sentinel.getOptions()
	})

	provider.RegisterAPI("/sentinel/options", nimo.HttpPost, func(body []byte) interface{} {
		// check the exist of every option. options will be configured only
		// if all the header kv pair are exist! this means that we ensure the
		// operation consistency

		options := sentinel.getOptions()

		kv := make(map[string]interface{})
		if err := json.Unmarshal(body, &kv); err != nil {
			LOG.Info("Register set options wrong format : %v", err)
			return map[string]string{"sentinel": "request json options wrong format"}
		}
		for name := range kv {
			if !reflect.ValueOf(options).Elem().FieldByName(name).IsValid() {
				return map[string]string{"sentinel": fmt.Sprintf("%s is not exist", name)}
			}
		}

		for name, value := range kv {
			field := reflect.ValueOf(options).Elem().FieldByName(name)
			switch field.Kind() {
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					field.SetBool(v)
					continue
				}
			case reflect.Int64:
				// fmt.Printf("%s, %v, %s", name, value, reflect.TypeOf(value).String())
				if v, ok := value.(float64); ok {
					if name == "TargetDelay" && int64(v) < 0 {
						v = 0
					}
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

		LOG.Info("new sentinel options: %v", options)

		return map[string]string{"sentinel": "success"}
	})
}
