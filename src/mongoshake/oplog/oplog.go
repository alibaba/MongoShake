package oplog

import (
	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo/bson"
	"reflect"
)

const (
	PrimaryKey = "_id"
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type PartialLog struct {
	Timestamp     bson.MongoTimestamp `bson:"ts"`
	Operation     string              `bson:"op"`
	Gid           string              `bson:"g"`
	Namespace     string              `bson:"ns"`
	Object        bson.D              `bson:"o"`
	Query         bson.M              `bson:"o2"`
	UniqueIndexes bson.M              `bson:"uk"`
	Lsid          interface{}         `bson:"lsid"`        // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate"` // move chunk

	/*
	 * Every field subsequent declared is NEVER persistent or
	 * transfer on network connection. They only be parsed from
	 * respective logic
	 */
	UniqueIndexesUpdates bson.M // generate by CollisionMatrix
	RawSize              int    // generate by Decorator
	SourceId             int    // generate by Validator
}

func LogEntryEncode(logs []*GenericOplog) [][]byte {
	var encodedLogs [][]byte
	// log entry encode
	for _, log := range logs {
		encodedLogs = append(encodedLogs, log.Raw)
	}
	return encodedLogs
}

func LogParsed(logs []*GenericOplog) []*PartialLog {
	parsedLogs := make([]*PartialLog, len(logs), len(logs))
	for i, log := range logs {
		parsedLogs[i] = log.Parsed
	}
	return parsedLogs
}

func NewPartialLog(data bson.M) *PartialLog {
	partialLog := new(PartialLog)
	logType := reflect.TypeOf(*partialLog)
	for i := 0; i < logType.NumField(); i++ {
		tagName := logType.Field(i).Tag.Get("bson")
		if v, ok := data[tagName]; ok {
			reflect.ValueOf(partialLog).Elem().Field(i).Set(reflect.ValueOf(v))
		}
	}
	return partialLog
}

// dump according to the given keys, "keys" == nil means ignore keys
func (partialLog *PartialLog) Dump(keys map[string]struct{}) bson.D {
	var out bson.D
	logType := reflect.TypeOf(*partialLog)
	for i := 0; i < logType.NumField(); i++ {
		if tagName, ok := logType.Field(i).Tag.Lookup("bson"); ok {
			// out[tagName] = reflect.ValueOf(partialLog).Elem().Field(i).Interface()
			value := reflect.ValueOf(partialLog).Elem().Field(i).Interface()
			if keys == nil {
				if _, ok := keys[tagName]; !ok {
					continue
				}
			}
			out = append(out, bson.DocElem{tagName, value})
		}
	}

	return out
}

func GetKey(log bson.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(log, wanted)
	return ret
}

func GetKeyWithIndex(log bson.D, wanted string) (interface{}, int) {
	if wanted == "" {
		wanted = PrimaryKey
	}

	// "_id" is always the first field
	for id, ele := range log {
		if ele.Name == wanted {
			return ele.Value, id
		}
	}

	nimo.Assert("you can't see me")
	return nil, 0
}

// convert bson.D to bson.M
func ConvertBsonD2M(input bson.D) (bson.M, map[string]struct{}) {
	m := bson.M{}
	keys := make(map[string]struct{}, len(input))
	for _, ele := range input {
		m[ele.Name] = ele.Value
		keys[ele.Name] = struct{}{}
	}

	return m, keys
}

func RemoveFiled(input bson.D, key string) bson.D {
	flag := -1
	for id := range input {
		if input[id].Name == key {
			flag = id
			break
		}
	}

	if flag != -1 {
		input = append(input[:flag], input[flag+1:]...)
	}
	return input
}

func SetFiled(input bson.D, key string, value interface{}) {
	for i, ele := range input {
		if ele.Name == key {
			input[i].Value = value
		}
	}
}
