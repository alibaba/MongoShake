package oplog

import (
	"reflect"
	"fmt"
	"encoding/json"

	"github.com/gugemichael/nimo4go"
	"github.com/vinllen/mgo/bson"

)

const (
	PrimaryKey = "_id"
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type ParsedLog struct {
	Timestamp     bson.MongoTimestamp `bson:"ts" json:"ts"`
	Operation     string              `bson:"op" json:"op"`
	Gid           string              `bson:"g" json:"g"`
	Namespace     string              `bson:"ns" json:"ns"`
	Object        bson.D              `bson:"o" json:"o"`
	Query         bson.M              `bson:"o2" json:"o2"`
	UniqueIndexes bson.M              `bson:"uk" json:"uk"`
	Lsid          interface{}         `bson:"lsid" json:"lsid"`               // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate" json:"fromMigrate"` // move chunk
}

type PartialLog struct {
	ParsedLog

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
	// partialLog := new(PartialLog)
	parsedLog := new(ParsedLog)
	logType := reflect.TypeOf(*parsedLog)
	for i := 0; i < logType.NumField(); i++ {
		tagName := logType.Field(i).Tag.Get("bson")
		if v, ok := data[tagName]; ok {
			reflect.ValueOf(parsedLog).Elem().Field(i).Set(reflect.ValueOf(v))
		}
	}
	return &PartialLog{
		ParsedLog: *parsedLog,
	}
}

func (partialLog *PartialLog) String() string {
	if ret, err := json.Marshal(partialLog.ParsedLog); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

// dump according to the given keys, "all" == true means ignore keys
func (partialLog *PartialLog) Dump(keys map[string]struct{}, all bool) bson.D {
	var out bson.D
	logType := reflect.TypeOf(partialLog.ParsedLog)
	for i := 0; i < logType.NumField(); i++ {
		if tagName, ok := logType.Field(i).Tag.Lookup("bson"); ok {
			// out[tagName] = reflect.ValueOf(partialLog).Elem().Field(i).Interface()
			value := reflect.ValueOf(partialLog.ParsedLog).Field(i).Interface()
			if !all {
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

func ConvertBsonM2D(input bson.M) bson.D {
	output := make(bson.D, 0, len(input))
	for key, val := range input {
		output = append(output, bson.DocElem{
			Name:  key,
			Value: val,
		})
	}
	return output
}

// pay attention: the input bson.D will be modified.
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

func ParseTimestampFromBson(intput []byte) bson.MongoTimestamp {
	log := new(PartialLog)
	if err := bson.Unmarshal(intput, log); err != nil {
		return -1
	}
	return log.Timestamp
}

func GatherApplyOps(input []*PartialLog) (*GenericOplog, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("input list is empty")
	}

	newOplog := &PartialLog{
		ParsedLog: ParsedLog{
			Timestamp:     input[0].Timestamp,
			Operation:     "c",
			Gid:           input[0].Gid,
			Namespace:     "admin.$cmd",
			UniqueIndexes: input[0].UniqueIndexes,
			Lsid:          input[0].Lsid,
			FromMigrate:   input[0].FromMigrate,
		},
	}

	applyOpsList := make([]bson.M, 0, len(input))
	for _, ele := range input {
		applyOpsList = append(applyOpsList, bson.M{
			"op": ele.Operation,
			"ns": ele.Namespace,
			"o":  ele.Object,
			"o2": ele.Query,
		})
	}
	newOplog.Object = bson.D{
		bson.DocElem{
			Name:  "applyOps",
			Value: applyOpsList,
		},
	}

	if out, err := bson.Marshal(newOplog); err != nil {
		return nil, fmt.Errorf("marshal new oplog[%v] failed[%v]", newOplog, err)
	} else {
		return &GenericOplog{
			Raw:    out,
			Parsed: newOplog,
		}, nil
	}
}
