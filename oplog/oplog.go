package oplog

import (
	"encoding/json"
	"fmt"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"

	"strings"
)

const (
	PrimaryKey = "_id"
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type ParsedLog struct {
	Timestamp     primitive.Timestamp `bson:"ts" json:"ts"`
	Term          *int64              `bson:"t" json:"t"`
	Hash          *int64              `bson:"h" json:"h"`
	Version       int                 `bson:"v" json:"v"`
	Operation     string              `bson:"op" json:"op"`
	Gid           string              `bson:"g,omitempty" json:"g,omitempty"`
	Namespace     string              `bson:"ns" json:"ns"`
	Object        bson.D              `bson:"o" json:"o"`
	Query         bson.D              `bson:"o2" json:"o2"`                                       // update condition
	UniqueIndexes bson.M              `bson:"uk,omitempty" json:"uk,omitempty"`                   //
	LSID          bson.Raw            `bson:"lsid,omitempty" json:"lsid,omitempty"`               // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate,omitempty" json:"fromMigrate,omitempty"` // move chunk
	TxnNumber     *int64              `bson:"txnNumber,omitempty" json:"txnNumber,omitempty"`     // transaction number in session
	DocumentKey   bson.D              `bson:"documentKey,omitempty" json:"documentKey,omitempty"` // exists when source collection is sharded, only including shard key and _id
	PrevOpTime    bson.Raw            `bson:"prevOpTime,omitempty"`
	UI            *primitive.Binary   `bson:"ui,omitempty" json:"ui,omitempty"` // do not enable currently
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
	encodedLogs := make([][]byte, 0, len(logs))
	// log entry encode
	for _, log := range logs {
		if log.Raw == nil {
			if out, err := bson.Marshal(log.Parsed); err != nil {
				LOG.Crashf("LogEntryEncode marshal Oplog[%v] failed[%v]", log.Parsed, err)
			} else {
				encodedLogs = append(encodedLogs, out)
			}
		} else {
			encodedLogs = append(encodedLogs, log.Raw)
		}
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
	parsedLog := new(ParsedLog)
	logType := reflect.TypeOf(*parsedLog)
	for i := 0; i < logType.NumField(); i++ {
		tagNameWithOption := logType.Field(i).Tag.Get("bson")
		tagName := strings.Split(tagNameWithOption, ",")[0]
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
		if tagNameWithOption, ok := logType.Field(i).Tag.Lookup("bson"); ok {
			// out[tagName] = reflect.ValueOf(partialLog).Elem().Field(i).Interface()
			value := reflect.ValueOf(partialLog.ParsedLog).Field(i).Interface()
			tagName := strings.Split(tagNameWithOption, ",")[0]
			if !all {
				if _, ok := keys[tagName]; !ok {
					continue
				}
			}
			out = append(out, primitive.E{tagName, value})
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
		if ele.Key == wanted {
			return ele.Value, id
		}
	}

	return nil, 0
}

func ConvertBsonD2MExcept(input bson.D, except map[string]struct{}) (bson.M, map[string]struct{}) {
	m := bson.M{}
	keys := make(map[string]struct{}, len(input))
	for _, ele := range input {
		switch ele.Value.(type) {
		case bson.D:
			if _, ok := except[ele.Key]; ok {
				m[ele.Key] = ele.Value
			} else {
				son, _ := ConvertBsonD2M(ele.Value.(bson.D))
				m[ele.Key] = son
			}
		default:
			m[ele.Key] = ele.Value
		}

		keys[ele.Key] = struct{}{}
	}

	return m, keys
}

// convert bson.D to bson.M
func ConvertBsonD2M(input bson.D) (bson.M, map[string]struct{}) {
	m := bson.M{}
	keys := make(map[string]struct{}, len(input))
	for _, ele := range input {
		m[ele.Key] = ele.Value
		switch ele.Value.(type) {
		case bson.D:
			son, _ := ConvertBsonD2M(ele.Value.(bson.D))
			m[ele.Key] = son
		default:
			m[ele.Key] = ele.Value
		}
		keys[ele.Key] = struct{}{}
	}

	return m, keys
}

func ConvertBsonM2D(input bson.M) bson.D {
	output := make(bson.D, 0, len(input))
	for key, val := range input {
		output = append(output, primitive.E{
			Key:   key,
			Value: val,
		})
	}
	return output
}

// pay attention: the input bson.D will be modified.
func RemoveFiled(input bson.D, key string) bson.D {
	flag := -1
	for id := range input {
		if input[id].Key == key {
			flag = id
			break
		}
	}

	if flag != -1 {
		input = append(input[:flag], input[flag+1:]...)
	}
	return input
}

func FindFiledPrefix(input bson.D, prefix string) bool {
	for id := range input {
		if strings.HasPrefix(input[id].Key, prefix) {
			return true
		}
	}

	return false
}

func SetFiled(input bson.D, key string, value interface{}) {
	for i, ele := range input {
		if ele.Key == key {
			input[i].Value = value
		}
	}
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
			LSID:          input[0].LSID,
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
		primitive.E{
			Key:   "applyOps",
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

// Oplog from mongod(5.0) in sharding&replica
// {"ts":{"T":1653449035,"I":3},"v":2,"op":"u","ns":"test.bar",
//  "o":[{"Key":"diff","Value":[{"Key":"d","Value":[{"Key":"ok","Value":false}]},
//                              {"Key":"i","Value":[{"Key":"plus_field","Value":2}]}]}],
//  "o2":[{"Key":"_id","Value":"628da11482387c117d4e9e45"}]}

// "o" : { "$v" : 2, "diff" : { "d" : { "count" : false }, "u" : { "name" : "orange" }, "i" : { "c" : 11 } } }
func DiffUpdateOplogToNormal(updateObj bson.D) (interface{}, error) {

	diffObj := GetKey(updateObj, "diff")
	if diffObj == nil {
		return updateObj, fmt.Errorf("don't have diff field updateObj:[%v]", updateObj)
	}

	bsonDiffObj, ok := diffObj.(bson.D)
	if !ok {
		return updateObj, fmt.Errorf("diff field is not bson.D updateObj:[%v]", updateObj)
	}

	result, err := BuildUpdateDelteOplog("", bsonDiffObj)
	if err != nil {
		return updateObj, fmt.Errorf("parse diffOplog failed updateObj:[%v] err[%v]", updateObj, err)
	}

	return result, nil

}

func BuildUpdateDelteOplog(prefixField string, obj bson.D) (interface{}, error) {
	var result bson.D

	for _, ele := range obj {
		if ele.Key == "d" {
			result = append(result, primitive.E{
				Key:   "$unset",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if ele.Key == "i" || ele.Key == "u" {
			result = append(result, primitive.E{
				Key:   "$set",
				Value: combinePrefixField(prefixField, ele.Value)})

		} else if len(ele.Key) > 1 && ele.Key[0] == 's' {
			// s means subgroup field(array or nest)
			tmpPrefixField := ""
			if len(prefixField) == 0 {
				tmpPrefixField = ele.Key[1:]
			} else {
				tmpPrefixField = prefixField + "." + ele.Key[1:]
			}

			nestObj, err := BuildUpdateDelteOplog(tmpPrefixField, ele.Value.(bson.D))
			if err != nil {
				return obj, fmt.Errorf("parse ele[%v] failed, updateObj:[%v]", ele, obj)
			}
			if _, ok := nestObj.(mongo.Pipeline); ok {
				return nestObj, nil
			} else if _, ok := nestObj.(bson.D); ok {
				for _, nestObjEle := range nestObj.(bson.D) {
					result = append(result, nestObjEle)
				}
			} else {
				return obj, fmt.Errorf("unknown nest type ele[%v] updateObj:[%v] nestObj[%v]", ele, obj, nestObj)
			}

		} else if len(ele.Key) > 1 && ele.Key[0] == 'u' {
			result = append(result, primitive.E{
				Key: "$set",
				Value: bson.D{
					primitive.E{
						Key:   prefixField + "." + ele.Key[1:],
						Value: ele.Value,
					},
				},
			})

		} else if ele.Key == "l" {
			if len(result) != 0 {
				return obj, fmt.Errorf("len should be 0, Key[%v] updateObj:[%v], result:[%v]",
					ele, obj, result)
			}

			return mongo.Pipeline{
				{{"$set", bson.D{
					{prefixField, bson.D{
						{"$slice", []interface{}{"$" + prefixField, ele.Value}},
					}},
				}}},
			}, nil

		} else if ele.Key == "a" && ele.Value == true {
			continue
		} else {
			return obj, fmt.Errorf("unknow Key[%v] updateObj:[%v]", ele, obj)
		}
	}

	return result, nil
}

func combinePrefixField(prefixField string, obj interface{}) interface{} {
	if len(prefixField) == 0 {
		return obj
	}

	tmpObj, ok := obj.(bson.D)
	if !ok {
		return obj
	}

	var result bson.D
	for _, ele := range tmpObj {
		result = append(result, primitive.E{
			Key:   prefixField + "." + ele.Key,
			Value: ele.Value})
	}

	return result
}
