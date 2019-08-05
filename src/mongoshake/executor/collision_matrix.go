package executor

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"mongoshake/oplog"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const MultiColumnIndexSplitter = "|"

type OplogRecord struct {
	original *PartialLogWithCallbak

	// wait() procedure will stop to wait for the dependent OplogRecord
	// that operate the equivalent index values ahead of the this one
	// we would insert sync mutex here !
	wait func()
}

type CollisionMatrix interface {
	// split oplogs into safety segments
	split(logs []*PartialLogWithCallbak) [][]*PartialLogWithCallbak

	convert(segment []*PartialLogWithCallbak) []*OplogRecord
}

type NoopMatrix struct{}

func (noop *NoopMatrix) split(logs []*PartialLogWithCallbak) [][]*PartialLogWithCallbak {
	return [][]*PartialLogWithCallbak{logs}
}

func (noop *NoopMatrix) convert(segment []*PartialLogWithCallbak) []*OplogRecord {
	records := make([]*OplogRecord, len(segment), len(segment))
	for index, log := range segment {
		// nothing to change !
		records[index] = &OplogRecord{original: log, wait: nil}
	}
	return records
}

type OplogUniqueIdentifier struct {
	signatureTable []string

	// sequence id. order by oplogs timing
	order int

	// key is unique index column name. value is index values
	operations map[string]interface{}
}

func (oplogUniqueIdentifier *OplogUniqueIdentifier) addSignature(signature string) {
	// for every field. sum name and value signature value
	oplogUniqueIdentifier.signatureTable = append(oplogUniqueIdentifier.signatureTable, signature)
}

func fillupOperationValues(log *PartialLogWithCallbak) {
	if log.partialLog.Operation != "i" && log.partialLog.Operation != "u" {
		return
	}

	// for update. need to initialize the UniqueIndexesUpdates
	if log.partialLog.Operation == "u" {
		log.partialLog.UniqueIndexesUpdates = bson.M{}
	}

	o := log.partialLog.Object
	for k := range log.partialLog.UniqueIndexes {
		// multi key index like "name|phone", seperate by MultiColumnIndexSplitter
		// every index value should be fetched respectively from oplog.o
		for _, singleIndex := range strings.Split(k, MultiColumnIndexSplitter) {
			// single index may be "aaa" or "aaa.bbb.ccc"
			parent, _ := oplog.ConvertBsonD2M(o)
			// all types of $inc, $mul, $rename, $unset, $set change to $set,$unset in oplog
			// $set looks like o:{$set:{a:{b:1}}} or o:{$set:{"a.b":1}}
			if m, exist := parent["$set"]; exist {
				if child, ok := m.(bson.M); ok {
					// skip $set operator
					parent = child
				}
			}

			var value interface{}
			// check if there already has "a.b.c" in $set
			if v, exist := parent[singleIndex]; exist {
				value = v
			} else {
				cascades := strings.Split(singleIndex, ".")
				descend := len(cascades) - 1
				// going down
				inPosition := true
				for i := 0; i != descend; i++ {
					if down, ok := parent[cascades[i]].(bson.M); ok {
						parent = down
					} else {
						inPosition = false
					}
				}
				if inPosition {
					value = parent[cascades[len(cascades)-1]]
				}
			}

			var fill interface{}
			switch log.partialLog.Operation {
			case "i":
				fill = log.partialLog.UniqueIndexes[k]
			case "u":
				fill = log.partialLog.UniqueIndexesUpdates[k]
			}

			if fill == nil {
				fill = make([]interface{}, 0)
			}

			if array, ok := fill.([]interface{}); ok {
				// doesn't find the target value. just fill NULL into it
				array = append(array, value)

				// reset to uk
				switch log.partialLog.Operation {
				case "i":
					log.partialLog.UniqueIndexes[k] = array
				case "u":
					log.partialLog.UniqueIndexesUpdates[k] = array
				}
			}
		}
	}
}

func newUniqueIdentifier(order int, log *PartialLogWithCallbak) *OplogUniqueIdentifier {
	nimo.AssertTrue(len(log.partialLog.UniqueIndexes) != 0, "make identifier of empty indexes wrong")

	// first of all, we need to fill up the oplog.uk colum field
	// if the operation is equal to "i","u"
	fillupOperationValues(log)

	// construct identifier
	identifier := &OplogUniqueIdentifier{}
	identifier.order = order
	for columnName, columnValue := range log.partialLog.UniqueIndexes {
		// if column value is an array of sub values. the size of array should be exactly equals
		// for MongoDB same index. so we don't counting the size of array into "signature"
		identifier.addSignature(fmt.Sprintf("%f", calculateSignature(columnName)+calculateSignature(columnValue)))
	}

	// for update only
	if log.partialLog.Operation == "u" {
		for columnName, columnValue := range log.partialLog.UniqueIndexesUpdates {
			identifier.addSignature(fmt.Sprintf("%f", calculateSignature(columnName)+calculateSignature(columnValue)))
		}
	}

	identifier.operations = log.partialLog.UniqueIndexes
	return identifier
}

func calculateSignature(object interface{}) (sign float64) {
	if object == nil {
		// prime number. different from boolean
		return 3
	}

	switch o := object.(type) {
	case bson.M: // recursive
		for k, v := range o {
			sign += calculateSignature(k)
			sign += calculateSignature(v)
		}
	case []interface{}: // recursive
		for _, v := range o {
			sign += calculateSignature(v)
		}
	case bson.Binary: // byte array
		for _, c := range o.Data {
			// consult from Java String.hashcode()
			sign = 31.0*sign + float64(c)
		}
	case string:
		for _, c := range o {
			// consult from Java String.hashcode()
			sign = 31.0*sign + float64(c)
		}
	case []byte:
		for _, c := range o {
			// consult from Java String.hashcode()
			sign = 31.0*sign + float64(c)
		}
	case bson.MongoTimestamp: // numbers
		sign = float64(o)
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
		if v, ok := object.(float64); ok {
			sign = float64(v)
		}

	case bool: // boolean
		sign++
		if o {
			sign++
		}
	default:
		nimo.Assert(fmt.Sprintf("bson value signature type is not recognized [%d, %s]", reflect.TypeOf(object).Kind(),
			reflect.TypeOf(object).Name()))
		LOG.Critical("Bson value signature type is not recognized [%d, %s]", reflect.TypeOf(object).Kind(),
			reflect.TypeOf(object).Name())

		// default value represents may be conflict. the signature is unsafe. just speed up !
		// so simply sum zero value here
		return 0
	}

	return sign
}

func ExactlyMatch(first, second interface{}) bool {
	if (first != nil && second == nil) || (first == nil && second != nil) ||
		reflect.TypeOf(first).Kind() != reflect.TypeOf(second).Kind() {
		return false
	}

	// both of them are null is accept
	if first == nil && second == nil {
		return true
	}

	switch o := first.(type) {
	case bson.M: // recursive
		for key := range o {
			if v, ok := second.(map[string]interface{}); ok && !ExactlyMatch(o[key], v[key]) {
				return false
			}
		}
	case []interface{}: // recursive
		if v, ok := second.([]interface{}); ok {
			if len(o) != len(v) {
				return false
			}
		}
		for i := range o {
			if v, ok := second.([]interface{}); ok && !ExactlyMatch(o[i], v[i]) {
				return false
			}
		}
	case []byte: // byte array
		if v, ok := second.([]byte); ok {
			return bytes.Compare(o, v) == 0
		}
	case bson.Binary:
		if v, ok := second.(bson.Binary); ok {
			return bytes.Compare(o.Data, v.Data) == 0
		}
	case string:
		if v, ok := second.(string); ok {
			return o == v
		}
	case bson.MongoTimestamp: // numbers
		if v, ok := second.(bson.MongoTimestamp); ok {
			return uint64(o) == uint64(v)
		}
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
		if v1, ok := first.(float64); ok {
			if v2, ok := second.(float64); ok {
				return v1 == v2
			}
		}
	case bool: // boolean
		if v, ok := second.(bool); ok {
			return o == v
		}
	default:
		nimo.Assert(fmt.Sprintf("bson value check similar. not recognized [%s]", reflect.TypeOf(first).Name()))
		LOG.Critical("bson value check similar. not recognized [%s]", reflect.TypeOf(first).Name())
	}

	// TODO: execute path may be walked from DEFAULT we meet something that the type
	// we don't know now. for safety, we treat these oplogs' unique indexes as equal exactly !!
	// This may cause more segments splitted but more safe.
	return true
}

func intersectionInOrder(one bson.M, other bson.M) bool {
	// TODO: BSON's object or array is already sorted ?! we can get the intersection more easier
	// BUT for now. we don't make such assumption. so make it simple, we iterate every field
	// in Map. and compare to another
	for k1, v1 := range one {
		if v2, exist := other[k1]; exist {
			return ExactlyMatch(v1, v2)
		}
	}
	return false
}

func intersection(this bson.M, other bson.M) bool {
	if len(this) == 0 || len(other) == 0 {
		return false
	}

	// compare each other
	return intersectionInOrder(this, other) || intersectionInOrder(other, this)
}

func haveMutualIndex(first, second *oplog.PartialLog) bool {
	if first == second {
		return false
	}

	var firstId, secondId bson.ObjectId
	var got bool
	firstId, got = oplog.GetIdOrNSFromOplog(first).(bson.ObjectId)
	secondId, got = oplog.GetIdOrNSFromOplog(second).(bson.ObjectId)

	if got && firstId.Hex() == secondId.Hex() {
		// oplogs operate the single MongoDB record. they should be serialized by executor
		return false
	}

	// unified approach for insert, update, delete
	return intersection(first.UniqueIndexes, second.UniqueIndexes) ||
		intersection(first.UniqueIndexes, second.UniqueIndexesUpdates) ||
		intersection(first.UniqueIndexesUpdates, second.UniqueIndexes) ||
		intersection(first.UniqueIndexesUpdates, second.UniqueIndexesUpdates)
}

// BarrierMatrix only split oplogs into segments. without convert stage stage.
type BarrierMatrix struct {
	NoopMatrix

	//	original []*PartialLogWithCallbak
}

func NewBarrierMatrix() *BarrierMatrix {
	return &BarrierMatrix{NoopMatrix{}}
}

func (*BarrierMatrix) split(logs []*PartialLogWithCallbak) [][]*PartialLogWithCallbak {
	var segmentList [][]*PartialLogWithCallbak
	var seg []*PartialLogWithCallbak
	var identifier *OplogUniqueIdentifier
	signatureSet := make(map[string][]*PartialLogWithCallbak)
	for i, log := range logs {
		// put the log into segment straightly if no unique index operations found
		if len(log.partialLog.UniqueIndexes) != 0 {
			identifier = newUniqueIdentifier(i, log)
			for _, current := range identifier.signatureTable {
				if potentialCandidates, exist := signatureSet[current]; exist {
					// found possible conflict keys
					for _, candidate := range potentialCandidates {
						// candidate and the "log" may be the same one !
						if haveMutualIndex(candidate.partialLog, log.partialLog) {
							// add this signature to duplication list
							segmentList = append(segmentList, seg)
							seg = nil
							break
						}
					}
					LOG.Warn("Logs have same identifier signature. but don't exactly match. signature : %s", current)
				}
				signatureSet[current] = append(signatureSet[current], log)
			}
		}

		seg = append(seg, log)
	}

	if len(seg) != 0 {
		segmentList = append(segmentList, seg)
	}

	LOG.Info("Barrier matrix split vector to length %d", len(segmentList))
	return segmentList
}

func (barrier *BarrierMatrix) convert(segment []*PartialLogWithCallbak) []*OplogRecord {
	return barrier.NoopMatrix.convert(segment)
}
