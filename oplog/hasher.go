package oplog

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
)

const (
	ShardByID        = "id"
	ShardByNamespace = "collection"
	ShardAutomatic   = "auto"
)

const (
	DefaultHashValue = uint32(0)
)

type Hasher interface {
	DistributeOplogByMod(log *PartialLog, mod int) uint32
}

// TableHasher hash by namespace
type TableHasher struct {
	Hasher
}

func (collectionHasher *TableHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	if mod == 1 {
		return 0
	}
	if len(log.Namespace) == 0 {
		return DefaultHashValue
	}

	// when oplog is DDL, go into worker 0.
	if log.Operation == "c" {
		return 0
	}

	return stringHashValue(log.Namespace) % uint32(mod)
}

// PrimaryKeyHasher will try to hash by _id firstly, {op:c} will use namespace instead
type PrimaryKeyHasher struct {
	Hasher
}

// DistributeOplogByMod
// We need to ensure that oplog entry will be sent to the same job[$hash] if they have the same ObjectID.
// thus we can consume the oplog entry sequentially
func (objectIdHasher *PrimaryKeyHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	if mod == 1 {
		return 0
	}

	var hashObject interface{}

	switch log.Operation {
	case "i", "d", "u", "c":
		hashObject = GetIdOrNSFromOplog(log)
	case "n":
		return DefaultHashValue
	}

	if hashObject == nil {
		_ = LOG.Warn("Couldn't extract hash object. collector has mixed up. use Oplog.Namespace instead %v", log)
		hashObject = log.Namespace
	}

	return Hash(hashObject) % uint32(mod)
}

// WhiteListObjectIdHasher
// 1) hash by collection in general;
// 2) hash by objectId only when hit whitelist;
type WhiteListObjectIdHasher struct {
	Hasher

	TableHasher
	PrimaryKeyHasher

	whiteList map[string]struct{} // no need to add lock, only reading operation
}

func NewWhiteListObjectIdHasher(whiteList []string) *WhiteListObjectIdHasher {
	mp := make(map[string]struct{}, len(whiteList))
	for _, ele := range whiteList {
		mp[ele] = struct{}{}
	}

	return &WhiteListObjectIdHasher{
		TableHasher:      TableHasher{},
		PrimaryKeyHasher: PrimaryKeyHasher{},
		whiteList:        mp,
	}
}

func (wloi *WhiteListObjectIdHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	ns := log.Namespace
	if len(ns) == 0 {
		return DefaultHashValue
	}

	if _, ok := wloi.whiteList[ns]; ok {
		return wloi.PrimaryKeyHasher.DistributeOplogByMod(log, mod)
	}
	return wloi.TableHasher.DistributeOplogByMod(log, mod)
}

/*********************************************/
func getValueFromBsonD(obj bson.D, key string) (interface{}, bool) {
	for _, ele := range obj {
		if ele.Key == key {
			return ele.Value, true
		}
	}

	return nil, false
}
func GetIdOrNSFromOplog(log *PartialLog) interface{} {
	switch log.Operation {
	case "i", "d":
		return GetKey(log.Object, "")
	case "u":
		if id, ok := getValueFromBsonD(log.Query, "_id"); ok {
			return id
		} else {
			return GetKey(log.Object, "")
		}
	case "c":
		// we don't treat vectored insert oplog(o.applyOps:{$exists:true}) as txns and dispatch to fixed worker 0
		if log.MultiOpType != nil && *log.MultiOpType == 1 {
			// use 'ts' field to hash
			return log.Timestamp.T + log.Timestamp.I
		}
		return log.Namespace
	default:
		_ = LOG.Critical("Unrecognized oplog object operation %s", log.Operation)
	}

	return log.Namespace
}

func stringHashValue(s string) uint32 {
	// consult from Java String.hashcode()
	var hashValue uint32
	for _, c := range s {
		hashValue = 31*hashValue + uint32(c)
	}
	if hashValue < 0 {
		return -hashValue
	}

	return hashValue
}

func Hash(hashObject interface{}) uint32 {
	switch object := hashObject.(type) {
	case primitive.ObjectID:
		return stringHashValue(object.Hex())
	case string:
		return stringHashValue(object)
	case int64:
		return uint32(object)
	case int32:
		return uint32(object)
	case int:
		return uint32(object)
	case uint:
		return uint32(object)
	case uint64:
		return uint32(object)
	case uint32:
		return object
	case nil:
		_ = LOG.Warn("Hash object is NIL. use default value %d", DefaultHashValue)
	default:
		_ = LOG.Warn("Hash object is UNKNOWN type[%T], value is [%v]. use default value %d",
			hashObject, hashObject, DefaultHashValue)
	}

	return DefaultHashValue
}
