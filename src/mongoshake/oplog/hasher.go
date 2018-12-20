package oplog

import (
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	ShardByID        = "id"
	ShardByNamespace = "collection"
	ShardAutomatic   = "auto"
)

const (
	DefaultHashValue = 0
)

type Hasher interface {
	DistributeOplogByMod(log *PartialLog, mod int) uint32
}

type PrimaryKeyHasher struct {
	Hasher
}

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

	return stringHashValue(log.Namespace) % uint32(mod)
}

func GetIdOrNSFromOplog(log *PartialLog) interface{} {
	switch log.Operation {
	case "i", "d":
		return log.Object["_id"]
	case "u":
		if id, ok := log.Query["_id"]; ok {
			return id
		} else {
			return log.Object["_id"]
		}
	case "c":
		return log.Namespace
	default:
		LOG.Critical("Unrecognized oplog object operation %s", log.Operation)
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
	case bson.ObjectId:
		return stringHashValue(object.Hex())
	case string:
		return stringHashValue(object)
	case int:
		return uint32(object)
	case nil:
		LOG.Warn("Hash object is NIL. use default value %d", DefaultHashValue)
	default:
		LOG.Warn("Hash object is UNKNOWN type[%T], value is [%v]. use default value %d",
			hashObject, hashObject, DefaultHashValue)
	}

	return DefaultHashValue
}

// we need to ensure that oplog entry will be sent to the same job[$hash]
// if they have the same ObjectID. thus we can consume the oplog entry
// sequentially
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
		LOG.Warn("Couldn't extract hash object. collector has mixed up. use Oplog.Namespace instead %v", log)
		hashObject = log.Namespace
	}

	return Hash(hashObject) % uint32(mod)
}
