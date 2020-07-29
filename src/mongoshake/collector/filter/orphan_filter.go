package filter

import (
	"crypto/md5"
	"encoding/binary"

	"mongoshake/oplog"
	"mongoshake/sharding"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	// refer to mongo/bson/bsontypes.h of mongodb kernel 4.0
	BsonInvalid    = -1
	BsonMinKey     = 0
	BsonTypeNumber = 10
	BsonTypeString = 15
	BsonTypeOid    = 35
	BsonMaxKey     = 100
)

type OrphanFilter struct {
	replset  string
	chunkMap sharding.DBChunkMap
}

func NewOrphanFilter(replset string, chunkMap sharding.DBChunkMap) *OrphanFilter {
	return &OrphanFilter{
		replset:  replset,
		chunkMap: chunkMap,
	}
}

func (filter *OrphanFilter) Filter(docD bson.D, namespace string) bool {
	if filter.chunkMap == nil {
		LOG.Warn("chunk map is nil")
		return false
	}

	shardCol, hasChunk := filter.chunkMap[namespace]
	if !hasChunk {
		return false
	}

NextChunk:
	for _, chunkRage := range shardCol.Chunks {
		// check greater and equal than the minimum of the chunk range
		for keyInd, keyName := range shardCol.Keys {
			key := oplog.GetKey(docD, keyName)
			if key == nil {
				LOG.Crashf("OrphanFilter find no shard key[%v] in doc %v", keyName, docD)
			}
			if shardCol.ShardType == sharding.HashedShard {
				key = ComputeHash(key)
			}
			if chunkLt(key, chunkRage.Mins[keyInd]) {
				continue NextChunk
			}
			if chunkGt(key, chunkRage.Mins[keyInd]) {
				break
			}
		}
		// check less than the maximum of the chunk range
		for keyInd, keyName := range shardCol.Keys {
			key := oplog.GetKey(docD, keyName)
			if key == nil {
				LOG.Crashf("OrphanFilter find no shard ke[%v] in doc %v", keyName, docD)
			}
			if shardCol.ShardType == sharding.HashedShard {
				key = ComputeHash(key)
			}
			if chunkGt(key, chunkRage.Maxs[keyInd]) {
				continue NextChunk
			}
			if chunkLt(key, chunkRage.Maxs[keyInd]) {
				break
			}
			if keyInd == len(shardCol.Keys)-1 {
				continue NextChunk
			}
		}
		// current key in the chunk, therefore dont filter
		return false
	}
	LOG.Warn("document syncer %v filter orphan document %v with shard key %v in ns[%v]",
		filter.replset, docD, shardCol.Keys, namespace)
	return true
}

func ComputeHash(data interface{}) int64 {
	// refer to mongo/db/hasher.cpp of mongodb kernel 4.0
	w := md5.New()
	var buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(0))
	w.Write(buf)

	switch rd := data.(type) {
	case string:
		binary.LittleEndian.PutUint32(buf, uint32(BsonTypeString))
		w.Write(buf)
		binary.LittleEndian.PutUint32(buf, uint32(len(rd)+1))
		w.Write(buf)
		s := []byte(rd)
		s = append(s, 0)
		w.Write(s)
	case int, int64, float64:
		var rdu uint64
		if rd1, ok := rd.(int); ok {
			rdu = uint64(rd1)
		} else if rd2, ok := rd.(int64); ok {
			rdu = uint64(rd2)
		} else if rd3, ok := rd.(float64); ok {
			rdu = uint64(rd3)
		}
		binary.LittleEndian.PutUint32(buf, uint32(BsonTypeNumber))
		w.Write(buf)
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, rdu)
		w.Write(buf)
	case bson.ObjectId:
		binary.LittleEndian.PutUint32(buf, uint32(BsonTypeOid))
		w.Write(buf)
		if len(rd) == 24 {
			buf = make([]byte, 12)
			bytes := []byte(rd)
			for i, j := 0, 0; i < len(buf); i++ {
				buf[i] = (fromHex(bytes[j]) << 4) | fromHex(bytes[j+1])
				j += 2
			}
		} else {
			buf = []byte(rd)
		}
		w.Write(buf)
	default:
		LOG.Crashf("ComputeHash unsupported bson type %T %#v\n", data, data)
	}
	out := w.Sum(nil)
	result := int64(binary.LittleEndian.Uint64(out))
	return result
}

func fromHex(c byte) byte {
	if '0' <= c && c <= '9' {
		return c - '0'
	}
	if 'a' <= c && c <= 'f' {
		return c - 'a' + 10
	}
	if 'A' <= c && c <= 'F' {
		return c - 'A' + 10
	}
	return 0xff
}

func chunkGt(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if xType != yType {
		return xType > yType
	}

	switch xType {
	case BsonMinKey:
		return false
	case BsonMaxKey:
		return false
	case BsonTypeNumber:
		return rx.(float64) > ry.(float64)
	case BsonTypeString:
		return rx.(string) > ry.(string)
	default:
		LOG.Crashf("chunkGt meet unknown type %v", xType)
	}
	return true
}

func chunkEqual(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if xType != yType {
		return false
	}

	switch xType {
	case BsonMinKey:
		return true
	case BsonMaxKey:
		return true
	case BsonTypeNumber:
		return rx.(float64) == ry.(float64)
	case BsonTypeString:
		return rx.(string) == ry.(string)
	default:
		LOG.Crashf("chunkEqual meet unknown type %v", xType)
	}
	return true
}

func chunkLt(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if xType != yType {
		return xType < yType
	}

	switch xType {
	case BsonMinKey:
		return false
	case BsonMaxKey:
		return false
	case BsonTypeNumber:
		return rx.(float64) < ry.(float64)
	case BsonTypeString:
		return rx.(string) < ry.(string)
	default:
		LOG.Crashf("chunkLt meet unknown type %v", xType)
	}
	return true
}

func getBsonType(x interface{}) (int, interface{}) {
	if x == bson.MinKey {
		return BsonMinKey, nil
	}
	if x == bson.MaxKey {
		return BsonMaxKey, nil
	}
	switch rx := x.(type) {
	case float32:
		return BsonTypeNumber, float64(rx)
	case float64:
		return BsonTypeNumber, rx
	case int:
		return BsonTypeNumber, float64(rx)
	case int32:
		return BsonTypeNumber, float64(rx)
	case int64:
		return BsonTypeNumber, float64(rx)
	case string:
		return BsonTypeString, rx
	case bson.ObjectId:
		return BsonTypeOid, string(rx)
	default:
		LOG.Crashf("chunkLt meet unknown type %T", x)
	}
	return BsonInvalid, nil
}
