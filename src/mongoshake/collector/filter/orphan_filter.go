package filter

import (
	"crypto/md5"
	"encoding/binary"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	utils "mongoshake/common"
	"mongoshake/oplog"
)

const (
	BsonInvalid    = 0
	BsonTypeNumber = 10
	BsonTypeString = 15
	BsonTypeOid    = 35
)

var ChunkRangeTypes = [][]string{
	{"float64", "int64", "int"},
	{"string"}, {"bson.ObjectId"}, {"bool"}}

type OrphanFilter struct {
	replset  string
	chunkMap utils.DBChunkMap
}

func NewOrphanFilter(replset string, chunkMap utils.DBChunkMap) *OrphanFilter {
	return &OrphanFilter{
		replset:  replset,
		chunkMap: chunkMap,
	}
}

func (filter *OrphanFilter) Filter(doc *bson.Raw, namespace string) bool {
	shardCol, hasChunk := filter.chunkMap[namespace]
	if !hasChunk {
		return false
	}
	var docD bson.D
	if err := bson.Unmarshal(doc.Data, &docD); err != nil {
		LOG.Warn("OrphanFilter unmarshal bson %v from ns[%v] failed. %v", doc.Data, namespace, err)
		return false
	}
	key := oplog.GetKey(docD, shardCol.Key)
	if key == nil {
		LOG.Warn("OrphanFilter find no key[%v] in doc %v", shardCol.Key, docD)
		return false
	}
	if shardCol.ShardType == utils.HashedShard {
		var err error
		if key, err = ComputeHash(key); err != nil {
			return false
		}
	}

	for _, chunkRage := range shardCol.Chunks {
		if chunkRage.Min != bson.MinKey {
			if !chunkGte(key, chunkRage.Min) {
				continue
			}
		}
		if chunkRage.Max != bson.MaxKey {
			if !chunkLt(key, chunkRage.Max) {
				continue
			}
		}
		return false
	}
	LOG.Warn("document syncer %v filter orphan document %v with shard key {%v: %v} in ns[%v]",
		filter.replset, docD, shardCol.Key, key, namespace)
	return true
}

func ComputeHash(data interface{}) (int64, error) {
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
		return 0, LOG.Error("ComputeHash unsupported bson type %T %#v\n", data, data)
	}
	out := w.Sum(nil)
	result := int64(binary.LittleEndian.Uint64(out))
	return result, nil
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

func chunkGte(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if xType != yType {
		return xType > yType
	}

	switch xType {
	case BsonTypeNumber:
		return rx.(float64) >= ry.(float64)
	case BsonTypeString:
		return rx.(string) >= ry.(string)
	default:
		LOG.Crashf("chunkLt meet unknown type %v", xType)
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
