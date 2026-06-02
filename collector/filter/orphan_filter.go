package filter

import (
	"crypto/md5"
	"encoding/binary"
	"math"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/sharding"
)

const (
	// canonical type ordering kept in sync with mongo/bson/bsontypes.h
	// (verified identical across kernel 4.0 and 8.0). BsonTypeInt64 is a
	// MongoShake-local refinement: BSON has a single "number" canonical
	// type, but we split int64 out so OrphanFilter can compare hashed
	// shard bounds (which are full-range int64) without losing precision
	// through float64. Mixed int64/float64 comparison promotes to float64.
	BsonInvalid     = -1
	BsonMinKey      = 0
	BsonTypeNumber  = 10
	BsonTypeInt64   = 11
	BsonTypeString  = 15
	BsonTypeOid     = 35
	BsonTypeBool    = 40
	BsonTypeDate    = 45
	BsonTypeTstamp  = 47
	BsonTypeNull    = 5
	BsonMaxKey      = 100
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
		l.Logger.Warnf("chunk map is nil")
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
				l.Logger.Panicf("OrphanFilter find no shard key[%v] in doc %v", keyName, docD)
			}
			// Compound shard keys can mix hashed and ranged columns
			// (e.g. {a: 1, b: "hashed"}); hash only the columns the
			// server hashes, otherwise the ranged column would be
			// compared against a hashed bound and silently mis-classify.
			if shardCol.ShardTypes[keyInd] == sharding.HashedShard {
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
				l.Logger.Panicf("OrphanFilter find no shard key[%v] in doc %v", keyName, docD)
			}
			if shardCol.ShardTypes[keyInd] == sharding.HashedShard {
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
	l.Logger.Warnf("document syncer %v filter orphan document %v with shard key %v in ns[%v]",
		filter.replset, docD, shardCol.Keys, namespace)
	return true
}

// ComputeHash reproduces MongoDB's hashed-shard hash for a BSON shard key
// value. Algorithm: MD5(seed_LE32 || canonicalType_LE32 || valueBytes),
// take the low 8 bytes as little-endian int64. Verified identical between
// kernel 4.0 and 8.0 (see hasher.cpp).
//
// All numeric BSON types (Double, Int, Long, Decimal128) are normalised to
// int64 server-side via safeNumberLongForHash before hashing; we mirror
// that for int / int32 / int64 / float64. Decimal128 is not supported here
// because faithfully reproducing safeNumberLongForHash for non-integer
// decimals (NaN / Inf / overflow) is nontrivial and Decimal128 hashed
// shard keys are extremely rare in practice — a clear panic is preferable
// to a silent mismatch with the kernel.
func ComputeHash(data interface{}) int64 {
	w := md5.New()
	var buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(0)) // seed = 0
	w.Write(buf)

	writeType := func(t int) {
		binary.LittleEndian.PutUint32(buf, uint32(t))
		w.Write(buf)
	}
	writeI64 := func(v int64) {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		w.Write(b)
	}

	switch rd := data.(type) {
	case string:
		// BSON string value layout: int32(len+1) | bytes | 0x00
		writeType(BsonTypeString)
		binary.LittleEndian.PutUint32(buf, uint32(len(rd)+1))
		w.Write(buf)
		w.Write(append([]byte(rd), 0))
	case int:
		writeType(BsonTypeNumber)
		writeI64(int64(rd))
	case int32:
		writeType(BsonTypeNumber)
		writeI64(int64(rd))
	case int64:
		writeType(BsonTypeNumber)
		writeI64(rd)
	case float64:
		// kernel: safeNumberLongForHash truncates toward zero, NaN/Inf
		// map to int64 sentinel values. Plain Go cast handles finite
		// in-range values identically; outside that range users would
		// hit different bytes from the server, but float64 hashed shard
		// keys are vanishingly rare.
		writeType(BsonTypeNumber)
		writeI64(int64(rd))
	case primitive.ObjectID:
		writeType(BsonTypeOid)
		w.Write(rd[:])
	case bool:
		// BSON bool value is a single byte.
		writeType(BsonTypeBool)
		if rd {
			w.Write([]byte{0x01})
		} else {
			w.Write([]byte{0x00})
		}
	case primitive.DateTime:
		// BSON Date value is int64 milliseconds since epoch, LE.
		writeType(BsonTypeDate)
		writeI64(int64(rd))
	case primitive.Timestamp:
		// BSON Timestamp value layout (per BSON spec / bsonelement.h):
		// 8 bytes total, low 4 = increment uint32 LE, high 4 = time uint32 LE.
		// primitive.Timestamp{T: seconds, I: increment} — note the field
		// names: T is "time" (seconds), I is "increment". Increment goes
		// FIRST in the byte layout; getting this order wrong is a common
		// source of hash mismatch with the server, so verified against
		// mongo/bson/bsonelement_inline.h Timestamp() accessor.
		writeType(BsonTypeTstamp)
		b := make([]byte, 8)
		binary.LittleEndian.PutUint32(b[0:4], rd.I)
		binary.LittleEndian.PutUint32(b[4:8], rd.T)
		w.Write(b)
	case nil:
		// BSON null carries no value bytes; only the canonical type contributes.
		// Note: OrphanFilter.Filter cannot currently reach this branch because
		// the upstream oplog.GetKey returns nil for both "field missing" and
		// "field is null" and Filter Panicf-s on nil. This case is kept as
		// future-proofing: if GetKey is ever fixed to distinguish the two,
		// ComputeHash already handles null without further changes.
		writeType(BsonTypeNull)
	default:
		l.Logger.Panicf("ComputeHash unsupported bson type %T %#v "+
			"(Decimal128 / BinData / Symbol are intentionally not implemented; "+
			"open an issue if you actually hit one)", data, data)
	}
	out := w.Sum(nil)
	return int64(binary.LittleEndian.Uint64(out))
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

// numericCmp returns -1/0/1 for x<y / x==y / x>y when both sides are
// numeric. int64 vs int64 stays exact; everything else (or mixed types)
// promotes to float64 (acceptable: BSON range-shard chunk bounds are
// virtually never mixed-type, and the chunk math beats a panic).
func numericCmp(xType int, rx interface{}, yType int, ry interface{}) int {
	if xType == BsonTypeInt64 && yType == BsonTypeInt64 {
		xv, yv := rx.(int64), ry.(int64)
		switch {
		case xv < yv:
			return -1
		case xv > yv:
			return 1
		}
		return 0
	}
	// Mixed int64/float64 (or float64/float64). Promote to float64; this
	// can lose precision above 2^53 if an int64 sneaks in. In a typical
	// BSON range shard chunk this never happens (bounds and doc values
	// share a type), but warn once so production has a breadcrumb if it
	// ever does.
	if xType != yType {
		l.Logger.Warnf("OrphanFilter numericCmp: mixed numeric types (xType=%d yType=%d) "+
			"promoted to float64; may lose precision for int64 above 2^53",
			xType, yType)
	}
	var xv, yv float64
	if xType == BsonTypeInt64 {
		xv = float64(rx.(int64))
	} else {
		xv = rx.(float64)
	}
	if yType == BsonTypeInt64 {
		yv = float64(ry.(int64))
	} else {
		yv = ry.(float64)
	}
	switch {
	case xv < yv:
		return -1
	case xv > yv:
		return 1
	}
	return 0
}

// numericType reports whether t is one of the two MongoShake numeric
// canonical-type tags (BsonTypeNumber covers float/int/int32; BsonTypeInt64
// is the int64-precise refinement).
func numericType(t int) bool {
	return t == BsonTypeNumber || t == BsonTypeInt64
}

func chunkGt(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if numericType(xType) && numericType(yType) {
		return numericCmp(xType, rx, yType, ry) > 0
	}
	if xType != yType {
		return xType > yType
	}

	switch xType {
	case BsonMinKey, BsonMaxKey:
		return false
	case BsonTypeString:
		return rx.(string) > ry.(string)
	default:
		l.Logger.Panicf("chunkGt meet unknown type %v", xType)
	}
	return true
}

func chunkEqual(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if numericType(xType) && numericType(yType) {
		return numericCmp(xType, rx, yType, ry) == 0
	}
	if xType != yType {
		return false
	}

	switch xType {
	case BsonMinKey, BsonMaxKey:
		return true
	case BsonTypeString:
		return rx.(string) == ry.(string)
	default:
		l.Logger.Panicf("chunkEqual meet unknown type %v", xType)
	}
	return true
}

func chunkLt(x, y interface{}) bool {
	xType, rx := getBsonType(x)
	yType, ry := getBsonType(y)

	if numericType(xType) && numericType(yType) {
		return numericCmp(xType, rx, yType, ry) < 0
	}
	if xType != yType {
		return xType < yType
	}

	switch xType {
	case BsonMinKey, BsonMaxKey:
		return false
	case BsonTypeString:
		return rx.(string) < ry.(string)
	default:
		l.Logger.Panicf("chunkLt meet unknown type %v", xType)
	}
	return true
}

func getBsonType(x interface{}) (int, interface{}) {
	switch rx := x.(type) {
	case primitive.MinKey:
		return BsonMinKey, nil
	case primitive.MaxKey:
		return BsonMaxKey, nil
	case float32:
		return BsonTypeNumber, float64(rx)
	case float64:
		return BsonTypeNumber, rx
	case int:
		return BsonTypeNumber, float64(rx)
	case int32:
		return BsonTypeNumber, float64(rx)
	case int64:
		if rx == math.MinInt64 {
			return BsonMinKey, nil
		}
		if rx == math.MaxInt64 {
			return BsonMaxKey, nil
		}
		// keep precision; chunkLt/Gt/Equal handle int64 specifically so
		// hashed-shard bounds (full-range int64) don't collapse via float64
		return BsonTypeInt64, rx
	case string:
		return BsonTypeString, rx
	case primitive.ObjectID:
		return BsonTypeOid, rx.Hex()
	default:
		l.Logger.Panicf("getBsonType meet unknown type %T", x)
	}
	return BsonInvalid, nil
}
