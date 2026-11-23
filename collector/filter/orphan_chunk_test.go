package filter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestFromHex(t *testing.T) {
	for c := byte('0'); c <= '9'; c++ {
		assert.Equal(t, c-'0', fromHex(c), "digit %c", c)
	}
	for c := byte('a'); c <= 'f'; c++ {
		assert.Equal(t, c-'a'+10, fromHex(c), "lower %c", c)
	}
	for c := byte('A'); c <= 'F'; c++ {
		assert.Equal(t, c-'A'+10, fromHex(c), "upper %c", c)
	}
	for _, c := range []byte{'g', 'G', 'z', ' ', '/', ':', '@', '`', 0x00, 0xff} {
		assert.Equal(t, byte(0xff), fromHex(c), "invalid %q", c)
	}
}

func TestGetBsonTypeCanonicalisation(t *testing.T) {
	for _, tc := range []struct {
		in   interface{}
		want int
		val  interface{}
	}{
		{float32(1.5), BsonTypeNumber, float64(1.5)},
		{float64(2.5), BsonTypeNumber, float64(2.5)},
		{int(3), BsonTypeNumber, float64(3)},
		{int32(4), BsonTypeNumber, float64(4)},
		{int64(5), BsonTypeInt64, int64(5)},
		{int64(math.MinInt64), BsonMinKey, nil},
		{int64(math.MaxInt64), BsonMaxKey, nil},
		{"s", BsonTypeString, "s"},
		{true, BsonTypeBool, int64(1)},
		{false, BsonTypeBool, int64(0)},
		{primitive.DateTime(1234), BsonTypeDate, int64(1234)},
		{primitive.MinKey{}, BsonMinKey, nil},
		{primitive.MaxKey{}, BsonMaxKey, nil},
	} {
		got, val := getBsonType(tc.in)
		assert.Equal(t, tc.want, got, "%T", tc.in)
		assert.Equal(t, tc.val, val, "%T", tc.in)
	}

	oid := primitive.NewObjectID()
	got, val := getBsonType(oid)
	assert.Equal(t, BsonTypeOid, got)
	assert.Equal(t, oid.Hex(), val)

	// T occupies the high 32 bits and must stay unsigned past 2^31
	ts := primitive.Timestamp{T: 0x80000000, I: 7}
	got, val = getBsonType(ts)
	assert.Equal(t, BsonTypeTstamp, got)
	assert.Equal(t, uint64(0x80000000)<<32|uint64(7), val)
}

func TestNumericCmpInt64StaysExact(t *testing.T) {
	// above 2^53 float64 would round these together
	a, b := int64(1<<53+1), int64(1<<53+2)
	assert.Equal(t, -1, numericCmp(BsonTypeInt64, a, BsonTypeInt64, b))
	assert.Equal(t, 1, numericCmp(BsonTypeInt64, b, BsonTypeInt64, a))
	assert.Equal(t, 0, numericCmp(BsonTypeInt64, a, BsonTypeInt64, a))
}

func TestNumericCmpMixedAndFloat(t *testing.T) {
	assert.Equal(t, -1, numericCmp(BsonTypeInt64, int64(1), BsonTypeNumber, float64(2)))
	assert.Equal(t, 1, numericCmp(BsonTypeNumber, float64(2), BsonTypeInt64, int64(1)))
	assert.Equal(t, 0, numericCmp(BsonTypeInt64, int64(2), BsonTypeNumber, float64(2)))
	assert.Equal(t, -1, numericCmp(BsonTypeNumber, float64(1), BsonTypeNumber, float64(2)))
	assert.Equal(t, 0, numericCmp(BsonTypeNumber, float64(2), BsonTypeNumber, float64(2)))
	assert.Equal(t, 1, numericCmp(BsonTypeNumber, float64(3), BsonTypeNumber, float64(2)))
}

func TestChunkComparisonsPerType(t *testing.T) {
	oidA, oidB := primitive.ObjectID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		primitive.ObjectID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	for _, tc := range []struct {
		name string
		lo   interface{}
		hi   interface{}
	}{
		{"number", float64(1), float64(2)},
		{"int64", int64(1), int64(2)},
		{"string", "a", "b"},
		{"oid", oidA, oidB},
		{"bool", false, true},
		{"date", primitive.DateTime(1), primitive.DateTime(2)},
		{"timestamp", primitive.Timestamp{T: 1, I: 0}, primitive.Timestamp{T: 2, I: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, chunkLt(tc.lo, tc.hi))
			assert.False(t, chunkLt(tc.hi, tc.lo))
			assert.True(t, chunkGt(tc.hi, tc.lo))
			assert.False(t, chunkGt(tc.lo, tc.hi))
			assert.True(t, chunkEqual(tc.lo, tc.lo))
			assert.False(t, chunkEqual(tc.lo, tc.hi))
		})
	}
}

func TestChunkMinMaxKeys(t *testing.T) {
	// MinKey/MaxKey are equal to themselves and never ordered against anything
	for _, k := range []interface{}{primitive.MinKey{}, primitive.MaxKey{}, int64(math.MinInt64), int64(math.MaxInt64)} {
		assert.True(t, chunkEqual(k, k))
		assert.False(t, chunkGt(k, k))
		assert.False(t, chunkLt(k, k))
	}
	// int64 sentinels canonicalise to the same tags as the primitive keys
	assert.True(t, chunkEqual(primitive.MinKey{}, int64(math.MinInt64)))
	assert.True(t, chunkEqual(primitive.MaxKey{}, int64(math.MaxInt64)))
}

func TestChunkCrossTypeOrdering(t *testing.T) {
	// different canonical tags order by tag, and are never equal
	assert.True(t, chunkLt(float64(1), "a"))
	assert.True(t, chunkGt("a", float64(1)))
	assert.False(t, chunkEqual(float64(1), "a"))

	assert.True(t, chunkLt("a", true))
	assert.True(t, chunkGt(true, "a"))

	// numeric tags compare by value, not by tag, even when the tags differ
	assert.True(t, chunkLt(int64(1), float64(2)))
	assert.True(t, chunkGt(float64(2), int64(1)))
	assert.True(t, chunkEqual(int64(2), float64(2)))
}

func TestNumericTypeTags(t *testing.T) {
	assert.True(t, numericType(BsonTypeNumber))
	assert.True(t, numericType(BsonTypeInt64))
	for _, tag := range []int{BsonMinKey, BsonMaxKey, BsonTypeString, BsonTypeOid, BsonTypeBool, BsonTypeDate, BsonTypeTstamp} {
		assert.False(t, numericType(tag), "tag %d", tag)
	}
}

func TestChunkTimestampIncrementOrdering(t *testing.T) {
	// same seconds, different increment: the low 32 bits must decide
	lo := primitive.Timestamp{T: 100, I: 1}
	hi := primitive.Timestamp{T: 100, I: 2}
	require.True(t, chunkLt(lo, hi))
	require.True(t, chunkGt(hi, lo))
	require.False(t, chunkEqual(lo, hi))
}
