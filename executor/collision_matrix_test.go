package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestCalculateSignatureTypes(t *testing.T) {
	// nil uses a prime so it cannot collide with the boolean encoding
	assert.Equal(t, float64(3), calculateSignature(nil))
	assert.Equal(t, float64(1), calculateSignature(false))
	assert.Equal(t, float64(2), calculateSignature(true))

	assert.Equal(t, float64(7), calculateSignature(float64(7)))
	// non-float64 numerics are not converted, so they contribute zero
	assert.Equal(t, float64(0), calculateSignature(int32(7)))

	// Java String.hashCode style accumulation
	assert.Equal(t, float64('a'), calculateSignature("a"))
	assert.Equal(t, 31.0*float64('a')+float64('b'), calculateSignature("ab"))
	assert.Equal(t, calculateSignature("ab"), calculateSignature([]byte("ab")))
	assert.Equal(t, calculateSignature("ab"), calculateSignature(primitive.Binary{Data: []byte("ab")}))

	assert.Equal(t, float64(int64(100)<<32+int64(2)), calculateSignature(primitive.Timestamp{T: 100, I: 2}))

	// containers recurse over keys and values
	assert.Equal(t, calculateSignature("k")+calculateSignature(float64(1)),
		calculateSignature(bson.M{"k": float64(1)}))
	assert.Equal(t, calculateSignature(float64(1))+calculateSignature(float64(2)),
		calculateSignature([]interface{}{float64(1), float64(2)}))
}

func TestAddSignatureAndIdentifier(t *testing.T) {
	id := &OplogUniqueIdentifier{}
	id.addSignature("a")
	id.addSignature("b")
	assert.Equal(t, []string{"a", "b"}, id.signatureTable)

	log := &PartialLogWithCallback{partialLog: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation:     "i",
			UniqueIndexes: bson.M{"x": float64(1), "y": float64(2)},
		},
	}}
	got := newUniqueIdentifier(5, log)
	assert.Equal(t, 5, got.order)
	assert.Len(t, got.signatureTable, 2)
	assert.Equal(t, map[string]interface{}{"x": float64(1), "y": float64(2)}, got.operations)

	// fillupOperationValues rebuilds UniqueIndexesUpdates from the oplog object
	// rather than carrying over what the caller supplied; with no object the
	// column resolves to nil, and that nil still contributes a signature.
	upd := &PartialLogWithCallback{partialLog: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation:     "u",
			UniqueIndexes: bson.M{"x": float64(1)},
		},
		UniqueIndexesUpdates: bson.M{"x": float64(9)},
	}}
	gotUpd := newUniqueIdentifier(1, upd)
	assert.Len(t, gotUpd.signatureTable, 2)
	assert.Equal(t, bson.M{"x": []interface{}{nil}}, upd.partialLog.UniqueIndexesUpdates)

	// the same column with different values must produce different signatures
	other := &PartialLogWithCallback{partialLog: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation:     "i",
			UniqueIndexes: bson.M{"x": float64(2), "y": float64(2)},
		},
	}}
	assert.NotEqual(t, got.signatureTable, newUniqueIdentifier(5, other).signatureTable)
}

func TestIntersection(t *testing.T) {
	assert.False(t, intersection(bson.M{}, bson.M{"x": float64(1)}))
	assert.False(t, intersection(bson.M{"x": float64(1)}, bson.M{}))
	assert.False(t, intersection(bson.M{"x": float64(1)}, bson.M{"y": float64(1)}))
	assert.True(t, intersection(bson.M{"x": float64(1)}, bson.M{"x": float64(1)}))
	assert.False(t, intersection(bson.M{"x": float64(1)}, bson.M{"x": float64(2)}))
	// order of the two maps must not matter
	assert.True(t, intersection(bson.M{"a": float64(1), "x": float64(2)}, bson.M{"x": float64(2)}))
	assert.True(t, intersection(bson.M{"x": float64(2)}, bson.M{"a": float64(1), "x": float64(2)}))

	assert.True(t, intersectionInOrder(bson.M{"x": nil}, bson.M{"x": nil}))
	assert.False(t, intersectionInOrder(bson.M{"x": nil}, bson.M{"y": nil}))
}

func TestHaveMutualIndex(t *testing.T) {
	a := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{UniqueIndexes: bson.M{"x": float64(1)}}}
	b := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{UniqueIndexes: bson.M{"x": float64(1)}}}
	c := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{UniqueIndexes: bson.M{"x": float64(2)}}}

	assert.False(t, haveMutualIndex(a, a), "the same log never conflicts with itself")
	assert.True(t, haveMutualIndex(a, b))
	assert.False(t, haveMutualIndex(a, c))

	// update-side indexes participate in all four combinations
	u := &oplog.PartialLog{UniqueIndexesUpdates: bson.M{"x": float64(1)}}
	assert.True(t, haveMutualIndex(a, u))
	assert.True(t, haveMutualIndex(u, a))
	assert.True(t, haveMutualIndex(u, anotherUpdateLog()))
}

func anotherUpdateLog() *oplog.PartialLog {
	return &oplog.PartialLog{UniqueIndexesUpdates: bson.M{"x": float64(1)}}
}

func TestNoopMatrix(t *testing.T) {
	logs := []*PartialLogWithCallback{
		{partialLog: &oplog.PartialLog{}},
		{partialLog: &oplog.PartialLog{}},
	}
	m := &NoopMatrix{}
	segs := m.split(logs)
	require.Len(t, segs, 1)
	assert.Equal(t, logs, segs[0])

	records := m.convert(segs[0])
	require.Len(t, records, 2)
	for i, r := range records {
		assert.Equal(t, logs[i], r.original)
		assert.Nil(t, r.wait)
	}
}

func TestBarrierMatrixSplit(t *testing.T) {
	m := NewBarrierMatrix()
	require.NotNil(t, m)

	// no unique indexes at all: everything stays in one segment
	plain := []*PartialLogWithCallback{
		{partialLog: &oplog.PartialLog{}},
		{partialLog: &oplog.PartialLog{}},
	}
	segs := m.split(plain)
	require.Len(t, segs, 1)
	assert.Len(t, segs[0], 2)

	// two logs touching the same index value must be split apart
	conflict := []*PartialLogWithCallback{
		{partialLog: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Operation: "i", UniqueIndexes: bson.M{"x": float64(1)}}}},
		{partialLog: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Operation: "i", UniqueIndexes: bson.M{"x": float64(1)}}}},
	}
	segs = m.split(conflict)
	require.Len(t, segs, 2)
	assert.Len(t, segs[0], 1)
	assert.Len(t, segs[1], 1)

	// different values share a signature space but do not conflict
	disjoint := []*PartialLogWithCallback{
		{partialLog: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Operation: "i", UniqueIndexes: bson.M{"x": float64(1)}}}},
		{partialLog: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Operation: "i", UniqueIndexes: bson.M{"x": float64(2)}}}},
	}
	segs = m.split(disjoint)
	require.Len(t, segs, 1)
	assert.Len(t, segs[0], 2)

	// empty input yields no segments
	assert.Empty(t, m.split(nil))

	records := m.convert(conflict[:1])
	require.Len(t, records, 1)
	assert.Equal(t, conflict[0], records[0].original)
}

// TestCalculateSignatureUsesTimestampSeconds covers the BSON Timestamp branch
// of calculateSignature.
//
// That branch read `int64(o.T<<32)`. primitive.Timestamp.T is a uint32, and Go
// defines a shift count at or above the operand width as zero rather than
// leaving it undefined, so o.T<<32 was always 0 and only the I component ever
// reached the signature. Two timestamps a whole second apart but sharing an I
// hashed identically, which the collision matrix reads as "same unique index
// value" and serialises.
//
// Signatures never leave the process: OplogUniqueIdentifier carries no
// serialisation tags, is never marshalled, and its signatureTable only keys an
// in-memory map inside BarrierMatrix.split. Changing the computation therefore
// cannot invalidate persisted state or alter how documents are routed.
//
// The values asserted below stay under 2^53 deliberately. float64 represents
// integers exactly only below that, and a wall-clock T of ~1.7e9 shifted left
// by 32 is ~7.5e18 -- above it -- so for realistic timestamps the conversion
// rounds and I stops contributing. T still dominates the signature, which is
// strictly better than discarding T entirely, but exactness here is a property
// of small T rather than of the fix. Preserving both components would mean
// moving the signature off float64, which is a larger change than this bug
// warrants given the scheme already formats each signature with %f.
func TestCalculateSignatureUsesTimestampSeconds(t *testing.T) {
	// 5<<32 + 7 = 21474836487, 6<<32 + 7 = 25769803783. Before the fix both
	// inputs returned 7.
	require.Equal(t, 21474836487.0, calculateSignature(primitive.Timestamp{T: 5, I: 7}),
		"the seconds component must reach the signature")

	require.NotEqual(t,
		calculateSignature(primitive.Timestamp{T: 5, I: 7}),
		calculateSignature(primitive.Timestamp{T: 6, I: 7}),
		"timestamps one second apart with the same increment must not share a signature")

	require.NotEqual(t,
		calculateSignature(primitive.Timestamp{T: 5, I: 7}),
		calculateSignature(primitive.Timestamp{T: 5, I: 8}),
		"the increment must still contribute while the combined value is exactly representable")
}
