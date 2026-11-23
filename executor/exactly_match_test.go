package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// A BSON null is a legitimate unique-index value. Two nulls must compare equal,
// as the "both of them are null is accept" branch intends; it used to be
// unreachable because reflect.TypeOf(nil).Kind() panicked first.
func TestExactlyMatchNullHandling(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.True(t, ExactlyMatch(nil, nil), "two nulls are equal")
	})
	assert.False(t, ExactlyMatch(nil, "x"))
	assert.False(t, ExactlyMatch("x", nil))
	assert.False(t, ExactlyMatch(nil, int32(1)))
	assert.False(t, ExactlyMatch(int32(1), nil))
}

func TestExactlyMatchKindsAndValues(t *testing.T) {
	assert.True(t, ExactlyMatch("abc", "abc"))
	assert.False(t, ExactlyMatch("abc", "abd"))
	assert.False(t, ExactlyMatch("abc", int32(1)), "different kinds never match")

	assert.True(t, ExactlyMatch(true, true))
	assert.False(t, ExactlyMatch(true, false))

	assert.True(t, ExactlyMatch([]byte{1, 2}, []byte{1, 2}))
	assert.False(t, ExactlyMatch([]byte{1, 2}, []byte{1, 3}))

	assert.True(t, ExactlyMatch(primitive.Binary{Data: []byte{9}}, primitive.Binary{Data: []byte{9}}))
	assert.False(t, ExactlyMatch(primitive.Binary{Data: []byte{9}}, primitive.Binary{Data: []byte{8}}))

	ts := primitive.Timestamp{T: 100, I: 2}
	assert.True(t, ExactlyMatch(ts, primitive.Timestamp{T: 100, I: 2}))
	assert.False(t, ExactlyMatch(ts, primitive.Timestamp{T: 100, I: 3}))

	assert.True(t, ExactlyMatch([]interface{}{float64(1), float64(2)}, []interface{}{float64(1), float64(2)}))
	assert.False(t, ExactlyMatch([]interface{}{float64(1), float64(2)}, []interface{}{float64(1)}))
	assert.False(t, ExactlyMatch([]interface{}{float64(1), float64(2)}, []interface{}{float64(2), float64(1)}))

	// Only float64 is compared by value; every other numeric kind falls through
	// to the conservative "treat as equal" default at the end of the function.
	// Pin that so a future change to the numeric arm is a deliberate decision.
	assert.True(t, ExactlyMatch(int32(1), int32(2)))
	assert.True(t, ExactlyMatch(float64(1), float64(1)))
	assert.False(t, ExactlyMatch(float64(1), float64(2)))
}
