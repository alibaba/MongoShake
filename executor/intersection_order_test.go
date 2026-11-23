package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

// intersectionInOrder used to return on the first key present in both maps,
// whether or not the values matched. With a differing column iterated first, a
// genuinely shared unique-index value was never examined, and Go's randomised
// map order made the answer non-deterministic.
func TestIntersectionFindsConflictBeyondFirstSharedKey(t *testing.T) {
	// "a" differs, "x" matches: these oplogs conflict on x and must serialise.
	one := bson.M{"a": float64(1), "x": float64(2)}
	other := bson.M{"a": float64(9), "x": float64(2)}
	for i := 0; i < 200; i++ {
		assert.True(t, intersection(one, other), "iteration %d: shared x must be found", i)
		assert.True(t, intersection(other, one), "iteration %d: argument order must not matter", i)
	}

	// no shared column value at all stays false
	assert.False(t, intersection(bson.M{"a": float64(1), "x": float64(2)},
		bson.M{"a": float64(9), "x": float64(3)}))

	// a single shared key still works in both directions
	assert.True(t, intersection(bson.M{"x": float64(2)}, bson.M{"a": float64(9), "x": float64(2)}))
	assert.True(t, intersection(bson.M{"a": float64(9), "x": float64(2)}, bson.M{"x": float64(2)}))

	// three columns, only the last one shared and equal
	assert.True(t, intersection(bson.M{"a": float64(1), "b": float64(2), "z": float64(7)},
		bson.M{"a": float64(8), "b": float64(9), "z": float64(7)}))
}
