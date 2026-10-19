package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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
