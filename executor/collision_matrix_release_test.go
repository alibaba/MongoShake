//go:build !debug

package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The unrecognised-type arm calls nimo.Assert, which panics under -tags debug
// (build.sh uses that for DEBUG=1) and is a no-op otherwise. Only assert the
// release behaviour, where the type is reported and contributes zero.
func TestCalculateSignatureUnrecognisedType(t *testing.T) {
	assert.Equal(t, float64(0), calculateSignature(struct{ X int }{1}))
}
