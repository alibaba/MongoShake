package module

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A malformed, truncated or wrong-codec block must be rejected with an error.
// GZip and Zlib used to discard the reader constructor's error and then call
// ReadAll on the nil reader, which panicked instead.
func TestDecompressMalformedInputReturnsError(t *testing.T) {
	for _, id := range []uint32{CompressWithGzip, CompressWithZlib, CompressWithDeflate} {
		c, err := GetCompressorById(id)
		require.NoError(t, err)
		for _, bad := range [][]byte{
			[]byte("definitely not a compressed stream"),
			{},
			{0x1f, 0x8b}, // truncated gzip magic
		} {
			assert.NotPanics(t, func() {
				_, err := c.Decompress(bad)
				assert.Error(t, err, "%s input %v", c.Name(), bad)
			}, "%s input %v", c.Name(), bad)
		}
	}
}
