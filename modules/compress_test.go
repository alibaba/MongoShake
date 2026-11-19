package module

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/tunnel"
)

func TestGetCompressorByName(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   uint32
	}{
		{utils.VarIncrSyncWorkerOplogCompressorGzip, CompressWithGzip},
		{utils.VarIncrSyncWorkerOplogCompressorSnappy, CompressWithSnappy},
		{utils.VarIncrSyncWorkerOplogCompressorZlib, CompressWithZlib},
		{utils.VarIncrSyncWorkerOplogCompressorDeflate, CompressWithDeflate},
	} {
		c, err := GetCompressorByName(tc.name)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.id, c.Id(), tc.name)
		assert.Equal(t, tc.name, c.Name(), tc.name)
	}

	// "none" deliberately falls through to the error branch: a Compressor with
	// no zipper must never be installed.
	for _, name := range []string{utils.VarIncrSyncWorkerOplogCompressorNone, "", "lz4", "GZIP"} {
		c, err := GetCompressorByName(name)
		assert.Error(t, err, name)
		assert.Nil(t, c, name)
	}
}

func TestGetCompressorById(t *testing.T) {
	for _, tc := range []struct {
		id   uint32
		name string
	}{
		{CompressWithGzip, utils.VarIncrSyncWorkerOplogCompressorGzip},
		{CompressWithSnappy, utils.VarIncrSyncWorkerOplogCompressorSnappy},
		{CompressWithZlib, utils.VarIncrSyncWorkerOplogCompressorZlib},
		{CompressWithDeflate, utils.VarIncrSyncWorkerOplogCompressorDeflate},
	} {
		c, err := GetCompressorById(tc.id)
		require.NoError(t, err)
		assert.Equal(t, tc.name, c.Name())
		assert.Equal(t, tc.id, c.Id())
	}

	for _, id := range []uint32{NoCompress, 5, 99} {
		c, err := GetCompressorById(id)
		assert.Error(t, err, id)
		assert.Nil(t, c, id)
	}
}

func TestCompressorRoundTrip(t *testing.T) {
	payloads := [][]byte{
		[]byte("a single oplog entry"),
		make([]byte, 64*1024), // highly compressible, exercises the level setting
		{0x00, 0xff, 0x10, 0x00, 0x7f},
	}
	for i := range payloads[1] {
		payloads[1][i] = byte(i % 7)
	}

	for _, id := range []uint32{CompressWithGzip, CompressWithSnappy, CompressWithZlib, CompressWithDeflate} {
		c, err := GetCompressorById(id)
		require.NoError(t, err)
		for _, payload := range payloads {
			zipped, err := c.Compress(payload)
			require.NoError(t, err, c.Name())
			unzipped, err := c.Decompress(zipped)
			require.NoError(t, err, c.Name())
			assert.Equal(t, payload, unzipped, c.Name())
			// Snappy is an identity pass-through here; the real codecs must
			// actually shrink the repetitive 64KB payload, otherwise a
			// no-op Compress would still satisfy the round-trip.
			if id != CompressWithSnappy && len(payload) == 64*1024 {
				assert.Less(t, len(zipped), len(payload), c.Name())
			}
		}
	}
}

func TestCompressorDecompressRejectsGarbage(t *testing.T) {
	// Only Deflate is asserted here. GZip and Zlib discard the error from
	// gzip.NewReader / zlib.NewReader and then dereference the nil reader, so
	// malformed input panics instead of returning an error; that is a separate
	// defect and needs a fix before it can be asserted on.
	c, err := GetCompressorById(CompressWithDeflate)
	require.NoError(t, err)
	_, err = c.Decompress([]byte("definitely not a compressed stream"))
	assert.Error(t, err, c.Name())
}

func TestCompressorInstall(t *testing.T) {
	saved := conf.Options.IncrSyncWorkerOplogCompressor
	savedLevel := CompressLevel
	t.Cleanup(func() {
		conf.Options.IncrSyncWorkerOplogCompressor = saved
		CompressLevel = savedLevel
	})

	for _, name := range []string{
		utils.VarIncrSyncWorkerOplogCompressorGzip,
		utils.VarIncrSyncWorkerOplogCompressorSnappy,
		utils.VarIncrSyncWorkerOplogCompressorZlib,
		utils.VarIncrSyncWorkerOplogCompressorDeflate,
	} {
		conf.Options.IncrSyncWorkerOplogCompressor = name
		// Install must force the high-ratio level, not leave whatever was set.
		CompressLevel = BestSpeed
		c := &Compressor{}
		assert.True(t, c.IsRegistered(), name)
		require.True(t, c.Install(), name)
		require.NotNil(t, c.zipper, name)
		assert.Equal(t, name, c.zipper.Name())
		assert.Equal(t, BestCompression, CompressLevel)
	}

	for _, name := range []string{utils.VarIncrSyncWorkerOplogCompressorNone, "bogus"} {
		conf.Options.IncrSyncWorkerOplogCompressor = name
		c := &Compressor{}
		assert.False(t, c.Install(), name)
		assert.Nil(t, c.zipper, name)
	}

	conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorNone
	assert.False(t, (&Compressor{}).IsRegistered())
}

func TestCompressorHandleWithoutLogs(t *testing.T) {
	saved := conf.Options.IncrSyncWorkerOplogCompressor
	t.Cleanup(func() { conf.Options.IncrSyncWorkerOplogCompressor = saved })
	conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorGzip

	c := &Compressor{}
	require.True(t, c.Install())

	msg := &tunnel.WMessage{TMessage: &tunnel.TMessage{}}
	assert.Equal(t, tunnel.ReplyOK, c.Handle(msg))
	assert.Equal(t, NoCompress, msg.Compress)
	assert.Empty(t, msg.RawLogs)
}

func TestCompressorHandleCompressesEveryLog(t *testing.T) {
	saved := conf.Options.IncrSyncWorkerOplogCompressor
	t.Cleanup(func() { conf.Options.IncrSyncWorkerOplogCompressor = saved })
	conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorGzip

	c := &Compressor{}
	require.True(t, c.Install())

	original := [][]byte{[]byte("first entry"), []byte("second entry"), []byte("third entry")}
	msg := &tunnel.WMessage{TMessage: &tunnel.TMessage{RawLogs: original}}
	require.Equal(t, tunnel.ReplyOK, c.Handle(msg))
	assert.Equal(t, CompressWithGzip, msg.Compress)
	require.Len(t, msg.RawLogs, len(original))
	for i, zipped := range msg.RawLogs {
		back, err := c.zipper.Decompress(zipped)
		require.NoError(t, err)
		assert.Equal(t, original[i], back)
	}
}

func TestCompressorHandleRejectsIncompleteCompression(t *testing.T) {
	saved := conf.Options.IncrSyncWorkerOplogCompressor
	t.Cleanup(func() { conf.Options.IncrSyncWorkerOplogCompressor = saved })
	// Snappy passes bytes through unchanged, so an empty entry yields a zero
	// compressed size and Handle must refuse the whole batch rather than send a
	// message whose logs cannot be reconstructed.
	conf.Options.IncrSyncWorkerOplogCompressor = utils.VarIncrSyncWorkerOplogCompressorSnappy

	c := &Compressor{}
	require.True(t, c.Install())
	msg := &tunnel.WMessage{TMessage: &tunnel.TMessage{RawLogs: [][]byte{{}}}}
	assert.Equal(t, tunnel.ReplyServerFault, c.Handle(msg))
}
