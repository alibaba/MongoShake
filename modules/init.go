package module

var (
	compressorGzip    = NewGZipCompressor()
	compressorSnappy  = NewSnappyCompressor()
	compressorZlib    = NewZlibCompressor()
	compressorDeflate = NewDeflateCompressor()
)
