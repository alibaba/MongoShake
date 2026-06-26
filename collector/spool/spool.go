package spool

// OplogSpool is a local temporary FIFO store used during full sync.
type OplogSpool interface {
	Put(data []byte) error
	ReadBatch(max int) ([][]byte, error)
	Advance(n int) error
	ReadAll() ([][]byte, error)
	LastWriteData() ([]byte, error)
	Depth() (uint64, error)
	Stats() Stats
	Close() error
	Delete() error
}

type Stats struct {
	Name     string
	Path     string
	ReadSeq  uint64
	WriteSeq uint64
	Depth    uint64
	Bytes    uint64
}

type OpenOptions struct {
	Name            string
	LogDir          string
	CreateIfMissing bool
	MetricName      string
	MetricStage     string
	MaxBytesMB      int64
}
