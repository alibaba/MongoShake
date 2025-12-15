package journal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

const (
	SampleFrequency = 1000
)

const (
	NothingOnDefault = iota
	Sampling
	All
)

const (
	BufferCapacity = 4 * 1024 * 1024
)

var FilePattern = utils.GlobalDiagnosticPath + string(filepath.Separator) + "%s.journal"

type Journal struct {
	writer *bufio.Writer
	hasher oplog.Hasher
}

func FileName(identifier string) string {
	return fmt.Sprintf(FilePattern, identifier)
}

func NewJournal(name string) *Journal {
	// dump every oplog detail if needed
	if file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0666); err == nil {
		w := bufio.NewWriterSize(file, BufferCapacity)
		return &Journal{writer: w, hasher: &oplog.PrimaryKeyHasher{}}
	}
	return nil
}

func (j *Journal) WriteRecord(oplog *oplog.PartialLog) {
	// 0 -> no journal
	// 1 -> sampling
	// 2 -> journal all
	switch utils.IncrSentinelOptions.OplogDump {
	case NothingOnDefault: // default. do nothing
	case Sampling:
		// object id will be sampled and all DDL oplog
		if j.hasher.DistributeOplogByMod(oplog, SampleFrequency) != 0 {
			break
		}
		fallthrough
	case All:
		j.journal(oplog)
	default:
	}
}

func (j *Journal) journal(oplog *oplog.PartialLog) {
	j.writer.WriteString(fmt.Sprintf("%v\n", oplog))
}
