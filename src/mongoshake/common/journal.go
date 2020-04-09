package utils

import (
	"bufio"
	"fmt"
	"mongoshake/oplog"
	"os"
	"path/filepath"
)

const (
	SampleFrequency = 1000
)

const (
	JournalNothingOnDefault = iota
	JournalSampling
	JournalAll
)

const (
	BufferCapacity = 4 * 1024 * 1024
)

var JournalFilePattern = GlobalDiagnosticPath + string(filepath.Separator) + "%s.journal"

type Journal struct {
	writer *bufio.Writer
	hasher oplog.Hasher
}

func JournalFileName(identifier string) string {
	return fmt.Sprintf(JournalFilePattern, identifier)
}

func NewJournal(name string) *Journal {
	// dump every oplog detail if need
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
	switch IncrSentinelOptions.OplogDump {
	case JournalNothingOnDefault: // default. do nothing
	case JournalSampling:
		// object id will be sampled and all DDL oplog
		if j.hasher.DistributeOplogByMod(oplog, SampleFrequency) != 0 {
			break
		}
		fallthrough
	case JournalAll:
		j.journal(oplog)
	default:
	}
}

func (j *Journal) journal(oplog *oplog.PartialLog) {
	j.writer.WriteString(fmt.Sprintf("%v\n", oplog))
}
