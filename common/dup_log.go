package utils

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// dupKeyLogInterval is the flush interval for aggregated duplicate-key
// summaries. Handled E11000 batches within one window are collapsed into a
// single line per namespace.
const dupKeyLogInterval = 60 * time.Second

// TruncateError returns err.Error() truncated to maxRunes runes (rune-safe).
// Used to keep one-line summaries readable when the underlying error embeds
// bulk write details (e.g. a 100+ entry duplicate key report).
func TruncateError(err error, maxRunes int) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}

// AllWriteErrorsDupKey reports whether every write error in a bulk write
// exception is a duplicate key error (code 11000) with no write concern issue.
// A batch that mixes duplicate keys with other errors (immutable shard key,
// write concern, ...) must keep the verbose error path.
func AllWriteErrorsDupKey(bulkErr mongo.BulkWriteException) bool {
	if bulkErr.WriteConcernError != nil {
		return false
	}
	if len(bulkErr.WriteErrors) == 0 {
		return false
	}
	for _, we := range bulkErr.WriteErrors {
		if we.Code != 11000 {
			return false
		}
	}
	return true
}

type dupKeyNsStat struct {
	batches int64
	docs    int64
	firstTs time.Time
	lastTs  time.Time
}

// DupKeyLog aggregates handled duplicate-key incidents per namespace. When a
// flood of duplicate key batches is successfully handled (converted to update,
// ignored, or skipped), reporting every batch verbatim drowns out real logs:
// the first occurrence is logged as a summary once, then one aggregate line
// per namespace per interval.
type DupKeyLog struct {
	mu    sync.Mutex
	stats map[string]*dupKeyNsStat
}

// NewDupKeyLog creates an empty DupKeyLog aggregator.
func NewDupKeyLog() *DupKeyLog {
	return &DupKeyLog{stats: make(map[string]*dupKeyNsStat)}
}

// GlobalDupKeyLog is the process-wide aggregator shared by full-sync and
// incremental writers.
var GlobalDupKeyLog = NewDupKeyLog()

// ReportHandled records one handled duplicate-key batch on ns. docCount is the
// number of documents in the batch; err is the underlying bulk error, kept
// truncated in the first-occurrence log for context.
func (d *DupKeyLog) ReportHandled(ns string, docCount int, err error) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()

	st, ok := d.stats[ns]
	if !ok {
		d.stats[ns] = &dupKeyNsStat{
			batches: 1,
			docs:    int64(docCount),
			firstTs: now,
			lastTs:  now,
		}
		l.Logger.Infof("dup key handled on ns[%v]: docs[%d] (convert-to-update/ignore/skip), first err: %v",
			ns, docCount, TruncateError(err, 300))
		return
	}

	st.batches++
	st.docs += int64(docCount)
	if now.Sub(st.lastTs) >= dupKeyLogInterval {
		l.Logger.Infof("dup key still handled on ns[%v]: batches[%d] docs[%d] in last ~%v",
			ns, st.batches, st.docs, dupKeyLogInterval)
		st.batches = 0
		st.docs = 0
		st.firstTs = now
		st.lastTs = now
	}
}
