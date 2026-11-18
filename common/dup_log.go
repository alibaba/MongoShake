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
// exception is a duplicate key error recognized by the driver, with no write
// concern issue.
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
		if !DuplicateKey(we) {
			return false
		}
	}
	return true
}

type dupKeyNsStat struct {
	batches int64
	docs    int64
	firstTs time.Time
}

// DupKeyLog aggregates successfully handled duplicate-key batches per namespace.
// The first batch is logged immediately; later batches are flushed by a timer,
// even when no more duplicate keys arrive. Idle namespaces are then discarded.
type DupKeyLog struct {
	mu         sync.Mutex
	stats      map[string]*dupKeyNsStat
	timer      *time.Timer
	generation uint64
	interval   time.Duration
	logf       func(string, ...any)
}

// NewDupKeyLog creates an empty DupKeyLog aggregator. No timer is started until
// the first handled batch arrives.
func NewDupKeyLog() *DupKeyLog {
	return newDupKeyLog(dupKeyLogInterval, func(format string, args ...any) {
		l.Logger.Infof(format, args...)
	})
}

func newDupKeyLog(interval time.Duration, logf func(string, ...any)) *DupKeyLog {
	return &DupKeyLog{
		stats:    make(map[string]*dupKeyNsStat),
		interval: interval,
		logf:     logf,
	}
}

// GlobalDupKeyLog is the process-wide aggregator shared by full-sync and
// incremental writers.
var GlobalDupKeyLog = NewDupKeyLog()

// ReportHandled records one successfully handled duplicate-key batch on ns.
// docCount is the number of duplicate write errors, not the insert batch size.
// err is the original bulk
// error, truncated in the first-occurrence log for context.
func (d *DupKeyLog) ReportHandled(ns string, docCount int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	st, ok := d.stats[ns]
	if !ok {
		// This batch is already accounted for by the immediate log. Only
		// subsequent batches belong in the pending summary.
		d.stats[ns] = &dupKeyNsStat{firstTs: time.Now()}
		d.logf("dup key handled on ns[%v]: docs[%d] (convert-to-update/ignore/skip), first err: %v",
			ns, docCount, TruncateError(err, 300))
	} else {
		st.batches++
		st.docs += int64(docCount)
	}

	if d.timer == nil {
		d.generation++
		generation := d.generation
		d.timer = time.AfterFunc(d.interval, func() { d.flush(generation) })
	}
}

// Flush writes all pending summaries and stops the current timer. Call it when
// full sync finishes and before closing the logger. It is safe to call more
// than once, or concurrently with ReportHandled and the timer callback.
func (d *DupKeyLog) Flush() {
	d.flush(0)
}

func (d *DupKeyLog) flush(generation uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// A stopped callback can already be waiting for the mutex. It must not
	// flush a newer window started after an explicit Flush.
	if generation != 0 && (generation != d.generation || d.timer == nil) {
		return
	}
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}

	now := time.Now()
	for ns, st := range d.stats {
		if st.batches > 0 {
			d.logf("dup key handled on ns[%v]: additional batches[%d] docs[%d] over %v",
				ns, st.batches, st.docs, now.Sub(st.firstTs))
		}
		delete(d.stats, ns)
	}
}
