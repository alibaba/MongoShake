package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestDupKeyLogFlushesWithoutFurtherReports(t *testing.T) {
	logs := make(chan string, 10)
	d := newDupKeyLog(100*time.Millisecond, func(format string, args ...any) {
		logs <- fmt.Sprintf(format, args...)
	})
	defer d.Flush()
	d.ReportHandled("db.coll", 2, errors.New("duplicate"))
	require.Contains(t, <-logs, "docs[2]")
	d.ReportHandled("db.coll", 3, nil)
	d.ReportHandled("db.coll", 4, nil)

	// No further reports or explicit flush: the timer must drain the tail.
	select {
	case summary := <-logs:
		require.Contains(t, summary, "additional batches[2] docs[7] over ")
		require.NotContains(t, summary, "last ~1m0s")
	case <-time.After(5 * time.Second):
		t.Fatal("pending batches were not flushed after reports stopped")
	}
	d.Flush()
	require.Empty(t, logs, "explicit flush must not duplicate a timer summary")
	d.mu.Lock()
	remaining, timer := len(d.stats), d.timer
	d.mu.Unlock()
	require.Zero(t, remaining, "idle namespaces must not be retained")
	require.Nil(t, timer)
}

func TestDupKeyLogFlushSeparatesNamespacesAndWindows(t *testing.T) {
	var logs []string
	d := newDupKeyLog(time.Hour, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	defer d.Flush()
	d.ReportHandled("db.a", 2, nil)
	d.ReportHandled("db.a", 3, nil)
	d.ReportHandled("db.b", 5, nil)
	d.ReportHandled("db.b", 7, nil)
	// Ensure the summary uses actual elapsed time, not the configured interval.
	d.mu.Lock()
	d.stats["db.a"].firstTs = time.Now().Add(-2 * time.Hour)
	staleGeneration := d.generation
	d.mu.Unlock()
	d.Flush()
	require.Len(t, logs, 4)
	require.Contains(t, strings.Join(logs, "\n"), "ns[db.a]: additional batches[1] docs[3] over 2h")
	require.Contains(t, strings.Join(logs, "\n"), "ns[db.b]: additional batches[1] docs[7]")
	d.Flush()
	require.Len(t, logs, 4, "repeated flush must be idempotent")

	d.ReportHandled("db.a", 11, nil)
	d.ReportHandled("db.a", 13, nil)
	d.flush(staleGeneration) // A callback already queued before Flush must do nothing.
	require.Len(t, logs, 5)
	d.Flush()
	require.Len(t, logs, 6)
	require.Contains(t, logs[4], "docs[11]")
	require.Contains(t, logs[5], "additional batches[1] docs[13]")
}

func TestDupKeyLogConcurrentReportsAndFlush(t *testing.T) {
	var logs []string
	d := newDupKeyLog(time.Millisecond, func(format string, args ...any) {
		// Emission is serialized with both explicit flushes and timer callbacks.
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	defer d.Flush()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := 0; batch < 100; batch++ {
				d.ReportHandled("db.coll", 2, nil)
				if batch%10 == 0 {
					d.Flush()
				}
			}
		}()
	}
	wg.Wait()
	d.Flush()
	var batches, docs int
	pattern := regexp.MustCompile(`(?:additional batches\[(\d+)\] )?docs\[(\d+)\]`)
	for _, line := range logs {
		match := pattern.FindStringSubmatch(line)
		require.Len(t, match, 3)
		count := 1
		if match[1] != "" {
			count, _ = strconv.Atoi(match[1])
		}
		batchDocs, _ := strconv.Atoi(match[2])
		batches += count
		docs += batchDocs
	}
	require.Equal(t, 800, batches, "each batch must be logged exactly once")
	require.Equal(t, 1600, docs)
}

func TestAllWriteErrorsDupKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  mongo.BulkWriteException
		want bool
	}{
		{"empty", mongo.BulkWriteException{}, false},
		{"duplicates", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11000}},
			{WriteError: mongo.WriteError{Code: 11000}},
		}}, true},
		{"legacy update duplicate", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11001}},
		}}, true},
		{"legacy capped duplicate", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 12582}},
		}}, true},
		{"legacy mongos duplicate", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 16460, Message: "error E11000 duplicate"}},
		}}, true},
		{"nonduplicate mongos error", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 16460, Message: "other error"}},
		}}, false},
		{"mixed", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Code: 11000}},
			{WriteError: mongo.WriteError{Code: 121}},
		}}, false},
		{"write concern", mongo.BulkWriteException{
			WriteErrors:       []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}},
			WriteConcernError: &mongo.WriteConcernError{Code: 64},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AllWriteErrorsDupKey(tc.err))
		})
	}
}

func TestTruncateError(t *testing.T) {
	require.Empty(t, TruncateError(nil, 3))
	require.Equal(t, "short", TruncateError(errors.New("short"), 5))
	require.Equal(t, "重复键...", TruncateError(errors.New("重复键错误"), 3))
}
