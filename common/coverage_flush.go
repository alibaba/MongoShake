package utils

import (
	"os"
	"runtime/coverage"
	"time"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const coverageFlushInterval = 5 * time.Second

// StartCoverageFlush periodically writes Go coverage counters to GOCOVERDIR.
// Go emits them at exit, but the integration suite tears the collector down with
// SIGKILL, so that emission never happens and the process contributes nothing to
// a profile despite executing most of the sync path.
//
// A no-op unless GOCOVERDIR is set, so production runs pay nothing. Requires a
// binary built with -cover -covermode=atomic (the runtime rejects set mode
// because counters are read while the program runs) and a toolchain providing
// runtime/coverage.
//
// Each WriteCountersDir call emits a NEW timestamped file, so a long-lived
// process with GOCOVERDIR set accumulates one file per interval without bound.
// That is acceptable for test runs (the directory is a per-run temp dir) but is
// why this must stay off in production.
func StartCoverageFlush() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(coverageFlushInterval)
		defer ticker.Stop()
		for range ticker.C {
			if err := coverage.WriteCountersDir(dir); err != nil {
				// Not built with -cover, or the directory is gone. Retrying
				// every interval would only spam the log.
				l.Logger.Warnf("coverage flush stopped: %v", err)
				return
			}
		}
	}()
}
