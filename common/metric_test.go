package utils

import (
	"runtime"
	"testing"
	"time"
)

// TestMetricCloseIsRaceFree covers the happens-before edge between Close() and the
// ticker goroutine started by startup(). Run it with -race:
//
//	go test -race ./common -run TestMetricCloseIsRaceFree
//
// Unlike the -count=5 lottery that first surfaced this, it is deterministic: the race
// detector keeps shadow memory of past accesses and compares vector clocks, so it does
// not need the read and the write to overlap in time. Sleeping past one tick guarantees
// the goroutine has already read isClosed, and Close() then writes it from the test
// goroutine. With a plain bool there is no edge between the two and the pair is
// reported; with an atomic there is one and nothing is reported.
//
// One caveat when reading the output: the race runtime reports a given pair of access
// stacks only once per process, so with -count=N only the first iteration fails. A plain
// run is the reliable gate, and it fails on every fresh process.
//
// Without -race this test asserts nothing and always passes.
func TestMetricCloseIsRaceFree(t *testing.T) {
	metric := NewMetric("rs-close-race", TypeFull, 0)

	time.Sleep(metricTickPeriod + 100*time.Millisecond)

	metric.Close()
}

// TestMetricCloseStopsTickerGoroutine covers the other half: the goroutine startup()
// leaves behind has to actually terminate. TestMetricCloseIsRaceFree cannot see this,
// because dropping the isClosed check entirely removes the racy access and so removes
// the race report, while leaking the goroutine for the lifetime of the process.
//
// The comparison is against the count taken after NewMetric rather than an absolute
// baseline, so goroutines leaked by unrelated tests cannot fail this one.
func TestMetricCloseStopsTickerGoroutine(t *testing.T) {
	metric := NewMetric("rs-close-exit", TypeFull, 0)

	time.Sleep(metricTickPeriod + 100*time.Millisecond)
	withMetric := runtime.NumGoroutine()

	metric.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() < withMetric {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("metric goroutine still running %v after Close(): have %d goroutines, want fewer than %d",
		time.Since(deadline.Add(-5*time.Second)).Round(time.Millisecond),
		runtime.NumGoroutine(), withMetric)
}
