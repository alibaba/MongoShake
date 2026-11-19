//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Every integration launch site tears the collector down with SIGKILL, so Go's
// at-exit coverage emission never runs and the process that executes most of the
// sync path contributes nothing to a profile. Counters must reach disk while the
// process is alive, and stay usable afterwards.
func TestCollectorCoverageSurvivesSigkill(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := dial(t, srcURL)

	covDir := t.TempDir()
	// The launch helpers inherit the test process environment, so these reach
	// the `go run ./cmd/collector` child without touching any helper.
	// WriteCountersDir rejects -covermode=set: reading counters while the
	// program runs needs atomic mode.
	t.Setenv("GOCOVERDIR", covDir)
	t.Setenv("GOFLAGS", "-cover -covermode=atomic")

	logDir := t.TempDir()
	confPath := writeConf(t, srcURL, tgtURL, logDir)

	// A stale checkpoint would let waitCheckpoint return before the collector
	// finished initialising, and the kill could land before the first flush.
	_ = src.Database(ckptDB).Collection(ckptColl).Drop(context.Background())

	runID := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	stopHb := startHeartbeat(t, src.Database(testDB).Collection(testColl), runID)
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	// Without this, a failing assertion below leaks a collector that outlives
	// the test binary (Setpgid, no Pdeathsig) and keeps the shared ports/target.
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	waitCheckpoint(t, src, startupTO)

	// Wait for the flush BEFORE killing. covmeta is written at process init, so
	// waiting for "any file" would be satisfied instantly and prove nothing;
	// only a covcounters file shows the ticker actually ran.
	waitCoverageCounters(t, covDir, 60*time.Second)

	proc.kill()

	entries, err := os.ReadDir(covDir)
	if err != nil {
		t.Fatalf("read coverage dir after kill: %v", err)
	}
	hasCounters := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "covcounters.") {
			hasCounters = true
		}
	}
	if !hasCounters {
		t.Fatalf("counters vanished after SIGKILL: %v", entries)
	}

	// `covdata pkglist` reads the meta file, so it lists every compiled package
	// even with zero counters -- asserting on it would be vacuous. percent has
	// to show that a package on the incremental sync path really executed.
	readerPkg := "github.com/alibaba/MongoShake/v2/collector/reader"
	pct := covPercent(t, covDir, readerPkg)
	if pct <= 0 {
		t.Fatalf("collector/reader shows %.1f%% coverage; counters carry no execution data", pct)
	}
	t.Logf("coverage survived SIGKILL: %d files, reader=%.1f%%", len(entries), pct)
}

func waitCoverageCounters(t *testing.T, dir string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "covcounters.") {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no covcounters file in %s within %v: the collector never flushed", dir, timeout)
}

var covPercentRe = regexp.MustCompile(`coverage:\s*([0-9.]+)%`)

func covPercent(t *testing.T, dir, pkg string) float64 {
	t.Helper()
	out, err := exec.Command("go", "tool", "covdata", "percent", "-i="+dir).CombinedOutput()
	if err != nil {
		t.Fatalf("covdata percent failed: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, pkg) {
			continue
		}
		if m := covPercentRe.FindStringSubmatch(line); m != nil {
			v, err := strconv.ParseFloat(m[1], 64)
			if err != nil {
				t.Fatalf("parse coverage %q: %v", m[1], err)
			}
			return v
		}
	}
	t.Fatalf("package %s absent from covdata percent output:\n%s", pkg, out)
	return 0
}
