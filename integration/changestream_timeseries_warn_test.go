//go:build integration

// End-to-end coverage for the change_stream/time-series startup warning (#1000).
//
// The unit tests in collector/coordinator call warnChangeStreamCannotSyncTimeSeries
// directly, so none of them fails if the call site at the top of startOplogReplication is
// deleted. This runs a real collector in change_stream mode against a real time-series
// collection and requires the warning to appear in its log, which is what pins the
// wiring.
//
// The warning has to be asserted from a running collector rather than from the symptom it
// describes, because the symptom is invisible: the change stream opens and resumes
// normally, the checkpoint advances on schedule, and write_success simply stays at 0.
//
// Run with:
//
//	go test -count=1 -tags integration ./integration -run TestChangeStreamWarnsOnTimeSeries -v -timeout 15m
//
// -count=1 is required, not optional: the code under test is compiled into a `go run`
// subprocess rather than imported by this package, so the test cache does not see it
// change and will replay a stale PASS after the warning is edited or removed.
//
// Environment (overridable):
//
//	MSHAKE_CS_SRC_URL     source mongodb, must be a replica set (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL        target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_COLLECTOR_BIN  prebuilt collector binary (default: launch via `go run`)
package integration

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	cswTestDB   = "cswarn_ts"
	cswTestColl = "gps"
	cswCkptColl = "ckpt_cswarn_it"
	cswID       = "cswarn_it"
	// the warning's own wording, matched rather than the whole line so that rewording
	// the surrounding prose does not silently break the assertion
	cswWarnMarker = "cannot sync time-series collections"
)

// cswWriteConf is csWriteConf with its own id, ports and checkpoint collection, plus the
// namespace whitelist the warning is gated on. It reuses csLogFile because
// csStartCollector reads the log back from exactly that name inside logDir.
func cswWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(csRepoRoot(t), "conf", "collector.conf"))
	if err != nil {
		t.Fatalf("read conf template: %v", err)
	}
	conf := string(tpl)
	set := func(key, value string) {
		re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*$`)
		if !re.MatchString(conf) {
			conf += "\n" + key + " = " + value + "\n"
			return
		}
		conf = re.ReplaceAllString(conf, key+" = "+value)
	}
	set("id", cswID)
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone") // direct connection to a mapped port
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("tunnel", "direct")
	// The warning only names collections this run was asked to sync, so the whitelist has
	// to cover the time-series collection for it to fire at all. That scoping is itself
	// asserted by TestTimeSeriesUnsupportedByChangeStream.
	set("filter.namespace.white", cswTestDB+"."+cswTestColl)
	set("filter.namespace.black", "")
	set("checkpoint.storage.collection", cswCkptColl)
	set("checkpoint.interval", "1000")
	// start from now so the retained oplog is not replayed
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	// distinct from the #986 test in this package, which uses 193xx and 19400
	set("full_sync.http_port", "19311")
	set("incr_sync.http_port", "19310")
	set("prom.http_port", "19312")
	set("system_profile_port", "19410")
	set("log.dir", logDir)
	set("log.file", csLogFile)
	// this test asserts on log content, and the default log.flush=false buffers writes for
	// up to a second, which would make the assertion racy
	set("log.flush", "true")
	// pinned; the template's value was an unevaluated expression before #998
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-cswarn.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func TestChangeStreamWarnsOnTimeSeries(t *testing.T) {
	srcURL := csEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := csEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	ctx := context.Background()
	src, err := mongo.Connect(ctx, options.Client().ApplyURI(srcURL))
	if err != nil {
		t.Skipf("no source mongodb at %s: %v", srcURL, err)
	}
	defer func() { _ = src.Disconnect(ctx) }()
	if err = src.Ping(ctx, nil); err != nil {
		t.Skipf("source mongodb at %s unreachable: %v", srcURL, err)
	}

	// A stored checkpoint wins over checkpoint.start_position, so a leftover one would
	// make the collector resume from an old position. The source database is dropped on
	// the way out too: a time-series database left behind holds a dangling view
	// definition, which makes the server reject listCollections for that whole database.
	_ = src.Database(cswTestDB).Drop(ctx)
	_ = src.Database(csCkptDB).Collection(cswCkptColl).Drop(ctx)
	defer func() {
		_ = src.Database(cswTestDB).Drop(ctx)
		_ = src.Database(csCkptDB).Collection(cswCkptColl).Drop(ctx)
	}()

	err = src.Database(cswTestDB).RunCommand(ctx, bson.D{
		{Key: "create", Value: cswTestColl},
		{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "ts"}}},
	}).Err()
	if err != nil {
		t.Skipf("source cannot create time-series collections (needs 5.0+): %v", err)
	}
	_, err = src.Database(cswTestDB).Collection(cswTestColl).InsertOne(ctx,
		bson.D{{Key: "ts", Value: time.Now()}, {Key: "v", Value: 1}})
	if err != nil {
		t.Fatalf("insert a measurement so the collection is not empty: %v", err)
	}

	logDir := t.TempDir()
	proc := csStartCollector(t, cswWriteConf(t, srcURL, tgtURL, logDir), logDir)
	defer proc.kill()

	// The warning is emitted at the top of startOplogReplication, before the syncer is
	// built and therefore before the change stream is opened, so polling for the warning
	// itself is both the earliest and the most specific readiness signal available here.
	deadline := time.Now().Add(csReadyTO)
	for time.Now().Before(deadline) {
		proc.requireAlive(t)
		if proc.logContains(cswWarnMarker) {
			break
		}
		// An open change stream proves the warning's slot in the startup sequence has
		// already been passed, so either it was logged or it never will be. Breaking here
		// keeps a genuine failure fast instead of waiting out the rest of the deadline,
		// which is sized for a cold `go run` compile rather than for this check.
		if proc.logContains("new change stream with options") {
			t.Log("change stream opened without the warning having been logged")
			break
		}
		time.Sleep(csPoll)
	}

	data, err := os.ReadFile(proc.logPath)
	if err != nil {
		proc.dumpLog(t)
		t.Fatalf("read the collector log: %v", err)
	}
	logged := string(data)
	if !strings.Contains(logged, cswWarnMarker) {
		proc.dumpLog(t)
		t.Fatalf("collector never warned that change_stream cannot sync time-series "+
			"collections; log searched for %q", cswWarnMarker)
	}
	for _, want := range []string{
		cswTestDB + "." + cswTestColl,
		"incr_sync.mongo_fetch_method = oplog",
		"https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/",
	} {
		if !strings.Contains(logged, want) {
			proc.dumpLog(t)
			t.Errorf("the warning must tell the operator %q", want)
		}
	}
}
