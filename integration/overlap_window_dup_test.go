//go:build integration

// End-to-end verification for #1047: inside the full/incr overlap window an
// insert hitting an _id duplicate is expected (the full scan already carried
// the document), so it must be acknowledged without the whole-batch $set
// conversion. Two legs:
//
//  1. incr restart: a checkpoint carrying full_finish_ts restores the window
//     after a hard SIGKILL mid-catch-up; the newer scanned version on the
//     target must survive the replay (no regression to the oplog's version).
//  2. parallel mode (oplog_store_disk): the full-finish timestamp is
//     backfilled before disk apply, so writes made during the full scan are
//     replayed as expected dups instead of being converted to updates.
//
// Red/green: on a baseline without #1047 the window value is 0/absent, the
// conversion runs, the target version regresses and opcounters.update grows.
//
// Run with:
//
//	go test -tags integration ./integration -run TestOverlapWindow -v -timeout 15m
//
// Environment (overridable):
//
//	MSHAKE_CS_URL      replica-set source (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL     target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_TGT_CONTAINER  target docker container, paused to stall replay
//	                      (default mshake-txn-tgt)
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	overlapDB   = "overlap1047"
	overlapColl = "c1"

	overlapIncrCkptColl     = "ckpt_overlap1047_incr_it"
	overlapParallelCkptColl = "ckpt_overlap1047_parallel_it"

	overlapSeedDocs         = 3000
	overlapParallelSeedDocs = 60000
)

func overlapWriteConf(t *testing.T, srcURL, tgtURL, logDir, mode, ckptColl string, extra map[string]string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(repoRoot(t), "conf", "collector.conf"))
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
	set("id", "overlap1047_it")
	set("sync_mode", mode)
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("incr_sync.mongo_fetch_method", "oplog")
	set("incr_sync.executor.insert_on_dup_update", "true")
	set("incr_sync.executor.dup_key_strategy", "ignore")
	set("incr_sync.worker", "1")
	set("checkpoint.storage.url", srcURL)
	set("checkpoint.storage.collection", ckptColl)
	set("checkpoint.interval", "1000")
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	// ports must not collide with the other integration tests
	set("full_sync.http_port", "19131")
	set("incr_sync.http_port", "19130")
	set("prom.http_port", "19132")
	set("system_profile_port", "19230")
	set("log.dir", logDir)
	set("log.file", "collector-it.log")
	set("tunnel.kafka.producer.max_message_bytes", "18874368")
	for key, value := range extra {
		set(key, value)
	}
	path := filepath.Join(logDir, "collector-overlap.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func overlapReplsetName(t *testing.T, src *mongo.Client) string {
	t.Helper()
	var hello bson.M
	require := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("hello: %v", err)
		}
	}
	require(src.Database("admin").RunCommand(context.Background(), bson.D{{Key: "hello", Value: 1}}).Decode(&hello))
	name, ok := hello["setName"].(string)
	if !ok || name == "" {
		t.Fatalf("source is not a replica set: %v", hello)
	}
	return name
}

func overlapSeed(t *testing.T, src, tgt *mongo.Client, idBase, n int, srcVersion, tgtVersion int) {
	t.Helper()
	ctx := context.Background()
	srcDocs := make([]interface{}, 0, n)
	tgtDocs := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		srcDocs = append(srcDocs, bson.D{{Key: "_id", Value: idBase + i}, {Key: "v", Value: srcVersion}})
		tgtDocs = append(tgtDocs, bson.D{{Key: "_id", Value: idBase + i}, {Key: "v", Value: tgtVersion}})
	}
	_, err := src.Database(overlapDB).Collection(overlapColl).InsertMany(ctx, srcDocs)
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}
	_, err = tgt.Database(overlapDB).Collection(overlapColl).InsertMany(ctx, tgtDocs)
	if err != nil {
		t.Fatalf("seed target: %v", err)
	}
}

// overlapStartProfiling enables a FRESH profiler on the target database so
// update attempts can be counted per namespace (opcounters is server-wide and
// would catch unrelated traffic; a no-op $set leaves no oplog entry but IS
// profiled). Earlier entries are dropped so the count needs no time filter —
// profile timestamps are millisecond-truncated and race with time.Now().
// It returns a reset closure the caller must DEFER (after its client-close
// defer, so LIFO runs the reset while the client is still open); a t.Cleanup
// here would run after the close and leave profiling enabled on the shared
// database (#1050 review M4).
func overlapStartProfiling(t *testing.T, tgt *mongo.Client) func() {
	t.Helper()
	ctx := context.Background()
	if err := tgt.Database(overlapDB).RunCommand(ctx, bson.D{{Key: "profile", Value: 0}}).Err(); err != nil {
		t.Fatalf("disable profiling: %v", err)
	}
	_ = tgt.Database(overlapDB).Collection("system.profile").Drop(ctx)
	if err := tgt.Database(overlapDB).RunCommand(ctx, bson.D{{Key: "profile", Value: 2}}).Err(); err != nil {
		t.Fatalf("enable profiling: %v", err)
	}
	return func() {
		if err := tgt.Database(overlapDB).RunCommand(context.Background(),
			bson.D{{Key: "profile", Value: 0}}).Err(); err != nil {
			t.Errorf("reset profiling: %v", err)
		}
	}
}

func overlapCountProfiledUpdates(t *testing.T, tgt *mongo.Client) int64 {
	t.Helper()
	n, err := tgt.Database(overlapDB).Collection("system.profile").CountDocuments(context.Background(),
		bson.M{"ns": overlapDB + "." + overlapColl, "op": "update"})
	if err != nil {
		t.Fatalf("count profiled updates: %v", err)
	}
	return n
}

func overlapReadCkptDoc(t *testing.T, src *mongo.Client, ckptColl string) bson.M {
	t.Helper()
	var doc bson.M
	err := src.Database(ckptDB).Collection(ckptColl).FindOne(context.Background(), bson.M{}).Decode(&doc)
	if err != nil {
		t.Fatalf("read checkpoint doc: %v", err)
	}
	return doc
}

func overlapWaitCkptAtLeast(t *testing.T, src *mongo.Client, ckptColl string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		var doc bson.M
		err := src.Database(ckptDB).Collection(ckptColl).FindOne(context.Background(), bson.M{}).Decode(&doc)
		if err == nil {
			if v, ok := doc["ckpt"].(int64); ok {
				last = v
				if v >= want {
					return
				}
			}
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("checkpoint stuck at %x, never reached %x within %v", last, want, timeout)
}

func overlapWaitLogMarker(t *testing.T, logDir, marker string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filepath.Join(logDir, "collector-it.log"))
		if err == nil && strings.Contains(string(data), marker) {
			return
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("log marker %q not seen within %v", marker, timeout)
}

func overlapAssertVersions(t *testing.T, tgt *mongo.Client, wantCount int64, wantVersion int) {
	t.Helper()
	ctx := context.Background()
	n, err := tgt.Database(overlapDB).Collection(overlapColl).CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count target: %v", err)
	}
	if n != wantCount {
		t.Fatalf("target has %d docs, want %d", n, wantCount)
	}
	wrong, err := tgt.Database(overlapDB).Collection(overlapColl).CountDocuments(ctx,
		bson.M{"v": bson.M{"$ne": wantVersion}})
	if err != nil {
		t.Fatalf("count wrong versions: %v", err)
	}
	if wrong != 0 {
		t.Fatalf("%d target docs regressed away from v=%d (whole-batch conversion ran)", wrong, wantVersion)
	}
}

// Leg 1: incr restart restores the window from the checkpoint. The target is
// paused mid-replay so the SIGKILL provably lands before catch-up finishes.
func TestOverlapWindowIncrRestartKeepsWindow(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	tgtContainer := envOr("MSHAKE_TGT_CONTAINER", "mshake-txn-tgt")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	for _, c := range []struct {
		client *mongo.Client
		db     string
		coll   string
	}{
		{src, overlapDB, overlapColl}, {src, overlapDB, "hb"},
		{tgt, overlapDB, overlapColl}, {src, ckptDB, overlapIncrCkptColl},
	} {
		if err := c.client.Database(c.db).Collection(c.coll).Drop(ctx); err != nil {
			t.Fatalf("drop %s.%s: %v", c.db, c.coll, err)
		}
	}
	// pre-create so no DDL oplog lands inside the window (the batcher's
	// DDL-before-fullFinish guard panics on it by design)
	for _, client := range []*mongo.Client{src, tgt} {
		if err := client.Database(overlapDB).CreateCollection(ctx, overlapColl); err != nil {
			t.Fatalf("create collection: %v", err)
		}
	}
	if err := src.Database(overlapDB).CreateCollection(ctx, "hb"); err != nil {
		t.Fatalf("create hb: %v", err)
	}

	// source carries v1; the target already holds the same _ids at a NEWER
	// version, exactly as a full scan that ran past the inserts would leave it
	start := packed(latestOplogTs(t, src))
	overlapSeed(t, src, tgt, 0, overlapSeedDocs, 1, 2)
	fence := packed(latestOplogTs(t, src))

	// hand-write the checkpoint a full-sync handover would leave behind
	ckptDoc := bson.M{
		"name":           overlapReplsetName(t, src),
		"ckpt":           start,
		"version":        2,
		"fetch_method":   "oplog",
		"full_finish_ts": fence,
	}
	if _, err := src.Database(ckptDB).Collection(overlapIncrCkptColl).InsertOne(ctx, ckptDoc); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	logDir := t.TempDir()
	confPath := overlapWriteConf(t, srcURL, tgtURL, logDir, "incr", overlapIncrCkptColl, nil)

	stopHb := startHeartbeat(t, src.Database(overlapDB).Collection("hb"), runID)
	t.Cleanup(stopHb)

	defer overlapStartProfiling(t, tgt)()

	// Pause the target BEFORE the collector starts so the first run provably
	// cannot ack anything: the SIGKILL below always lands mid-catch-up, on
	// fast machines too.
	if out, err := exec.Command("docker", "pause", tgtContainer).CombinedOutput(); err != nil {
		t.Skipf("cannot pause target container %s: %v (%s)", tgtContainer, err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "unpause", tgtContainer).Run() })

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() {
		proc.kill()
		proc.dumpLog(t)
		if data, err := os.ReadFile(filepath.Join(logDir, "collector-it.log")); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 60 {
				lines = lines[len(lines)-60:]
			}
			t.Logf("collector-it.log tail:\n%s", strings.Join(lines, "\n"))
		}
	})

	// Sanity check only (#1050 review N5): the hand-written checkpoint already
	// carries the field and $set never removes it, so these assertions also
	// pass on develop. The real red anchor for startup persistence is
	// TestOverlapWindowSerializePersistsBound.
	deadline := time.Now().Add(startupTO)
	for time.Now().Before(deadline) {
		doc := overlapReadCkptDoc(t, src, overlapIncrCkptColl)
		if v, ok := doc["full_finish_ts"].(int64); ok && v == fence {
			break
		}
		time.Sleep(pollPeriod)
	}
	doc := overlapReadCkptDoc(t, src, overlapIncrCkptColl)
	if v, _ := doc["full_finish_ts"].(int64); v != fence {
		t.Fatalf("full_finish_ts not persisted at startup: %v", doc)
	}

	// hard-kill mid-catch-up (the paused target guarantees nothing was acked)
	time.Sleep(3 * time.Second)
	proc.kill()
	doc = overlapReadCkptDoc(t, src, overlapIncrCkptColl)
	if v, _ := doc["ckpt"].(int64); v >= fence {
		t.Fatalf("replay finished before the kill; restart leg would be vacuous (ckpt=%x fence=%x)", v, fence)
	}
	if v, _ := doc["full_finish_ts"].(int64); v != fence {
		t.Fatalf("full_finish_ts lost across the kill: %v", doc)
	}
	if out, err := exec.Command("docker", "unpause", tgtContainer).CombinedOutput(); err != nil {
		t.Fatalf("unpause target: %v (%s)", err, out)
	}

	// restart: the window must be restored from the checkpoint alone
	proc = startCollector(t, confPath, logDir)
	overlapWaitCkptAtLeast(t, src, overlapIncrCkptColl, fence, syncTO)
	// let the tail batches flush
	time.Sleep(3 * time.Second)

	overlapAssertVersions(t, tgt, overlapSeedDocs, 2)
	// heartbeat entries re-replayed after the restart may legitimately fall
	// OUTSIDE the window (ts > fence) and convert; only c1 is asserted here
	if n := overlapCountProfiledUpdates(t, tgt); n != 0 {
		t.Fatalf("expected zero conversion updates on %s inside the window, got %d", overlapColl, n)
	}
}

// Leg 2: parallel mode backfills the window before disk apply. Documents
// inserted while the full scan runs are carried by the scan AND replayed from
// the spool; the replay must acknowledge them as expected dups.
func TestOverlapWindowParallelDiskApply(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	for _, c := range []struct {
		client *mongo.Client
		db     string
		coll   string
	}{
		{src, overlapDB, overlapColl}, {src, overlapDB, "hb"},
		{tgt, overlapDB, overlapColl}, {tgt, overlapDB, "hb"},
		{src, ckptDB, overlapParallelCkptColl},
	} {
		if err := c.client.Database(c.db).Collection(c.coll).Drop(ctx); err != nil {
			t.Fatalf("drop %s.%s: %v", c.db, c.coll, err)
		}
	}
	for _, client := range []*mongo.Client{src, tgt} {
		if err := client.Database(overlapDB).CreateCollection(ctx, "hb"); err != nil {
			t.Fatalf("create hb: %v", err)
		}
	}

	// seed ahead of time; the scan walks _id ascending, so documents inserted
	// during the scan with larger _ids land in front of the cursor and are
	// carried by it — the exact shape that produces overlap-window dups
	seedDocs := make([]interface{}, 0, overlapParallelSeedDocs)
	for i := 0; i < overlapParallelSeedDocs; i++ {
		seedDocs = append(seedDocs, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: 1}})
	}
	if _, err := src.Database(overlapDB).Collection(overlapColl).InsertMany(ctx, seedDocs); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	logDir := t.TempDir()
	confPath := overlapWriteConf(t, srcURL, tgtURL, logDir, "all", overlapParallelCkptColl, map[string]string{
		"full_sync.reader.oplog_store_disk":    "true",
		"full_sync.reader.fetch_batch_size":    "32",
		"full_sync.reader.document_batch_size": "64",
	})

	stopHb := startHeartbeat(t, src.Database(overlapDB).Collection("hb"), runID)
	t.Cleanup(stopHb)

	defer overlapStartProfiling(t, tgt)()

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() {
		proc.kill()
		proc.dumpLog(t)
		if data, err := os.ReadFile(filepath.Join(logDir, "collector-it.log")); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 60 {
				lines = lines[len(lines)-60:]
			}
			t.Logf("collector-it.log tail:\n%s", strings.Join(lines, "\n"))
		}
		if data, err := os.ReadFile(filepath.Join(logDir, consoleLogName)); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 30 {
				lines = lines[len(lines)-30:]
			}
			t.Logf("console tail:\n%s", strings.Join(lines, "\n"))
		}
	})

	// Insert a bounded burst right after the scan starts, then STOP well
	// before the scan ends (the large seed + small batches keep the scan
	// running for several seconds). Every burst doc is carried by the scan
	// AND replayed from the spool — the overlap-window dups asserted here.
	// The burst must end before the scan does: inserts continuing past
	// fullFinishTs would share boundary batches with in-window dups, and the
	// batch-level window check conservatively converts such batches (design
	// decision 2 in #1047), making a zero-update assertion timing-dependent.
	overlapWaitLogMarker(t, logDir, "document syncer-0 do replication", startupTO)
	duringFull := 0
	for duringFull < 100 {
		_, err := src.Database(overlapDB).Collection(overlapColl).InsertOne(ctx,
			bson.D{{Key: "_id", Value: 100000 + duringFull}, {Key: "v", Value: 1}})
		if err != nil {
			t.Fatalf("insert during full: %v", err)
		}
		duringFull++
		time.Sleep(5 * time.Millisecond)
	}
	overlapWaitLogMarker(t, logDir, "full sync done!", syncTO)
	t.Logf("inserted %d documents during the full scan", duringFull)
	fence := packed(latestOplogTs(t, src))

	// catch-up: every source document present on the target, checkpoint past
	// the last write
	deadline := time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		srcN, err1 := src.Database(overlapDB).Collection(overlapColl).CountDocuments(ctx, bson.M{})
		tgtN, err2 := tgt.Database(overlapDB).Collection(overlapColl).CountDocuments(ctx, bson.M{})
		if err1 == nil && err2 == nil && srcN == tgtN {
			break
		}
		time.Sleep(pollPeriod)
	}
	overlapWaitCkptAtLeast(t, src, overlapParallelCkptColl, fence, syncTO)
	time.Sleep(3 * time.Second)

	overlapAssertVersions(t, tgt, int64(overlapParallelSeedDocs+duringFull), 1)
	// Positive control (#1050 review M5): prove overlap duplicates actually
	// happened before asserting zero conversions — otherwise a burst that
	// missed the scan would pass vacuously on develop too. The collector's
	// aggregated dup log records every handled batch on this namespace.
	dupHandled := 0
	deadline = time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filepath.Join(logDir, "collector-it.log"))
		if err == nil {
			dupHandled = 0
			for _, m := range regexp.MustCompile(`dup key handled on ns\[`+overlapDB+`\.`+overlapColl+`\]: docs\[(\d+)\]`).FindAllStringSubmatch(string(data), -1) {
				n, _ := strconv.Atoi(m[1])
				dupHandled += n
			}
			if dupHandled > 0 {
				break
			}
		}
		time.Sleep(pollPeriod)
	}
	if dupHandled == 0 {
		t.Fatalf("no overlap duplicates were handled on %s.%s; the zero-conversion assertion would be vacuous", overlapDB, overlapColl)
	}
	t.Logf("overlap duplicates handled: %d", dupHandled)
	// the backfilled window must turn every overlap dup into a no-op: no
	// conversion updates on the data collection (the workload is insert-only)
	if n := overlapCountProfiledUpdates(t, tgt); n != 0 {
		// diagnose WHICH documents were converted and where their oplog ts
		// sits relative to the persisted window bound
		var entries []bson.M
		cur, err := tgt.Database(overlapDB).Collection("system.profile").Find(ctx,
			bson.M{"ns": overlapDB + "." + overlapColl, "op": "update"},
			options.Find().SetLimit(5))
		if err == nil {
			_ = cur.All(ctx, &entries)
		}
		for _, e := range entries {
			// profiler entries store the operation under "command", not
			// "query"/"update" (#1050 review N6)
			t.Logf("converted update profile entry: command=%v ts=%v", e["command"], e["ts"])
		}
		ck := overlapReadCkptDoc(t, src, overlapParallelCkptColl)
		t.Logf("checkpoint full_finish_ts=%v ckpt=%v", ck["full_finish_ts"], ck["ckpt"])
		t.Fatalf("parallel backfill missing: %d conversion updates for %d overlap docs", n, duringFull)
	}
	// and the window value must be persisted for a later restart
	doc := overlapReadCkptDoc(t, src, overlapParallelCkptColl)
	if v, _ := doc["full_finish_ts"].(int64); v <= 0 {
		t.Fatalf("parallel mode did not persist full_finish_ts: %v", doc)
	}
}

// Leg 3: serialize first run must persist the window bound at the incremental
// handover even when the source goes idle afterwards — the checkpoint never
// advances again, so only the explicit flush at loadCheckpoint writes the
// field. Deleting that flush block makes this test fail after its 30s poll
// (red anchor), not a go-test timeout.
func TestOverlapWindowSerializePersistsBound(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	ckptColl := "ckpt_overlap1047_serialize_it"

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	for _, c := range []struct {
		client *mongo.Client
		db     string
		coll   string
	}{
		{src, overlapDB, overlapColl}, {tgt, overlapDB, overlapColl},
		{src, ckptDB, ckptColl},
	} {
		if err := c.client.Database(c.db).Collection(c.coll).Drop(ctx); err != nil {
			t.Fatalf("drop %s.%s: %v", c.db, c.coll, err)
		}
	}
	// pre-create so no DDL oplog lands inside the window
	for _, client := range []*mongo.Client{src, tgt} {
		if err := client.Database(overlapDB).CreateCollection(ctx, overlapColl); err != nil {
			t.Fatalf("create collection: %v", err)
		}
	}
	seedDocs := make([]interface{}, 0, 100)
	for i := 0; i < 100; i++ {
		seedDocs = append(seedDocs, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: 1}})
	}
	if _, err := src.Database(overlapDB).Collection(overlapColl).InsertMany(ctx, seedDocs); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	logDir := t.TempDir()
	confPath := overlapWriteConf(t, srcURL, tgtURL, logDir, "all", ckptColl, map[string]string{
		"full_sync.reader.oplog_store_disk": "false",
		"full_sync.http_port":               "19151",
		"incr_sync.http_port":               "19150",
		"prom.http_port":                    "19152",
		"system_profile_port":               "19250",
	})
	// NO heartbeat: after the one-shot seed replay the source is idle and the
	// checkpoint timestamp stops moving — the field must still be persisted.

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() {
		proc.kill()
		proc.dumpLog(t)
		if data, err := os.ReadFile(filepath.Join(logDir, "collector-it.log")); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 40 {
				lines = lines[len(lines)-40:]
			}
			t.Logf("collector-it.log tail:\n%s", strings.Join(lines, "\n"))
		}
	})

	overlapWaitLogMarker(t, logDir, "full sync done!", startupTO)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		doc := overlapReadCkptDoc(t, src, ckptColl)
		if v, ok := doc["full_finish_ts"].(int64); ok && v > 0 {
			// persisted while idle: kill hard and re-read from storage
			proc.kill()
			doc = overlapReadCkptDoc(t, src, ckptColl)
			if v2, _ := doc["full_finish_ts"].(int64); v2 != v {
				t.Fatalf("full_finish_ts not durable across kill: %v", doc)
			}
			return
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("serialize first run never persisted full_finish_ts on an idle source")
}
