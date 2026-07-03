//go:build integration

// End-to-end verification for issue #986: change stream update events carrying
// updateDescription.truncatedArrays must not wipe the target document, and must
// not stall the pipeline when they cannot be replayed faithfully.
//
// Two scenarios share one collector run:
//
//	truncate_only                  — the exact shape reported in the issue
//	modify_and_truncate_same_array — the conflict case, see below
//
// Scenario 1:
//
//  1. same document on source and target: {_id:1, arr:["a","b","c"], keep:"value"}
//  2. an aggregation-pipeline update truncates the array on the source:
//     updateOne({_id:1}, [{$set: {arr: {$slice: ["$arr", 2]}}}])
//  3. MongoDB emits an update event with empty updatedFields/removedFields and
//     truncatedArrays: [{field:"arr", newSize:2}]
//  4. the target must become {_id:1, arr:["a","b"], keep:"value"}
//
// Before the fix the generated replay object was empty, which the executor
// treated as a full-document replacement, leaving only {_id:1} on the target.
//
// Scenario 2: one event can both modify and truncate the same array. The $v:2
// delta then carries u<N> and l side by side — verified on MongoDB 7.0.37, a
// 200-element array updated with
//
//	[{$set:{arr:{$concatArrays:[["X"],{$slice:["$arr",1,99]}]}}}]
//
// produces {"$v":2,"diff":{"sarr":{"a":true,"l":100,"u0":"X"}}}, surfaced as
// updatedFields {"arr.0":"X"} plus truncatedArrays {field:"arr", newSize:100}.
// Replaying that as $set + $push makes the server reject the write with
// "Updating the path 'arr' would create a conflict at 'arr'", which the executor
// retries forever — every namespace queued behind it stops syncing. The
// conversion therefore drops the conflicting truncation with a warning. This
// asserts the $set still lands, the warning is emitted, no conflicting write
// reaches the server, and an unrelated update afterwards still converges.
//
// NOTE on array sizes: MongoDB's diff heuristic rewrites tiny changes into
// whole-field replacements instead of truncatedArrays. Verified on 7.0: 2->1
// yields updatedFields only, while 3->2, 5->3 and 12->10 yield truncatedArrays.
// Scenario 1 therefore uses a 3-element array and scenario 2 a 200-element one,
// and both assert the captured event really has the shape they need, so neither
// can silently degrade into a vacuous pass.
//
// Run with:
//
//	go test -count=1 -tags integration ./integration -run TestChangeStreamTruncatedArrays -v -timeout 10m
//
// -count=1 is required, not optional: the code under test is compiled into a
// `go run` subprocess rather than imported by this package, so Go's test cache
// does not see it change and will happily replay a stale PASS after the
// conversion logic is edited.
//
// Environment (overridable):
//
//	MSHAKE_CS_SRC_URL     source mongodb, must be a replica set (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL        target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_COLLECTOR_BIN  prebuilt collector binary (default: launch via `go run`)
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	csTestDB   = "cs986test"
	csTestColl = "items"
	csCkptDB   = "mongoshake"
	csCkptColl = "ckpt_cs986_it"
	csLogFile  = "collector-cs986.log"
	// the change stream must be open before the source is modified, and `go run`
	// may have to compile the collector first, so both waits are generous
	csReadyTO = 180 * time.Second
	csSyncTO  = 90 * time.Second
	csPoll    = 500 * time.Millisecond
	// large enough that MongoDB keeps the $v:2 delta instead of rewriting the
	// change into a whole-array replacement
	csBigArray = 200
)

func csEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func csRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func csDial(t *testing.T, url string) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url).SetDirect(true))
	if err != nil {
		t.Skipf("integration env not ready: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("integration env not ready: %v", err)
	}
	return client
}

func csWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
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
	set("id", "cs986_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone") // direct connection to a mapped port
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("tunnel", "direct")
	set("checkpoint.storage.collection", csCkptColl)
	set("checkpoint.interval", "1000")
	// start from now so the retained oplog is not replayed
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19301")
	set("incr_sync.http_port", "19300")
	set("prom.http_port", "19302")
	set("system_profile_port", "19400")
	set("log.dir", logDir)
	set("log.file", csLogFile)
	// this test asserts on log content, and the default log.flush=false buffers
	// writes for up to a second, which would make those assertions racy
	set("log.flush", "true")
	// the template on develop carries an uncommented expression value that the
	// config parser rejects (see issue #998); override with a plain number
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-cs986.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type csCollectorProc struct {
	cmd        *exec.Cmd
	logPath    string
	cleanupDir string // diagnostic/ dir to remove if this test created it (go run mode)
	exited     chan error
}

// csStartCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise the collector is launched via `go run ./cmd/collector`. The go run
// path is a fallback for hosts that SIGKILL freshly-built standalone binaries on
// direct exec (observed under some endpoint-security policies, where even a
// hello-world `go build -o` product is killed but a `go run` child is allowed).
// Without it the collector dies instantly and the test reports a bogus "target
// did not converge" only after the full sync timeout.
//
// The process is started in its own process group so kill() reaps the whole tree
// — important because `go run` is a wrapper that spawns the actual collector
// child, which would otherwise be orphaned.
func csStartCollector(t *testing.T, confPath, logDir string) *csCollectorProc {
	t.Helper()
	var cmd *exec.Cmd
	var cleanupDir string
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		root := csRepoRoot(t)
		cmd = exec.Command("go", "run", "./cmd/collector", "-conf="+confPath)
		cmd.Dir = root
		// `go run` runs the collector with CWD=repoRoot, where it creates a
		// diagnostic/ journal dir; remove it afterwards if we created it.
		diag := filepath.Join(root, "diagnostic")
		if _, err := os.Stat(diag); os.IsNotExist(err) {
			cleanupDir = diag
		}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	p := &csCollectorProc{
		cmd:        cmd,
		logPath:    filepath.Join(logDir, csLogFile),
		cleanupDir: cleanupDir,
		exited:     make(chan error, 1),
	}
	// The exit status is kept rather than discarded so a collector that dies
	// early is reported as a dead collector, not as a sync that failed to
	// converge. See requireAlive.
	go func() { p.exited <- cmd.Wait() }()
	return p
}

// requireAlive fails the test if the collector has already exited, dumping its
// log first. Call this before concluding that the target merely lagged behind.
func (p *csCollectorProc) requireAlive(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.exited:
		p.dumpLog(t)
		t.Fatalf("collector exited before the sync completed: %v (hosts that SIGKILL "+
			"freshly-built binaries need the go run path or MSHAKE_COLLECTOR_BIN)", err)
	default:
	}
}

// waitReady blocks until the collector logs that its change stream is open.
// Generating the source event before that point would lose it, and a fixed sleep
// is either too short on a cold `go run` build or wastefully long otherwise.
func (p *csCollectorProc) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(csReadyTO)
	for time.Now().Before(deadline) {
		p.requireAlive(t)
		if p.logContains("new change stream with options") {
			t.Log("collector change stream is open")
			return
		}
		time.Sleep(csPoll)
	}
	p.dumpLog(t)
	t.Fatalf("collector did not open its change stream within %v", csReadyTO)
}

func (p *csCollectorProc) logContains(substr string) bool {
	data, err := os.ReadFile(p.logPath)
	return err == nil && strings.Contains(string(data), substr)
}

func (p *csCollectorProc) kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		// kill the whole process group so the `go run` collector child dies too
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	time.Sleep(300 * time.Millisecond)
	if p.cleanupDir != "" {
		_ = os.RemoveAll(p.cleanupDir)
	}
}

func (p *csCollectorProc) dumpLog(t *testing.T) {
	data, err := os.ReadFile(p.logPath)
	if err == nil && len(data) > 0 {
		lines := strings.Split(string(data), "\n")
		if len(lines) > 40 {
			lines = lines[len(lines)-40:]
		}
		t.Logf("collector log tail:\n%s", strings.Join(lines, "\n"))
	}
}

// csWaitDoc waits until the target document satisfies pred, returning the last
// document seen so the caller can report the actual mismatch.
func csWaitDoc(t *testing.T, coll *mongo.Collection, id interface{},
	pred func(bson.M) bool, timeout time.Duration) (bson.M, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bson.M
	for time.Now().Before(deadline) {
		var got bson.M
		err := coll.FindOne(context.Background(), bson.M{"_id": id}).Decode(&got)
		if err == nil {
			last = got
			if pred(got) {
				return got, true
			}
		}
		time.Sleep(csPoll)
	}
	return last, false
}

// captureUpdateDescription drains the change stream until the next update event
// and returns its updateDescription. Non-update events (the insert that seeds
// scenario 2, for instance) are skipped.
func captureUpdateDescription(t *testing.T, ctx context.Context, cs *mongo.ChangeStream) bson.M {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if !cs.TryNext(ctx) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var ev bson.M
		if err := cs.Decode(&ev); err != nil {
			t.Fatalf("decode change stream event: %v", err)
		}
		if ev["operationType"] != "update" {
			continue
		}
		ud, _ := ev["updateDescription"].(bson.M)
		return ud
	}
	t.Fatal("no update event observed on the source change stream")
	return nil
}

// requireTruncatedArrays skips the test when this MongoDB version reports the
// change as a whole-array replacement instead of truncatedArrays. Without it the
// test could pass while never exercising the #986 code path.
func requireTruncatedArrays(t *testing.T, ud bson.M) bson.A {
	t.Helper()
	truncated, _ := ud["truncatedArrays"].(bson.A)
	if len(truncated) == 0 {
		t.Skipf("this MongoDB emitted updatedFields=%v instead of truncatedArrays; "+
			"the #986 code path is not exercised on this server version", ud["updatedFields"])
	}
	return truncated
}

func TestChangeStreamTruncatedArrays(t *testing.T) {
	srcURL := csEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := csEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := csDial(t, srcURL)
	tgt := csDial(t, tgtURL)
	ctx := context.Background()

	// change streams require a replica set on the source
	var hello bson.M
	if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("hello on source: %v", err)
	}
	if hello["setName"] == nil {
		t.Skipf("source %s is not a replica set; change streams unavailable", srcURL)
	}

	srcColl := src.Database(csTestDB).Collection(csTestColl)
	tgtColl := tgt.Database(csTestDB).Collection(csTestColl)

	// clean slate, then seed the SAME document on both sides (incr-only sync)
	_ = srcColl.Drop(ctx)
	_ = tgtColl.Drop(ctx)
	_ = src.Database(csCkptDB).Collection(csCkptColl).Drop(ctx)

	seed := bson.M{"_id": 1, "arr": bson.A{"a", "b", "c"}, "keep": "value"}
	if _, err := srcColl.InsertOne(ctx, seed); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := tgtColl.InsertOne(ctx, seed); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	logDir := t.TempDir()
	confPath := csWriteConf(t, srcURL, tgtURL, logDir)
	proc := csStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })
	proc.waitReady(t)

	// watch the source ourselves so each scenario can prove the event it captured
	// really has the shape under test
	verifyCS, err := srcColl.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		t.Fatalf("open verification change stream: %v", err)
	}
	defer verifyCS.Close(ctx)

	// The reported reproduction: an aggregation-pipeline update that only
	// truncates the array -> updatedFields/removedFields empty, truncatedArrays set.
	t.Run("truncate_only", func(t *testing.T) {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, mongo.Pipeline{
			{{Key: "$set", Value: bson.M{"arr": bson.M{"$slice": bson.A{"$arr", 2}}}}},
		}); err != nil {
			t.Fatalf("pipeline truncate update: %v", err)
		}

		ud := captureUpdateDescription(t, ctx, verifyCS)
		requireTruncatedArrays(t, ud)
		if updated, _ := ud["updatedFields"].(bson.M); len(updated) != 0 {
			t.Skipf("expected a truncate-only event but got updatedFields=%v; "+
				"this MongoDB chose a different diff representation", updated)
		}

		// sanity: the source really is {_id:1, arr:["a","b"], keep:"value"}
		var srcGot bson.M
		if err := srcColl.FindOne(ctx, bson.M{"_id": 1}).Decode(&srcGot); err != nil {
			t.Fatalf("read source: %v", err)
		}
		if srcArr, ok := srcGot["arr"].(bson.A); !ok || len(srcArr) != 2 {
			t.Fatalf("unexpected source state after truncate: %v", srcGot)
		}

		// the target must converge to the same shape: array truncated AND the
		// unrelated "keep" field preserved
		got, ok := csWaitDoc(t, tgtColl, 1, func(d bson.M) bool {
			arr, isArr := d["arr"].(bson.A)
			return isArr && len(arr) == 2 && d["keep"] == "value"
		}, csSyncTO)
		proc.requireAlive(t)
		if !ok {
			if got == nil {
				t.Fatalf("target document _id=1 disappeared entirely within %v", csSyncTO)
			}
			if _, hasKeep := got["keep"]; !hasKeep {
				t.Fatalf("issue #986 reproduced: target lost unrelated fields, got %v "+
					`(expected {_id:1, arr:["a","b"], keep:"value"})`, got)
			}
			t.Fatalf("target did not converge within %v, got %v", csSyncTO, got)
		}

		arr := got["arr"].(bson.A)
		if arr[0] != "a" || arr[1] != "b" {
			t.Fatalf("target array truncated to the wrong elements: %v", got)
		}
		t.Logf("target converged correctly: %v", got)
	})

	// A single event that both modifies and truncates the same array cannot be
	// replayed: $set on "arr.0" and $push on "arr" address overlapping paths,
	// which the server rejects and the executor would retry forever. The
	// conversion degrades it instead, and this asserts the degradation holds.
	t.Run("modify_and_truncate_same_array", func(t *testing.T) {
		big := make(bson.A, 0, csBigArray)
		for i := 0; i < csBigArray; i++ {
			big = append(big, fmt.Sprintf("e%d", i))
		}
		if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 2, "arr": big, "keep": "v2"}); err != nil {
			t.Fatalf("seed source _id=2: %v", err)
		}
		// let the insert land first so the update replays against a document that
		// exists on the target
		if _, ok := csWaitDoc(t, tgtColl, 2, func(d bson.M) bool {
			arr, isArr := d["arr"].(bson.A)
			return isArr && len(arr) == csBigArray
		}, csSyncTO); !ok {
			proc.requireAlive(t)
			t.Fatal("the _id=2 insert never reached the target; cannot exercise the conflict path")
		}

		// replace element 0 AND truncate 200 -> 100 in one pipeline update, which
		// yields the $v:2 delta {"sarr":{"a":true,"l":100,"u0":"X"}}
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 2}, mongo.Pipeline{
			{{Key: "$set", Value: bson.M{"arr": bson.M{
				"$concatArrays": bson.A{
					bson.A{"X"},
					bson.M{"$slice": bson.A{"$arr", 1, csBigArray/2 - 1}},
				},
			}}}},
		}); err != nil {
			t.Fatalf("mixed update: %v", err)
		}

		ud := captureUpdateDescription(t, ctx, verifyCS)
		truncated := requireTruncatedArrays(t, ud)
		updated, _ := ud["updatedFields"].(bson.M)
		if len(updated) == 0 {
			t.Skipf("expected updatedFields alongside truncatedArrays=%v, but this "+
				"MongoDB reported the change as a whole-array replacement", truncated)
		}
		t.Logf("source event: updatedFields=%v truncatedArrays=%v", updated, truncated)

		// the $set part must land; the truncation is dropped by design
		got, ok := csWaitDoc(t, tgtColl, 2, func(d bson.M) bool {
			arr, isArr := d["arr"].(bson.A)
			return isArr && len(arr) > 0 && arr[0] == "X"
		}, csSyncTO)
		proc.requireAlive(t)
		if !ok {
			arr, _ := got["arr"].(bson.A)
			t.Fatalf("the $set part of the conflicting event never reached the target within %v: "+
				"target still has arr len=%d keep=%v — the write was likely rejected and is being retried",
				csSyncTO, len(arr), got["keep"])
		}
		if arr, _ := got["arr"].(bson.A); len(arr) != csBigArray {
			t.Fatalf("expected the conflicting truncation to be dropped (target keeps %d elements), got %d",
				csBigArray, len(arr))
		}
		if !proc.logContains("drops truncatedArrays") {
			t.Fatal("the truncation was dropped without the warning that documents the divergence")
		}
		for _, bad := range []string{"would create a conflict", "BulkWriteException"} {
			if proc.logContains(bad) {
				t.Fatalf("the conflicting event reached the server instead of being degraded; log contains %q", bad)
			}
		}
		t.Logf("conflict degraded correctly: target arr[0]=X len=%d", csBigArray)

		// proof the pipeline is not wedged behind the degraded event
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
			bson.M{"$set": bson.M{"keep": "after_conflict"}}); err != nil {
			t.Fatalf("post-conflict update: %v", err)
		}
		if _, ok := csWaitDoc(t, tgtColl, 1, func(d bson.M) bool {
			return d["keep"] == "after_conflict"
		}, csSyncTO); !ok {
			proc.requireAlive(t)
			t.Fatal("sync stalled behind the degraded event: an unrelated update never reached the target")
		}
		t.Log("sync continued past the degraded event")
	})
}
