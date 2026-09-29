//go:build integration

// End-to-end regression test for the oplog fetch path, covering the lazy BSON
// decoding that #1003 turned on by default (incr_sync.lazy_oplog_parse).
//
// One collector run drives eight CRUD shapes plus a DDL inside the sync window:
//
//  1. insert with rich BSON types (Decimal128, DateTime, Binary, Regex,
//     nesting, arrays, explicit null)
//  2. insert of a second document
//  3. v2-diff update ($set on nested and deep paths)
//  4. v1-modifier update ($push, which keeps the classic modifier oplog form)
//  5. $unset (v2 diff carrying a delete entry)
//  6. update of the unique-indexed dotted field, exercising the collision
//     matrix on a dotted path
//  7. full-document replacement (op=u without $ operators)
//  8. upsert of a brand new document, then its delete
//
// plus a createIndexes on a nested field issued after the collector starts, so
// the DDL lands inside the sync window. The assertion is that source and target
// agree on canonical BSON bytes for every document, that the deleted document
// stays deleted, and that the index sets match.
//
// Run with:
//
//	go test -count=1 -tags integration ./integration -run TestLazyOplogEndToEnd -v -timeout 10m
//
// -count=1 is required, not optional: the collector is compiled into a `go run`
// subprocess rather than imported by this package, so Go's test cache does not
// see it change and will replay a stale PASS after the sync code is edited.
//
// Environment (overridable):
//
//	MSHAKE_CS_SRC_URL     source mongodb, must be a replica set (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL        target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_LAZY           value for incr_sync.lazy_oplog_parse; unset keeps the
//	                      server default (true). Set to "false" to cover the
//	                      eager parser as well — both modes are expected to pass.
//	MSHAKE_COLLECTOR_BIN  prebuilt collector binary (default: launch via `go run`).
//	                      Pointing this at a baseline build turns the test into an
//	                      A/B comparison against the branch under review.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	lazyDB      = "lazytest"
	lazyColl    = "crud"
	lazyCkpt    = "ckpt_lazy"
	lazyLogFile = "collector-lazy.log"
	// the oplog cursor must be open before the source is written, and `go run`
	// may have to compile the collector first, so the readiness wait is generous
	lazyReadyTO = 180 * time.Second
	lazySyncTO  = 120 * time.Second
	lazyPoll    = 500 * time.Millisecond
)

func lzEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func lzRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func lzDial(t *testing.T, url string) *mongo.Client {
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

func lzWriteConf(t *testing.T, srcURL, tgtURL, logDir, id string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(lzRepoRoot(t), "conf", "collector.conf"))
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
	set("id", id)
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("mongo_connect_mode", "secondaryPreferred")
	set("tunnel", "direct")
	set("tunnel.address", tgtURL)
	set("tunnel.message", "raw")
	set("incr_sync.mongo_fetch_method", "oplog")
	if v := os.Getenv("MSHAKE_LAZY"); v != "" {
		set("incr_sync.lazy_oplog_parse", v)
	}
	set("incr_sync.worker", "4")
	set("incr_sync.executor.upsert", "true")
	set("incr_sync.executor.insert_on_dup_update", "false")
	set("incr_sync.executor.dup_key_strategy", "ignore")
	set("filter.namespace.white", lazyDB)
	set("filter.ddl_enable", "true")
	set("checkpoint.storage.collection", lazyCkpt)
	set("checkpoint.interval", "1000")
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19311")
	set("incr_sync.http_port", "19310")
	set("prom.http_port", "19312")
	set("system_profile_port", "19410")
	set("log.dir", logDir)
	set("log.file", lazyLogFile)
	set("log.level", "info")
	// the readiness probe reads the log to find out when the collector is up, so
	// it must not sit in the 1s buffer that log.flush=false leaves it in
	set("log.flush", "true")
	// the template carries an uncommented expression value the parser rejects (#998)
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-lazy.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type lzProc struct {
	cmd        *exec.Cmd
	logPath    string
	cleanupDir string // diagnostic/ dir to remove if this test created it (go run mode)
	exited     chan error
}

// lzStartCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise the collector is launched via `go run ./cmd/collector`. The go run
// path is a fallback for hosts that SIGKILL freshly-built standalone binaries on
// direct exec (observed under some endpoint-security policies, where even a
// hello-world `go build -o` product is killed but a `go run` child is allowed).
//
// The process is started in its own process group so kill() reaps the whole tree
// — important because `go run` is a wrapper that spawns the actual collector
// child, which would otherwise be orphaned.
func lzStartCollector(t *testing.T, confPath, logDir string) *lzProc {
	t.Helper()
	var cmd *exec.Cmd
	var cleanupDir string
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		root := lzRepoRoot(t)
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
	p := &lzProc{
		cmd:        cmd,
		logPath:    filepath.Join(logDir, lazyLogFile),
		cleanupDir: cleanupDir,
		exited:     make(chan error, 1),
	}
	// The exit status is kept rather than discarded so a collector that dies
	// early is reported as a dead collector. See requireAlive.
	go func() { p.exited <- cmd.Wait() }()
	return p
}

// requireAlive fails the test if the collector has already exited, dumping its
// log first.
//
// This must be a failure and not a skip: skipping turned "the collector never
// ran" into a permanently green test that verified nothing, which is worse than
// no test at all.
func (p *lzProc) requireAlive(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.exited:
		p.dumpLog(t)
		t.Fatalf("collector exited before the sync completed: %v (hosts that SIGKILL "+
			"freshly-built binaries need the go run path or MSHAKE_COLLECTOR_BIN)", err)
	default:
	}
}

// waitReady blocks until the collector reports its incremental stage is running.
// Writing to the source before that point relies on the oplog still retaining
// the events, which is luck rather than design; a fixed sleep is also either too
// short for a cold `go run` build or wastefully long otherwise.
func (p *lzProc) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(lazyReadyTO)
	for time.Now().Before(deadline) {
		p.requireAlive(t)
		if p.logContains("stage=incr") {
			t.Log("collector incremental stage is running")
			return
		}
		time.Sleep(lazyPoll)
	}
	p.dumpLog(t)
	t.Fatalf("collector did not reach the incremental stage within %v", lazyReadyTO)
}

func (p *lzProc) logContains(substr string) bool {
	data, err := os.ReadFile(p.logPath)
	return err == nil && strings.Contains(string(data), substr)
}

func (p *lzProc) kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		// kill the whole process group so the `go run` collector child dies too
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	time.Sleep(300 * time.Millisecond)
	if p.cleanupDir != "" {
		_ = os.RemoveAll(p.cleanupDir)
	}
}

func (p *lzProc) dumpLog(t *testing.T) {
	data, err := os.ReadFile(p.logPath)
	if err == nil && len(data) > 0 {
		lines := strings.Split(string(data), "\n")
		if len(lines) > 60 {
			lines = lines[len(lines)-60:]
		}
		t.Logf("collector log tail:\n%s", strings.Join(lines, "\n"))
	}
}

func TestLazyOplogEndToEnd(t *testing.T) {
	srcURL := lzEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := lzEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := lzDial(t, srcURL)
	defer src.Disconnect(context.Background())
	tgt := lzDial(t, tgtURL)
	defer tgt.Disconnect(context.Background())

	buildInfo := struct {
		Version string `bson:"version"`
	}{}
	if err := src.Database("admin").RunCommand(context.Background(),
		bson.D{{"buildInfo", 1}}).Decode(&buildInfo); err != nil {
		t.Skipf("cannot read buildInfo: %v", err)
	}
	var hello bson.M
	if err := src.Database("admin").RunCommand(context.Background(),
		bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("hello on source: %v", err)
	}
	if hello["setName"] == nil {
		t.Skipf("source %s is not a replica set; there is no oplog to tail", srcURL)
	}
	lazy := os.Getenv("MSHAKE_LAZY")
	if lazy == "" {
		lazy = "(default)"
	}
	t.Logf("source MongoDB %s, incr_sync.lazy_oplog_parse=%s", buildInfo.Version, lazy)

	scoll := src.Database(lazyDB).Collection(lazyColl)
	tcoll := tgt.Database(lazyDB).Collection(lazyColl)
	ctx := context.Background()
	_ = src.Database(lazyDB).Drop(ctx)
	_ = tgt.Database(lazyDB).Drop(ctx)

	logDir := t.TempDir()
	confPath := lzWriteConf(t, srcURL, tgtURL, logDir,
		fmt.Sprintf("lazy_it_%d", time.Now().UnixNano()))
	proc := lzStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })
	proc.waitReady(t)

	// unique index on a nested field: created after the collector starts so the
	// createIndexes DDL is inside the sync window. It also exercises the
	// collision matrix / ParsedLog.IndexValue path for dotted keys.
	if _, err := scoll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{"profile.email", 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()

	// 1. insert with rich BSON types, nesting and an array
	doc1 := bson.D{
		{"_id", oid1},
		{"profile", bson.D{
			{"email", "a@example.com"},
			{"name", "alice"},
			{"age", int32(30)},
		}},
		{"tags", bson.A{"x", "y", "z"}},
		{"amount", primitive.NewDecimal128(1, 2)},
		{"when", primitive.DateTime(time.Now().UnixMilli())},
		{"blob", primitive.Binary{Subtype: 0, Data: []byte{1, 2, 3, 255}}},
		{"rx", primitive.Regex{Pattern: "^a", Options: "i"}},
		{"nothing", nil},
		{"deep", bson.D{{"l1", bson.D{{"l2", bson.D{{"l3", int64(42)}}}}}}},
	}
	if _, err := scoll.InsertOne(ctx, doc1); err != nil {
		t.Fatalf("insert doc1: %v", err)
	}
	if _, err := scoll.InsertOne(ctx, bson.D{
		{"_id", oid2}, {"profile", bson.D{{"email", "b@example.com"}}}, {"n", int32(1)},
	}); err != nil {
		t.Fatalf("insert doc2: %v", err)
	}

	// 2. v2 diff update ($set on a nested field)
	if _, err := scoll.UpdateOne(ctx, bson.M{"_id": oid1},
		bson.M{"$set": bson.M{"profile.age": int32(31), "deep.l1.l2.l3": int64(43)}}); err != nil {
		t.Fatalf("v2 diff update: %v", err)
	}

	// 3. v1 modifier update ($push keeps the classic modifier oplog form)
	if _, err := scoll.UpdateOne(ctx, bson.M{"_id": oid1},
		bson.M{"$push": bson.M{"tags": "w"}}); err != nil {
		t.Fatalf("push update: %v", err)
	}

	// 4. $unset (v2 diff with a delete entry)
	if _, err := scoll.UpdateOne(ctx, bson.M{"_id": oid2},
		bson.M{"$unset": bson.M{"n": ""}}); err != nil {
		t.Fatalf("unset update: %v", err)
	}

	// 5. update the unique-indexed nested field -> collision matrix on a dotted path
	if _, err := scoll.UpdateOne(ctx, bson.M{"_id": oid1},
		bson.M{"$set": bson.M{"profile.email": "a2@example.com"}}); err != nil {
		t.Fatalf("unique index update: %v", err)
	}

	// 6. full-document replacement update (op=u without $ operators)
	if _, err := scoll.ReplaceOne(ctx, bson.M{"_id": oid2}, bson.D{
		{"_id", oid2}, {"profile", bson.D{{"email", "b2@example.com"}}}, {"replaced", true},
	}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	// 7. upsert of a brand new document
	if _, err := scoll.UpdateOne(ctx, bson.M{"_id": oid3},
		bson.M{"$set": bson.M{"profile": bson.D{{"email", "c@example.com"}}}},
		options.Update().SetUpsert(true)); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// 8. delete
	if _, err := scoll.DeleteOne(ctx, bson.M{"_id": oid3}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// wait for the target to converge, then diff both sides
	deadline := time.Now().Add(lazySyncTO)
	lastLog := time.Time{}
	for {
		proc.requireAlive(t)
		if time.Now().After(deadline) {
			proc.dumpLog(t)
			t.Fatalf("target did not converge within %v", lazySyncTO)
		}
		diff, err := lzDiff(ctx, scoll, tcoll)
		if err != nil {
			t.Fatalf("compare source and target: %v", err)
		}
		if len(diff) == 0 {
			t.Logf("source and target converged")
			break
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			t.Logf("pending diff: %v", diff)
		}
		time.Sleep(lazyPoll)
	}

	// An empty diff also comes out of two empty collections, so pin down what the
	// target is actually expected to hold: oid1 and oid2, and not the deleted oid3.
	if n, err := tcoll.CountDocuments(ctx, bson.M{}); err != nil {
		t.Fatalf("count target: %v", err)
	} else if n != 2 {
		t.Errorf("expected exactly 2 documents on the target, got %d", n)
	}
	for _, id := range []primitive.ObjectID{oid1, oid2} {
		if n, err := tcoll.CountDocuments(ctx, bson.M{"_id": id}); err != nil {
			t.Fatalf("count _id=%v: %v", id, err)
		} else if n != 1 {
			t.Errorf("document _id=%v missing on the target", id)
		}
	}
	if n, err := tcoll.CountDocuments(ctx, bson.M{"_id": oid3}); err != nil {
		t.Fatalf("count deleted: %v", err)
	} else if n != 0 {
		t.Errorf("deleted document reappeared on target")
	}

	srcIdx := lzIndexNames(ctx, t, scoll)
	tgtIdx := lzIndexNames(ctx, t, tcoll)
	if len(srcIdx) == 0 {
		t.Error("source reported no indexes; the createIndexes DDL did not take effect")
	}
	if strings.Join(srcIdx, ",") != strings.Join(tgtIdx, ",") {
		t.Errorf("index mismatch: src=%v tgt=%v", srcIdx, tgtIdx)
	}
}

func lzIndexNames(ctx context.Context, t *testing.T, coll *mongo.Collection) []string {
	t.Helper()
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer cur.Close(ctx)
	var names []string
	for cur.Next(ctx) {
		var raw bson.M
		if err := cur.Decode(&raw); err != nil {
			t.Fatalf("decode index: %v", err)
		}
		if n, ok := raw["name"].(string); ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names
}

// lzDiff compares every document by canonical BSON bytes and returns a list of
// human readable mismatches. A failed query is an error rather than an empty
// snapshot: swallowing it would let two failed reads compare equal and report
// the source and target as converged.
func lzDiff(ctx context.Context, scoll, tcoll *mongo.Collection) ([]string, error) {
	snap := func(coll *mongo.Collection) (map[string]string, error) {
		out := map[string]string{}
		cur, err := coll.Find(ctx, bson.M{})
		if err != nil {
			return nil, err
		}
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var d bson.D
			if err := cur.Decode(&d); err != nil {
				return nil, err
			}
			key := ""
			for _, e := range d {
				if e.Key == "_id" {
					key = fmt.Sprintf("%v", e.Value)
				}
			}
			b, err := bson.Marshal(d)
			if err != nil {
				return nil, err
			}
			out[key] = fmt.Sprintf("%x", b)
		}
		if err := cur.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}

	s, err := snap(scoll)
	if err != nil {
		return nil, fmt.Errorf("read source: %w", err)
	}
	tg, err := snap(tcoll)
	if err != nil {
		return nil, fmt.Errorf("read target: %w", err)
	}

	var diffs []string
	for k, v := range s {
		got, ok := tg[k]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("missing on target: %s", k))
			continue
		}
		if got != v {
			diffs = append(diffs, fmt.Sprintf("content mismatch _id=%s\n  src=%s\n  tgt=%s", k, v, got))
		}
	}
	for k := range tg {
		if _, ok := s[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("unexpected on target: %s", k))
		}
	}
	sort.Strings(diffs)
	return diffs, nil
}
