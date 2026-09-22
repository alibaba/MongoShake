//go:build integration

// End-to-end verification for the incremental (change stream) field whitelist:
//   - whitelisted field update propagates
//   - non-whitelisted field update is a target no-op (field never appears)
//   - sub-path update of a whitelisted object field propagates (firstComponent match)
//   - insert is projected to {_id + whitelisted fields}
//   - delete removes the target document
//
// This runs in incr-only mode (sync_mode=incr), mirroring the sibling
// changestream integration test. Full-sync projection and all-or-nothing index
// filtering are covered by the live-mongo unit tests TestDocumentReaderFieldProjection
// and TestStartIndexSyncFieldWhitelist (collector/docsyncer).
//
// Run with:
//
//	go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m
//
// This file is self-contained (it does not rely on helpers from other files in
// this package). The collector is launched via `go run ./cmd/collector` unless
// MSHAKE_COLLECTOR_BIN points at a prebuilt binary; see ffStartCollector for why.
//
// Environment (overridable):
//
//	MSHAKE_CS_SRC_URL    source mongodb, must be a replica set (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL       target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_COLLECTOR_BIN prebuilt collector binary (default: launch via `go run`)
//
// See docs/superpowers/guides/2026-09-22-field-level-sync.md for the full setup.
package integration

import (
	"context"
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
	ffDB      = "ff_it"
	ffColl    = "c1"
	ffCkptDB  = "mongoshake"
	ffCkpt    = "ckpt_ff_it"
	ffLogFile = "collector-ff.log"
	ffTO      = 120 * time.Second
	ffPoll    = 500 * time.Millisecond
)

func ffEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func ffRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func ffDial(t *testing.T, url string) *mongo.Client {
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

func ffWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(ffRepoRoot(t), "conf", "collector.conf"))
	if err != nil {
		t.Fatalf("read conf template: %v", err)
	}
	confStr := string(tpl)
	set := func(key, value string) {
		re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*$`)
		if !re.MatchString(confStr) {
			confStr += "\n" + key + " = " + value + "\n"
			return
		}
		confStr = re.ReplaceAllString(confStr, key+" = "+value)
	}
	set("id", "ff_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("tunnel", "direct")
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("filter.namespace.white", ffDB+"."+ffColl)
	set("full_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("incr_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("checkpoint.storage.collection", ffCkpt)
	set("checkpoint.interval", "1000")
	// start from now so the retained oplog is not replayed
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19311")
	set("incr_sync.http_port", "19310")
	set("prom.http_port", "19312")
	set("system_profile_port", "19410")
	set("log.dir", logDir)
	set("log.file", ffLogFile)
	// the template on develop carries an uncommented expression value that the
	// config parser rejects (see issue #998); override with a plain number
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-ff.conf")
	if err := os.WriteFile(path, []byte(confStr), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type ffCollectorProc struct {
	cmd        *exec.Cmd
	logPath    string
	cleanupDir string // diagnostic/ dir to remove if this test created it (go run mode)
}

// ffStartCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise the collector is launched via `go run ./cmd/collector`. The go run
// path is a fallback for hosts that SIGKILL freshly-built standalone binaries
// on direct exec (observed under some endpoint-security policies, where even a
// hello-world `go build -o` binary is killed but a `go run` child is allowed).
//
// The process is started in its own process group so kill() reaps the whole
// tree — important because `go run` is a wrapper that spawns the actual
// collector child, which would otherwise be orphaned.
func ffStartCollector(t *testing.T, confPath, logDir string) *ffCollectorProc {
	t.Helper()
	var cmd *exec.Cmd
	var cleanupDir string
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		root := ffRepoRoot(t)
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
	go func() { _ = cmd.Wait() }()
	return &ffCollectorProc{
		cmd:        cmd,
		logPath:    filepath.Join(logDir, ffLogFile),
		cleanupDir: cleanupDir,
	}
}

func (p *ffCollectorProc) kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		// kill the whole process group so the `go run` collector child dies too
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	time.Sleep(300 * time.Millisecond)
	if p.cleanupDir != "" {
		_ = os.RemoveAll(p.cleanupDir)
	}
}

func (p *ffCollectorProc) dumpLog(t *testing.T) {
	data, err := os.ReadFile(p.logPath)
	if err == nil && len(data) > 0 {
		lines := strings.Split(string(data), "\n")
		if len(lines) > 40 {
			lines = lines[len(lines)-40:]
		}
		t.Logf("collector log tail:\n%s", strings.Join(lines, "\n"))
	}
}

func ffWaitDoc(t *testing.T, tgt *mongo.Client, id interface{},
	pred func(bson.M) bool, timeout time.Duration) (bson.M, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bson.M
	for time.Now().Before(deadline) {
		var got bson.M
		err := tgt.Database(ffDB).Collection(ffColl).
			FindOne(context.Background(), bson.M{"_id": id}).Decode(&got)
		if err == nil {
			last = got
			if pred(got) {
				return got, true
			}
		}
		time.Sleep(ffPoll)
	}
	return last, false
}

func TestFieldFilterSync(t *testing.T) {
	srcURL := ffEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := ffEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := ffDial(t, srcURL)
	tgt := ffDial(t, tgtURL)
	ctx := context.Background()

	var hello bson.M
	if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("hello on source: %v", err)
	}
	if hello["setName"] == nil {
		t.Skipf("source %s is not a replica set; change streams unavailable", srcURL)
	}

	srcColl := src.Database(ffDB).Collection(ffColl)
	tgtColl := tgt.Database(ffDB).Collection(ffColl)
	_ = srcColl.Drop(ctx)
	_ = tgtColl.Drop(ctx)
	_ = src.Database(ffCkptDB).Collection(ffCkpt).Drop(ctx)

	// seed source: whitelisted scalar (a), whitelisted object (profile), and a secret
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10, "secret": "x",
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	// incr-only sync (mirrors the sibling changestream integration test): the
	// target is pre-populated with the projected form of the source document.
	// Full-sync projection and index filtering are covered by the live-mongo
	// unit tests TestDocumentReaderFieldProjection / TestStartIndexSyncFieldWhitelist.
	if _, err := tgtColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10,
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	logDir := t.TempDir()
	confPath := ffWriteConf(t, srcURL, tgtURL, logDir)
	proc := ffStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// let the collector establish its change stream before generating events
	time.Sleep(8 * time.Second)

	// 1. incremental: update whitelisted field -> propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"a": 99}}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool { return d["a"] == 99 }, ffTO); !ok {
		t.Fatalf("whitelisted field update did not propagate")
	}

	// 2. incremental: update non-whitelisted field -> target unchanged (still no secret)
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"secret": "y"}}); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	time.Sleep(5 * time.Second)
	var afterSecret bson.M
	if err := tgtColl.FindOne(ctx, bson.M{"_id": 1}).Decode(&afterSecret); err != nil {
		t.Fatalf("read target: %v", err)
	}
	if _, has := afterSecret["secret"]; has {
		t.Fatalf("non-whitelisted field must not appear on target, got %v", afterSecret)
	}

	// 3. incremental: update sub-path of whitelisted object -> propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$set": bson.M{"profile.city": "sh"}}); err != nil {
		t.Fatalf("update profile.city: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool {
		p, ok := d["profile"].(bson.M)
		return ok && p["city"] == "sh"
	}, ffTO); !ok {
		t.Fatalf("sub-path update of whitelisted object did not propagate")
	}

	// 4. incremental: insert new doc -> projected
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 2, "a": 5, "secret": "z"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 2, func(d bson.M) bool {
		_, hasSecret := d["secret"]
		return d["a"] == 5 && !hasSecret
	}, ffTO); !ok {
		t.Fatalf("inserted doc not projected on target")
	}

	// 5. incremental: delete -> removed on target
	if _, err := srcColl.DeleteOne(ctx, bson.M{"_id": 2}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	delDeadline := time.Now().Add(ffTO)
	deleted := false
	for time.Now().Before(delDeadline) {
		err := tgtColl.FindOne(ctx, bson.M{"_id": 2}).Decode(&bson.M{})
		if err == mongo.ErrNoDocuments {
			deleted = true
			break
		}
		time.Sleep(ffPoll)
	}
	if !deleted {
		t.Fatalf("deleted doc still present on target within %v", ffTO)
	}
}
