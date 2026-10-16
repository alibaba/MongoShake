//go:build integration

// End-to-end verification for issue #995: full sync must recreate a capped
// collection on the target as a capped collection.
//
// Before the fix nothing looked at the source's creation options for an
// ordinary-typed collection, so the first inserted document implicitly created a
// normal collection on the target. The consequences are silent and permanent:
//
//   - the target grows without bound where the source wraps around, so a
//     log/queue-style capped collection eventually fills the target's disk;
//   - the two sides report different counts, which is what #995 was filed as;
//   - capped-ness cannot be added afterwards — collMod's convertToCapped was
//     removed from the server (verified on 7.0.37: "BSON field
//     'collMod.convertToCapped' is an unknown field"), so the only way back is to
//     drop and resync.
//
// Test 1 drives a real `sync_mode = all` run over a source holding two capped
// collections (one with max, one size-only) plus an ordinary one, then asserts
// that:
//
//  1. both capped collections are capped on the target;
//  2. their creation options match the source exactly, size and max included;
//  3. every document arrived — the fixtures stay well under size and max, so
//     nothing may legitimately wrap away;
//  4. the ordinary collection is still ordinary, which guards against applying
//     capped-ness to collections that do not have it;
//  5. the index sets match;
//  6. the log carries a positive anchor that the create step really ran, and no
//     write error or unreported mismatch.
//
// Test 2 covers the recovery path a user upgrading from a pre-fix build actually
// hits: the target already holds an ordinary collection of the same name and
// full_sync.collection_exist_drop is left at its default false. Dropping data is
// not ours to decide, so the run must report the situation loudly and still
// complete rather than fail or silently pretend it worked.
//
// Run with:
//
//	go test -count=1 -tags integration ./integration -run TestCappedFullSync -v -timeout 10m
//
// -count=1 is required, not optional: the collector is compiled into a `go run`
// subprocess rather than imported by this package, so Go's test cache does not
// see it change and will replay a stale PASS after the sync code is edited.
//
// Environment (overridable):
//
//	MSHAKE_CS_SRC_URL     source mongodb (default mongodb://127.0.0.1:27030)
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
	capDB = "capfullsync"
	// capped with both size and max, so the max option is covered too
	capCollMax  = "capped_max"
	capCollSize = "capped_size"
	capPlain    = "plain"
	capCkpt     = "ckpt_cap_it"
	capLogFile  = "collector-cap.log"

	capReadyTO = 180 * time.Second
	// stays far below size and max so no document may legitimately wrap away
	capDocN   = 50
	capSizeKB = 1024
	capMaxN   = 1000
)

func capEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func capRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func capDial(t *testing.T, url string) *mongo.Client {
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

// capWriteConf renders conf/collector.conf for one collector run.
//
// collectionDrop maps to full_sync.collection_exist_drop; test 2 needs the
// default false so that a pre-existing ordinary collection on the target
// survives into the create step and exercises the "cannot be capped in place"
// branch.
func capWriteConf(t *testing.T, id, srcURL, tgtURL, logDir string, collectionDrop bool,
	httpPort int) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(capRepoRoot(t), "conf", "collector.conf"))
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
	set("sync_mode", "full")
	set("mongo_urls", srcURL)
	set("mongo_connect_mode", "secondaryPreferred")
	set("tunnel", "direct")
	set("tunnel.address", tgtURL)
	set("tunnel.message", "raw")
	set("filter.namespace.white", capDB)
	set("full_sync.collection_exist_drop", fmt.Sprintf("%v", collectionDrop))
	set("full_sync.create_index", "background")
	set("full_sync.reader.parallel_thread", "4")
	set("full_sync.executor.insert_on_dup_update", "false")
	set("checkpoint.storage.collection", capCkpt)
	set("checkpoint.interval", "1000")
	set("full_sync.http_port", fmt.Sprintf("%d", httpPort))
	set("incr_sync.http_port", fmt.Sprintf("%d", httpPort+1))
	set("prom.http_port", fmt.Sprintf("%d", httpPort+2))
	set("system_profile_port", fmt.Sprintf("%d", httpPort+10))
	set("log.dir", logDir)
	set("log.file", capLogFile)
	set("log.level", "info")
	// the assertions below read the log, which log.flush=false leaves buffered
	set("log.flush", "true")
	// pinned; the template's value was an unevaluated expression before #998
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-cap.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type capProc struct {
	cmd         *exec.Cmd
	logPath     string
	consolePath string
	console     *os.File
	cleanupDir  string // diagnostic/ dir to remove if this test created it
	exited      chan error
}

// capStartCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise the collector is launched via `go run ./cmd/collector`: some
// endpoint-security policies SIGKILL freshly-built standalone binaries on direct
// exec, where a `go run` child is allowed. The process runs in its own process
// group so kill() reaps the `go run` wrapper's collector child too.
func capStartCollector(t *testing.T, confPath, logDir string) *capProc {
	t.Helper()
	var cmd *exec.Cmd
	var cleanupDir string
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		root := capRepoRoot(t)
		cmd = exec.Command("go", "run", "./cmd/collector", "-conf="+confPath)
		cmd.Dir = root
		diag := filepath.Join(root, "diagnostic")
		if _, err := os.Stat(diag); os.IsNotExist(err) {
			cleanupDir = diag
		}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// A collector that dies during config parsing never opens its log file, so
	// capture the child's console output as well or the failure is undiagnosable.
	consoleFile := "collector-cap-console.log"
	console, err := os.Create(filepath.Join(logDir, consoleFile))
	if err != nil {
		t.Fatalf("create console capture: %v", err)
	}
	cmd.Stdout = console
	cmd.Stderr = console
	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	p := &capProc{
		cmd:         cmd,
		logPath:     filepath.Join(logDir, capLogFile),
		consolePath: console.Name(),
		console:     console,
		cleanupDir:  cleanupDir,
		// A collector that dies must be reported as dead. Skipping instead would
		// turn that into a permanently green test that verified nothing.
		exited: make(chan error, 1),
	}
	go func() { p.exited <- cmd.Wait() }()
	return p
}

func (p *capProc) logContains(substr string) bool {
	data, err := os.ReadFile(p.logPath)
	return err == nil && strings.Contains(string(data), substr)
}

// logHasLine reports whether one single log line contains every substring. Used
// for positive anchors: logContains alone cannot tie an event to a collection,
// because the same names show up all over an unrelated line.
func (p *capProc) logHasLine(substrs ...string) bool {
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		all := true
		for _, s := range substrs {
			if !strings.Contains(line, s) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// waitExit blocks until a `sync_mode = full` collector has finished and exited on
// its own. Exit is the completion signal: startDocumentReplication waits on every
// dbSyncer (full.go's wg.Wait) before returning, and coordinator.Start then runs
// out of cases to handle. Polling the log for "full sync done!" instead would not
// work — that line is only logged by the mixed full+incr modes, not by
// sync_mode = full.
func (p *capProc) waitExit(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.exited:
		if err != nil {
			p.dumpLog(t)
			t.Fatalf("collector exited with an error: %v", err)
		}
	case <-time.After(capReadyTO):
		p.dumpLog(t)
		t.Fatalf("collector did not exit within %v", capReadyTO)
	}
}

func (p *capProc) kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	time.Sleep(300 * time.Millisecond)
	if p.console != nil {
		_ = p.console.Close()
	}
	if p.cleanupDir != "" {
		_ = os.RemoveAll(p.cleanupDir)
	}
}

func (p *capProc) dumpLog(t *testing.T) {
	for _, path := range []string{p.logPath, p.consolePath} {
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}
		lines := strings.Split(string(data), "\n")
		if len(lines) > 50 {
			lines = lines[len(lines)-50:]
		}
		t.Logf("collector output %s tail:\n%s", filepath.Base(path), strings.Join(lines, "\n"))
	}
}

// capCollectionInfo is the listCollections entry for one collection.
type capCollectionInfo struct {
	Name    string `bson:"name"`
	Type    string `bson:"type"`
	Options bson.M `bson:"options"`
}

func capListInfo(t *testing.T, client *mongo.Client, name string) *capCollectionInfo {
	t.Helper()
	cur, err := client.Database(capDB).ListCollections(context.Background(), bson.M{"name": name})
	if err != nil {
		t.Fatalf("listCollections(%s): %v", name, err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var info capCollectionInfo
		if err := bson.Unmarshal(cur.Current, &info); err != nil {
			t.Fatalf("decode collection info of %s: %v", name, err)
		}
		return &info
	}
	return nil
}

func capIndexNames(t *testing.T, client *mongo.Client, coll string) []string {
	t.Helper()
	cur, err := client.Database(capDB).Collection(coll).Indexes().List(context.Background())
	if err != nil {
		t.Fatalf("list indexes of %s: %v", coll, err)
	}
	defer cur.Close(context.Background())
	var names []string
	for cur.Next(context.Background()) {
		var raw bson.M
		if err := cur.Decode(&raw); err != nil {
			t.Fatalf("decode index of %s: %v", coll, err)
		}
		if n, ok := raw["name"].(string); ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names
}

// capCanonical renders a value as a stable string with every document's keys
// sorted. bson.Marshal of a bson.M follows Go's randomized map iteration order,
// so byte comparison of two option maps that print identically is not usable.
func capCanonical(v interface{}) string {
	switch t := v.(type) {
	case bson.M:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+"="+capCanonical(t[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case bson.A:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, capCanonical(e))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case primitive.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case primitive.ObjectID:
		return t.Hex()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// capBuildSource lays down the fixture: two capped collections and one ordinary
// one, each holding the same documents so the counts are directly comparable.
func capBuildSource(t *testing.T, src *mongo.Client) {
	t.Helper()
	ctx := context.Background()
	db := src.Database(capDB)

	// size is given in bytes; capSizeKB is deliberately generous so that capDocN
	// small documents cannot fill it and wrap
	if err := db.CreateCollection(ctx, capCollMax,
		options.CreateCollection().SetCapped(true).
			SetSizeInBytes(int64(capSizeKB)*1024).SetMaxDocuments(int64(capMaxN))); err != nil {
		t.Skipf("source cannot create capped collections: %v", err)
	}
	if err := db.CreateCollection(ctx, capCollSize,
		options.CreateCollection().SetCapped(true).SetSizeInBytes(int64(capSizeKB)*1024)); err != nil {
		t.Fatalf("create size-only capped collection: %v", err)
	}
	// an index on a capped collection, so index sync over a capped target is
	// covered as well (createIndex on a missing collection would implicitly
	// create an ordinary one)
	if _, err := db.Collection(capCollMax).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.D{{Key: "seq", Value: 1}}}); err != nil {
		t.Fatalf("create index on capped collection: %v", err)
	}

	docs := make([]interface{}, 0, capDocN)
	for i := 0; i < capDocN; i++ {
		docs = append(docs, bson.M{"_id": i, "seq": i, "payload": strings.Repeat("x", 32)})
	}
	for _, coll := range []string{capCollMax, capCollSize, capPlain} {
		if _, err := db.Collection(coll).InsertMany(ctx, docs); err != nil {
			t.Fatalf("insert into %s: %v", coll, err)
		}
	}

	// the fixture is only meaningful if nothing wrapped away on the source
	for _, coll := range []string{capCollMax, capCollSize, capPlain} {
		n, err := db.Collection(coll).CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("count source %s: %v", coll, err)
		}
		if n != capDocN {
			t.Fatalf("source fixture %s holds %d documents, want %d; the capped collection wrapped "+
				"and the test cannot distinguish wrap-around from data loss", coll, n, capDocN)
		}
	}
}

// capRunFullSync runs one collector to completion against the current state of
// source and target.
func capRunFullSync(t *testing.T, id, srcURL, tgtURL string, collectionDrop bool,
	httpPort int) *capProc {
	t.Helper()
	logDir := t.TempDir()
	confPath := capWriteConf(t, id, srcURL, tgtURL, logDir, collectionDrop, httpPort)
	proc := capStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })
	proc.waitExit(t)
	return proc
}

func TestCappedFullSync(t *testing.T) {
	srcURL := capEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := capEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := capDial(t, srcURL)
	tgt := capDial(t, tgtURL)
	ctx := context.Background()

	// clean slate on both sides
	if err := src.Database(capDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(capDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	_ = src.Database("mongoshake").Collection(capCkpt).Drop(ctx)

	capBuildSource(t, src)

	srcMax := capListInfo(t, src, capCollMax)
	srcSize := capListInfo(t, src, capCollSize)
	if srcMax == nil || srcMax.Options["capped"] != true {
		t.Fatalf("source fixture %s is not capped: %+v", capCollMax, srcMax)
	}
	if srcSize == nil || srcSize.Options["capped"] != true {
		t.Fatalf("source fixture %s is not capped: %+v", capCollSize, srcSize)
	}
	srcMaxIdx := capIndexNames(t, src, capCollMax)
	t.Logf("source %s options=%v indexes=%v", capCollMax, srcMax.Options, srcMaxIdx)
	t.Logf("source %s options=%v", capCollSize, srcSize.Options)

	proc := capRunFullSync(t, "cap_it", srcURL, tgtURL, true, 19531)

	// 1. both capped collections must still be capped on the target
	for name, srcInfo := range map[string]*capCollectionInfo{capCollMax: srcMax, capCollSize: srcSize} {
		tgtInfo := capListInfo(t, tgt, name)
		if tgtInfo == nil {
			t.Fatalf("target collection %s.%s was never created", capDB, name)
		}
		if tgtInfo.Options["capped"] != true {
			t.Fatalf("issue #995 reproduced: target %s.%s is a normal collection (type=%q options=%v), "+
				"the source's capped properties were lost and the target will now grow without bound",
				capDB, name, tgtInfo.Type, tgtInfo.Options)
		}
		// 2. size and max must survive verbatim, otherwise the target wraps at a
		// different point than the source and the counts diverge again
		if capCanonical(srcInfo.Options) != capCanonical(tgtInfo.Options) {
			t.Errorf("capped options differ on %s:\n  src=%s\n  tgt=%s",
				name, capCanonical(srcInfo.Options), capCanonical(tgtInfo.Options))
		}
	}

	// 3. every document must have arrived
	for _, coll := range []string{capCollMax, capCollSize, capPlain} {
		n, err := tgt.Database(capDB).Collection(coll).CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("count target %s: %v", coll, err)
		}
		if n != capDocN {
			t.Errorf("target %s holds %d documents, want %d", coll, n, capDocN)
		}
	}

	// 4. the ordinary collection must not have been capped by mistake
	plainInfo := capListInfo(t, tgt, capPlain)
	if plainInfo == nil {
		t.Fatalf("target collection %s.%s was never created", capDB, capPlain)
	}
	if _, ok := plainInfo.Options["capped"]; ok {
		t.Errorf("ordinary collection %s became capped on the target: options=%v",
			capPlain, plainInfo.Options)
	}

	// 5. index sets must match, capped collections included
	if got := capIndexNames(t, tgt, capCollMax); strings.Join(srcMaxIdx, ",") != strings.Join(got, ",") {
		t.Errorf("index mismatch on %s: src=%v tgt=%v", capCollMax, srcMaxIdx, got)
	}

	// 6. the create step must have actually run for both capped collections —
	// without this anchor a build that skips the step entirely and happens to find
	// the collections already capped would pass too. utils.NS has no String(), so
	// the namespace reaches the log as "{db coll}".
	for _, coll := range []string{capCollMax, capCollSize} {
		if !proc.logHasLine("create capped collection", coll, "succeed") {
			t.Errorf("collector log has no line reporting that capped collection %s.%s was "+
				"created on the target", capDB, coll)
		}
	}

	// 7. no write errors, and no collection left unconverted without saying so.
	// A hard create failure needs no log assertion: it propagates out of
	// startDocumentReplication into crash(-6), which waitExit already fails on.
	for _, bad := range []string{
		"E11000", "BadValue", "can't shard a capped",
		"Capped properties cannot be added to an existing collection",
	} {
		if proc.logContains(bad) {
			t.Errorf("collector log contains %q; the capped path was not handled cleanly", bad)
		}
	}
}

// TestCappedFullSyncTargetExistsAsOrdinary covers a user upgrading from a
// pre-fix build: the target already holds an ordinary collection of the same
// name, and full_sync.collection_exist_drop is left at its default false.
//
// Capped-ness cannot be added to an existing collection — convertToCapped is gone
// from the server — and dropping the user's data is not ours to decide. The run
// must therefore say so in the log and still complete, rather than fail outright
// or quietly carry on as if the collection were capped.
func TestCappedFullSyncTargetExistsAsOrdinary(t *testing.T) {
	srcURL := capEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := capEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := capDial(t, srcURL)
	tgt := capDial(t, tgtURL)
	ctx := context.Background()

	if err := src.Database(capDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(capDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	_ = src.Database("mongoshake").Collection(capCkpt).Drop(ctx)

	capBuildSource(t, src)

	// empty ordinary collections standing in for what a pre-fix run left behind;
	// empty so the subsequent insert cannot trip over duplicate keys and mask the
	// branch under test
	for _, coll := range []string{capCollMax, capCollSize, capPlain} {
		if err := tgt.Database(capDB).CreateCollection(ctx, coll); err != nil {
			t.Fatalf("pre-create ordinary target collection %s: %v", coll, err)
		}
	}

	proc := capRunFullSync(t, "cap_exist_it", srcURL, tgtURL, false, 19551)

	// The situation must be reported. The marker has to be wording only the capped
	// path emits: StartDropDestCollection already logs "ns ... already exists in
	// dest mongodb" for every collection when collection_exist_drop is false, so
	// asserting on that alone passes on an unfixed build for the wrong reason.
	const marker = "Capped properties cannot be added to an existing collection"
	if !proc.logContains(marker) {
		proc.dumpLog(t)
		t.Errorf("collector did not report that the ordinary target collection %s.%s blocks the "+
			"capped properties (expected the log to contain %q); a silent run leaves the user with an "+
			"unbounded target and no clue why", capDB, capCollMax, marker)
	}

	// the data must still land in the pre-existing collections
	for _, coll := range []string{capCollMax, capCollSize, capPlain} {
		n, err := tgt.Database(capDB).Collection(coll).CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("count target %s: %v", coll, err)
		}
		if n != capDocN {
			t.Errorf("target %s holds %d documents, want %d", coll, n, capDocN)
		}
	}

	// ... and the pre-existing collection must not have been dropped behind the
	// user's back to make room for the capped one
	if info := capListInfo(t, tgt, capCollMax); info == nil {
		t.Errorf("target collection %s.%s disappeared", capDB, capCollMax)
	} else if info.Options["capped"] == true {
		t.Errorf("target %s.%s became capped although full_sync.collection_exist_drop is false; "+
			"the user's existing collection must not be replaced silently", capDB, capCollMax)
	}

	for _, bad := range []string{"E11000", "panic"} {
		if proc.logContains(bad) {
			t.Errorf("collector log contains %q", bad)
		}
	}
}
