//go:build integration

// End-to-end verification for issue #993: full sync must treat a time-series
// collection as a time-series collection.
//
// Before the fix, full sync of a time-series collection failed or corrupted:
//
//   - the reader hinted {_id:1}, which a time-series logical view rejects with
//     BadValue because it has no _id index (#897 landed with the condition
//     inverted, so the hint was cleared for ordinary collections and kept for
//     time-series ones);
//   - it also sorted by _id, which the server satisfies with a blocking sort over
//     every unpacked measurement, spilling to disk and on large collections
//     exhausting the source's free space (#992/#993);
//   - the splitter probed min/max on _id and ran splitVector, both of which need
//     an index the view does not have;
//   - nothing created the target collection, so the first insert built an
//     ordinary one with an _id_ unique index the source does not have, which then
//     made every rerun fail with E11000.
//
// This drives a real `sync_mode = all` run over a source holding one time-series
// and one ordinary collection, then asserts on the target that:
//
//  1. the time-series collection is type "timeseries", not an ordinary one;
//  2. its creation options match the source byte for byte, including
//     expireAfterSeconds and the derived bucketMaxSpanSeconds;
//  3. its index set matches the source and carries no _id_ index;
//  4. every measurement arrived, compared excluding _id — a time-series target
//     re-buckets on insert, so the synthesized _id legitimately differs;
//  5. the ordinary collection still syncs exactly, _id included, which guards the
//     reader restructure against regressing the non-time-series path;
//  6. the collector log is free of duplicate-key and BadValue errors.
//
// Run with:
//
//	go test -count=1 -tags integration ./integration -run TestTimeSeriesFullSync -v -timeout 10m
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
	tsDB          = "tsfullsync"
	tsColl        = "metrics"
	tsPlainColl   = "plain"
	tsCkpt        = "ckpt_ts_it"
	tsLogFile     = "collector-ts.log"
	tsConsoleFile = "collector-ts-console.log"
	// `go run` may have to compile the collector before it starts at all
	tsReadyTO = 180 * time.Second
	tsSyncTO  = 120 * time.Second
	tsPoll    = 500 * time.Millisecond
	tsDocN    = 12
)

func tsEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func tsRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func tsDial(t *testing.T, url string) *mongo.Client {
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

func tsWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(tsRepoRoot(t), "conf", "collector.conf"))
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
	set("id", "ts_it")
	set("sync_mode", "all")
	set("mongo_urls", srcURL)
	set("mongo_connect_mode", "secondaryPreferred")
	set("tunnel", "direct")
	set("tunnel.address", tgtURL)
	set("tunnel.message", "raw")
	set("filter.namespace.white", tsDB)
	set("filter.ddl_enable", "true")
	set("full_sync.collection_exist_drop", "true")
	set("full_sync.create_index", "background")
	set("full_sync.reader.parallel_thread", "4") // must be ignored for time-series
	set("full_sync.executor.insert_on_dup_update", "false")
	set("checkpoint.storage.collection", tsCkpt)
	set("checkpoint.interval", "1000")
	set("full_sync.http_port", "19331")
	set("incr_sync.http_port", "19330")
	set("prom.http_port", "19332")
	set("system_profile_port", "19430")
	set("log.dir", logDir)
	set("log.file", tsLogFile)
	set("log.level", "info")
	// the assertions below read the log, which log.flush=false leaves buffered
	set("log.flush", "true")
	// the template carries an uncommented expression value the parser rejects (#998)
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-ts.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type tsCollectorProc struct {
	cmd         *exec.Cmd
	logPath     string
	consolePath string
	console     *os.File
	cleanupDir  string // diagnostic/ dir to remove if this test created it (go run mode)
	exited      chan error
}

// tsStartCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise the collector is launched via `go run ./cmd/collector`: some
// endpoint-security policies SIGKILL freshly-built standalone binaries on direct
// exec, where a `go run` child is allowed. The process runs in its own process
// group so kill() reaps the `go run` wrapper's collector child too.
func tsStartCollector(t *testing.T, confPath, logDir string) *tsCollectorProc {
	t.Helper()
	var cmd *exec.Cmd
	var cleanupDir string
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		root := tsRepoRoot(t)
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
	console, err := os.Create(filepath.Join(logDir, tsConsoleFile))
	if err != nil {
		t.Fatalf("create console capture: %v", err)
	}
	cmd.Stdout = console
	cmd.Stderr = console
	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	p := &tsCollectorProc{
		cmd:         cmd,
		logPath:     filepath.Join(logDir, tsLogFile),
		consolePath: console.Name(),
		console:     console,
		cleanupDir:  cleanupDir,
		exited:      make(chan error, 1),
	}
	// A collector that dies must be reported as dead. Skipping instead would turn
	// that into a permanently green test that verified nothing.
	go func() { p.exited <- cmd.Wait() }()
	return p
}

func (p *tsCollectorProc) requireAlive(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.exited:
		p.dumpLog(t)
		t.Fatalf("collector exited before the sync completed: %v (hosts that SIGKILL "+
			"freshly-built binaries need the go run path or MSHAKE_COLLECTOR_BIN)", err)
	default:
	}
}

// waitFullSync blocks until the collector has finished the full-sync stage, so
// the assertions below do not race an incremental sync that is still catching up.
func (p *tsCollectorProc) waitFullSync(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(tsReadyTO)
	for time.Now().Before(deadline) {
		p.requireAlive(t)
		// coordinator/replication.go logs this once every document syncer returns
		if p.logContains("full sync done!") {
			t.Log("collector reports the full sync stage is done")
			return
		}
		time.Sleep(tsPoll)
	}
	p.dumpLog(t)
	t.Fatalf("collector did not finish the full sync stage within %v", tsReadyTO)
}

func (p *tsCollectorProc) logContains(substr string) bool {
	data, err := os.ReadFile(p.logPath)
	return err == nil && strings.Contains(string(data), substr)
}

func (p *tsCollectorProc) kill() {
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

func (p *tsCollectorProc) dumpLog(t *testing.T) {
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

// tsCollectionInfo is the listCollections entry for one collection.
type tsCollectionInfo struct {
	Name    string `bson:"name"`
	Type    string `bson:"type"`
	Options bson.M `bson:"options"`
}

func tsListInfo(t *testing.T, client *mongo.Client, name string) *tsCollectionInfo {
	t.Helper()
	cur, err := client.Database(tsDB).ListCollections(context.Background(), bson.M{"name": name})
	if err != nil {
		t.Fatalf("listCollections(%s): %v", name, err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var info tsCollectionInfo
		if err := bson.Unmarshal(cur.Current, &info); err != nil {
			t.Fatalf("decode collection info of %s: %v", name, err)
		}
		return &info
	}
	return nil
}

func tsIndexNames(t *testing.T, client *mongo.Client, coll string) []string {
	t.Helper()
	cur, err := client.Database(tsDB).Collection(coll).Indexes().List(context.Background())
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

// tsCanonical renders a value as a stable string with every document's keys
// sorted, so that two documents carrying the same data compare equal regardless
// of BSON field order.
//
// Byte-level comparison is not usable here: a time-series target re-buckets on
// insert and unpacks measurements in its own field order, and bson.Marshal of a
// bson.M follows Go's randomized map iteration order.
func tsCanonical(v interface{}) string {
	switch t := v.(type) {
	case bson.M:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+"="+tsCanonical(t[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case bson.D:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, e.Key+"="+tsCanonical(e.Value))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case bson.A:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, tsCanonical(e))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case primitive.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case primitive.ObjectID:
		return t.Hex()
	case primitive.Binary:
		return fmt.Sprintf("%x:%x", t.Subtype, t.Data)
	case primitive.Regex:
		return fmt.Sprintf("/%s/%s", t.Pattern, t.Options)
	case []byte:
		return fmt.Sprintf("%x", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// tsMeasurementSet reads every measurement of a time-series collection and
// returns them as canonical strings with _id removed. A time-series target
// re-buckets on insert, so the synthesized _id legitimately differs between
// source and target and must not take part in the comparison.
func tsMeasurementSet(t *testing.T, client *mongo.Client) []string {
	t.Helper()
	cur, err := client.Database(tsDB).Collection(tsColl).Find(context.Background(), bson.M{})
	if err != nil {
		t.Fatalf("read measurements: %v", err)
	}
	defer cur.Close(context.Background())
	var out []string
	for cur.Next(context.Background()) {
		var d bson.M
		if err := cur.Decode(&d); err != nil {
			t.Fatalf("decode measurement: %v", err)
		}
		delete(d, "_id")
		out = append(out, tsCanonical(d))
	}
	sort.Strings(out)
	return out
}

func TestTimeSeriesFullSync(t *testing.T) {
	srcURL := tsEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := tsEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := tsDial(t, srcURL)
	tgt := tsDial(t, tgtURL)
	ctx := context.Background()

	var hello bson.M
	if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("hello on source: %v", err)
	}

	// clean slate on both sides
	if err := src.Database(tsDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(tsDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	_ = src.Database("mongoshake").Collection(tsCkpt).Drop(ctx)

	// a time-series collection with an explicit expireAfterSeconds, so the option
	// passthrough is covered and not just timeField/metaField/granularity
	if err := src.Database(tsDB).CreateCollection(ctx, tsColl,
		options.CreateCollection().
			SetTimeSeriesOptions(options.TimeSeries().
				SetTimeField("ts").
				SetMetaField("meta").
				SetGranularity("seconds")).
			SetExpireAfterSeconds(7200)); err != nil {
		t.Skipf("source cannot create time-series collections (needs MongoDB 5.0+): %v", err)
	}

	base := time.Now().Add(-time.Duration(tsDocN) * time.Minute).Truncate(time.Millisecond)
	for i := 0; i < tsDocN; i++ {
		doc := bson.M{
			"ts":   base.Add(time.Duration(i) * time.Minute),
			"meta": bson.M{"host": fmt.Sprintf("h%d", i%3), "region": "cn"},
			"v":    int64(i),
		}
		if _, err := src.Database(tsDB).Collection(tsColl).InsertOne(ctx, doc); err != nil {
			t.Fatalf("insert measurement %d: %v", i, err)
		}
	}
	// an ordinary collection alongside it: the reader changes must not regress the
	// non-time-series path
	plain := make([]interface{}, 0, 5)
	for i := 0; i < 5; i++ {
		plain = append(plain, bson.M{"_id": i, "name": fmt.Sprintf("n%d", i), "arr": bson.A{i, i + 1}})
	}
	if _, err := src.Database(tsDB).Collection(tsPlainColl).InsertMany(ctx, plain); err != nil {
		t.Fatalf("insert plain docs: %v", err)
	}

	srcInfo := tsListInfo(t, src, tsColl)
	if srcInfo == nil || srcInfo.Type != "timeseries" {
		t.Fatalf("source fixture is not a time-series collection: %+v", srcInfo)
	}
	srcIdx := tsIndexNames(t, src, tsColl)
	wantMeasurements := tsMeasurementSet(t, src)
	t.Logf("source time-series options=%v indexes=%v measurements=%d",
		srcInfo.Options, srcIdx, len(wantMeasurements))

	logDir := t.TempDir()
	confPath := tsWriteConf(t, srcURL, tgtURL, logDir)
	proc := tsStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// wait for the data, then for the stage to be reported complete
	deadline := time.Now().Add(tsSyncTO)
	for time.Now().Before(deadline) {
		proc.requireAlive(t)
		if n, err := tgt.Database(tsDB).Collection(tsColl).CountDocuments(ctx, bson.M{}); err == nil &&
			int(n) == tsDocN {
			break
		}
		time.Sleep(tsPoll)
	}
	proc.waitFullSync(t)
	proc.requireAlive(t)

	// 1. the target must be a time-series collection, not an ordinary one
	tgtInfo := tsListInfo(t, tgt, tsColl)
	if tgtInfo == nil {
		t.Fatalf("target collection %s.%s was never created", tsDB, tsColl)
	}
	if tgtInfo.Type != "timeseries" {
		t.Fatalf("issue #993 reproduced: target %s.%s is type[%q], the time-series properties were lost "+
			"(options=%v)", tsDB, tsColl, tgtInfo.Type, tgtInfo.Options)
	}

	// 2. creation options must survive verbatim, expireAfterSeconds included
	if tsCanonical(srcInfo.Options) != tsCanonical(tgtInfo.Options) {
		t.Errorf("time-series options differ:\n  src=%s\n  tgt=%s",
			tsCanonical(srcInfo.Options), tsCanonical(tgtInfo.Options))
	}

	// 3. index set must match, and in particular carry no _id_ index
	tgtIdx := tsIndexNames(t, tgt, tsColl)
	if strings.Join(srcIdx, ",") != strings.Join(tgtIdx, ",") {
		t.Errorf("index mismatch on %s: src=%v tgt=%v", tsColl, srcIdx, tgtIdx)
	}
	for _, name := range tgtIdx {
		if name == "_id_" {
			t.Errorf("target time-series collection has an _id_ index the source does not have; "+
				"reruns would fail with E11000. indexes=%v", tgtIdx)
		}
	}

	// 4. every measurement must arrive; _id is excluded by construction
	gotMeasurements := tsMeasurementSet(t, tgt)
	if len(gotMeasurements) != len(wantMeasurements) {
		t.Errorf("measurement count differs: src=%d tgt=%d", len(wantMeasurements), len(gotMeasurements))
	}
	for i := range wantMeasurements {
		if i >= len(gotMeasurements) {
			break
		}
		if wantMeasurements[i] != gotMeasurements[i] {
			t.Errorf("measurement %d differs:\n  src=%s\n  tgt=%s", i, wantMeasurements[i], gotMeasurements[i])
		}
	}

	// 5. the ordinary collection must still sync exactly, _id included
	plainN, err := tgt.Database(tsDB).Collection(tsPlainColl).CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count plain docs on target: %v", err)
	}
	if plainN != int64(len(plain)) {
		t.Errorf("ordinary collection regressed: expected %d documents on the target, got %d",
			len(plain), plainN)
	}
	for _, want := range plain {
		id := want.(bson.M)["_id"]
		var got bson.M
		if err := tgt.Database(tsDB).Collection(tsPlainColl).
			FindOne(ctx, bson.M{"_id": id}).Decode(&got); err != nil {
			t.Errorf("ordinary document _id=%v missing on the target: %v", id, err)
			continue
		}
		if tsCanonical(want) != tsCanonical(got) {
			t.Errorf("ordinary document _id=%v differs:\n  src=%s\n  tgt=%s",
				id, tsCanonical(want), tsCanonical(got))
		}
	}

	// 6. no duplicate-key or query-shape errors anywhere in the run
	for _, bad := range []string{"E11000", "BadValue", "non-multi update", "must provide at least one element"} {
		if proc.logContains(bad) {
			t.Errorf("collector log contains %q; the time-series path was not handled cleanly", bad)
		}
	}
}
