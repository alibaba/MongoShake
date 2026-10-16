//go:build integration

// End-to-end verification for the transaction checkpoint pin fix.
//
// Scenario (the silent data-loss window), reproduced with a REAL cross-shard
// distributed transaction whose commit phase is frozen with docker pause:
//
//	t1  commitTransaction starts; shard1 writes its prepare oplog entry
//	t2  shard2 is paused, so 2PC stalls with shard1 stuck in prepared state
//	t3  normal DML on shard1 (dispatched, acked, would advance the checkpoint)
//	t4  collector killed with SIGKILL while shard1 is still prepared
//	t5  collector restarted (fetches oplog with $gt checkpoint)
//	t6  shard2 unpaused -> 2PC completes -> commitTransaction oplog arrives
//
// Before the fix, the checkpoint could persist t3 while the transaction was
// only buffered in memory, so t5 skipped the prepare entry and the
// transaction was silently committed as empty. After the fix, the checkpoint
// is pinned below the prepare ts until the inner ops are acked.
//
// Run with:
//
//	go test -tags integration ./integration -run TestTxnCheckpointPin -v -timeout 10m
//
// Environment (overridable):
//
//	MSHAKE_MONGOS_URL    mongos of the source sharded cluster (default mongodb://127.0.0.1:27018)
//	MSHAKE_SHARD1_URL    shard1 of the source cluster (default mongodb://127.0.0.1:27021)
//	MSHAKE_SHARD2_URL    shard2 of the source cluster (default mongodb://127.0.0.1:27022)
//	MSHAKE_CONFIGSVR_URL config server replica set (default mongodb://127.0.0.1:27020)
//	MSHAKE_TGT_URL       target mongodb (default mongodb://127.0.0.1:27019)
//	MSHAKE_SHARD2_CONTAINER docker container name of shard2 (default mshake-sh2)
//	MSHAKE_COLLECTOR_BIN prebuilt collector binary (default: launch via `go run`)
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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDB     = "txnpintest"
	testColl   = "c1"
	ckptDB     = "mongoshake"
	ckptColl   = "ckpt_txnpin_it"
	startupTO  = 90 * time.Second
	syncTO     = 90 * time.Second
	pollPeriod = 500 * time.Millisecond

	// collector stdout/stderr, which is the only trace left when the process
	// dies before it opens its own log file
	consoleLogName = "collector-it-console.log"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

func dial(t *testing.T, url string) *mongo.Client {
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

func writeConf(t *testing.T, srcURL, tgtURL, logDir string, cfgSvrURL ...string) string {
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
	set("id", "txnpin_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	if len(cfgSvrURL) > 0 && cfgSvrURL[0] != "" {
		set("mongo_cs_url", cfgSvrURL[0])
		// sharded source requires worker count == shard count
		set("incr_sync.worker", "2")
		set("incr_sync.tunnel.write_thread", "2")
		// sharded source requires an explicit checkpoint storage; use shard1
		set("checkpoint.storage.url", strings.Split(srcURL, ";")[0])
	}
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone") // SetDirect(true), required for mapped ports
	set("incr_sync.mongo_fetch_method", "oplog")
	set("checkpoint.storage.collection", ckptColl)
	set("checkpoint.interval", "1000")
	// start from now: replaying the whole retained oplog would slow the test
	// and let stale docs satisfy the waiters before the scenario runs
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19101")
	set("incr_sync.http_port", "19100")
	set("prom.http_port", "19102")
	set("system_profile_port", "19200")
	set("log.dir", logDir)
	set("log.file", "collector-it.log")
	// the template on develop carries an uncommented expression value that the
	// config parser rejects; override with a plain number
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-it.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type collectorProc struct {
	cmd     *exec.Cmd
	logDir  string
	console *os.File
}

// startCollector launches the collector.
//
// If MSHAKE_COLLECTOR_BIN is set, that prebuilt binary is exec'd directly.
// Otherwise it is launched via `go run ./cmd/collector`: some endpoint-security
// policies SIGKILL freshly-built standalone binaries on direct exec, where a
// `go run` child is allowed.
//
// Either way the process gets its own process group, because this test's whole
// point is a hard SIGKILL followed by a restart. Under `go run` the collector
// is a child of the wrapper, so signalling only the wrapper would orphan the
// real collector and the restarted one would race it for the same oplog.
func startCollector(t *testing.T, confPath, logDir string) *collectorProc {
	t.Helper()
	var cmd *exec.Cmd
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		t.Logf("using prebuilt collector binary: %s", bin)
		cmd = exec.Command(bin, "-conf="+confPath)
		cmd.Dir = logDir
	} else {
		cmd = exec.Command("go", "run", "./cmd/collector", "-conf="+confPath)
		cmd.Dir = repoRoot(t)
		// `go run` makes the collector create <repoRoot>/diagnostic. Registered
		// here rather than hung off the returned struct, because the caller
		// reassigns its proc variable when it restarts the collector and both
		// t.Cleanup closures capture that variable, not the first process.
		diag := filepath.Join(cmd.Dir, "diagnostic")
		if _, err := os.Stat(diag); os.IsNotExist(err) {
			t.Cleanup(func() { _ = os.RemoveAll(diag) })
		}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// A collector that dies while parsing its config never opens its log file,
	// so capture the child's console output as well or the failure leaves
	// nothing but an empty log directory behind.
	console, err := os.Create(filepath.Join(logDir, consoleLogName))
	if err != nil {
		t.Fatalf("create console log: %v", err)
	}
	cmd.Stdout = console
	cmd.Stderr = console

	if err := cmd.Start(); err != nil {
		_ = console.Close()
		t.Fatalf("start collector: %v", err)
	}
	p := &collectorProc{cmd: cmd, logDir: logDir, console: console}
	go func() { _ = cmd.Wait(); _ = console.Close() }()
	return p
}

func (p *collectorProc) kill() {
	if p.cmd.Process == nil {
		return
	}
	// negative pid targets the whole process group; fall back to the single
	// process if the group signal is not permitted
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
	}
	time.Sleep(500 * time.Millisecond)
}

func (p *collectorProc) dumpLog(t *testing.T) {
	tail := func(name string, n int) {
		data, err := os.ReadFile(filepath.Join(p.logDir, name))
		if err != nil || len(data) == 0 {
			return
		}
		lines := strings.Split(string(data), "\n")
		if len(lines) > n {
			lines = lines[len(lines)-n:]
		}
		t.Logf("%s tail:\n%s", name, strings.Join(lines, "\n"))
	}
	tail(filepath.Join("logs", "collector-it.log"), 40)
	tail(consoleLogName, 20)
}

func waitCheckpoint(t *testing.T, src *mongo.Client, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var doc bson.M
		err := src.Database(ckptDB).Collection(ckptColl).FindOne(context.Background(), bson.M{}).Decode(&doc)
		if err == nil {
			if v, ok := doc["ckpt"].(int64); ok {
				return v
			}
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("checkpoint document not created within %v", timeout)
	return 0
}

func readCheckpoint(t *testing.T, src *mongo.Client) int64 {
	t.Helper()
	var doc bson.M
	err := src.Database(ckptDB).Collection(ckptColl).FindOne(context.Background(), bson.M{}).Decode(&doc)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	v, ok := doc["ckpt"].(int64)
	if !ok {
		t.Fatalf("checkpoint doc has no int64 ckpt field: %v", doc)
	}
	return v
}

func waitDoc(t *testing.T, client *mongo.Client, id interface{}, timeout time.Duration) {
	t.Helper()
	waitDocIn(t, client, testDB, testColl, id, timeout)
}

func waitDocIn(t *testing.T, client *mongo.Client, db, coll string, id interface{}, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := client.Database(db).Collection(coll).
			CountDocuments(context.Background(), bson.M{"_id": id})
		if err == nil && n > 0 {
			return
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("doc %v not synced to target %s.%s within %v", id, db, coll, timeout)
}

func packed(ts primitive.Timestamp) int64 {
	return int64(ts.T)<<32 | int64(ts.I)
}

// startHeartbeat keeps the oplog moving: a single write can fall into the
// checkpoint throttle window at startup, after which an idle oplog never
// triggers another checkpoint calculation. Continuous traffic mirrors
// production and lets the checkpoint advance.
func startHeartbeat(t *testing.T, coll *mongo.Collection, runID string) func() {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = coll.InsertOne(context.Background(), bson.M{"_id": fmt.Sprintf("hb-%s-%d", runID, i)})
			i++
			time.Sleep(300 * time.Millisecond)
		}
	}()
	return func() { close(stop); <-done }
}

func latestOplogTs(t *testing.T, src *mongo.Client) primitive.Timestamp {
	t.Helper()
	var doc bson.M
	err := src.Database("local").Collection("oplog.rs").FindOne(
		context.Background(), bson.M{},
		options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}),
	).Decode(&doc)
	if err != nil {
		t.Fatalf("read latest oplog ts: %v", err)
	}
	ts, ok := doc["ts"].(primitive.Timestamp)
	if !ok {
		t.Fatalf("latest oplog entry has no ts: %v", doc)
	}
	return ts
}

func currentTerm(t *testing.T, src *mongo.Client) int64 {
	t.Helper()
	var doc bson.M
	if err := src.Database("admin").RunCommand(context.Background(), bson.D{{Key: "hello", Value: 1}}).Decode(&doc); err != nil {
		t.Fatalf("hello: %v", err)
	}
	term, _ := doc["term"].(int64)
	return term
}

func insertOplog(t *testing.T, src *mongo.Client, entry bson.M) {
	t.Helper()
	if _, err := src.Database("local").Collection("oplog.rs").InsertOne(context.Background(), entry); err != nil {
		t.Fatalf("inject oplog entry: %v", err)
	}
}

func dockerCmd(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %v failed: %v\n%s", args, err, out)
	}
}

// latestPrepareTs returns the packed timestamp of the newest prepare entry in
// shard1's oplog, waiting until it appears.
func latestPrepareTs(t *testing.T, shard1 *mongo.Client) int64 {
	t.Helper()
	deadline := time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		var doc bson.M
		err := shard1.Database("local").Collection("oplog.rs").FindOne(
			context.Background(),
			bson.M{"ns": "admin.$cmd", "o.prepare": bson.M{"$exists": true}},
			options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}),
		).Decode(&doc)
		if err == nil {
			if ts, ok := doc["ts"].(primitive.Timestamp); ok {
				return packed(ts)
			}
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("no prepare entry in shard1 oplog within %v", syncTO)
	return 0
}

func TestTxnCheckpointPin(t *testing.T) {
	mongosURL := envOr("MSHAKE_MONGOS_URL", "mongodb://127.0.0.1:27018")
	shard1URL := envOr("MSHAKE_SHARD1_URL", "mongodb://127.0.0.1:27021")
	shard2URL := envOr("MSHAKE_SHARD2_URL", "mongodb://127.0.0.1:27022")
	cfgURL := envOr("MSHAKE_CONFIGSVR_URL", "mongodb://127.0.0.1:27020")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	shard2Container := envOr("MSHAKE_SHARD2_CONTAINER", "mshake-sh2")

	mongos := dial(t, mongosURL)
	shard1 := dial(t, shard1URL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()

	// clean slate: delete docs but keep collections (implicit collection
	// creation is not allowed inside cross-shard transactions)
	_, _ = tgt.Database(testDB).Collection(testColl).DeleteMany(ctx, bson.M{})
	_, _ = tgt.Database("txnpintest2").Collection("c2").DeleteMany(ctx, bson.M{})
	_ = shard1.Database(ckptDB).Collection(ckptColl).Drop(ctx)
	_, _ = mongos.Database(testDB).Collection(testColl).DeleteMany(ctx, bson.M{})
	_, _ = mongos.Database("txnpintest2").Collection("c2").DeleteMany(ctx, bson.M{})
	// collections must exist upfront: implicit creation is not allowed inside
	// cross-shard transactions
	for _, tc := range []struct{ db, coll string }{{testDB, testColl}, {"txnpintest2", "c2"}} {
		if err := mongos.Database(tc.db).RunCommand(ctx, bson.D{{Key: "create", Value: tc.coll}}).Err(); err != nil &&
			!strings.Contains(err.Error(), "NamespaceExists") {
			t.Fatalf("create %s.%s: %v", tc.db, tc.coll, err)
		}
	}

	// run-unique ids so leftovers from earlier runs can never satisfy waiters
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	idInTxn := "in-txn-" + runID
	idInTxn2 := "in-txn2-" + runID
	idPost := "post-prepare-" + runID

	logDir := t.TempDir()
	confPath := writeConf(t,
		shard1URL+";"+shard2URL, // MongoShake connects to every shard for oplog tailing
		tgtURL, logDir, cfgURL)

	srcColl := mongos.Database(testDB).Collection(testColl)
	stopHb := startHeartbeat(t, srcColl, runID)
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// collector up and checkpoint initialized on shard1 (heartbeat keeps it moving)
	ckpt0 := waitCheckpoint(t, shard1, startupTO)
	t.Logf("initial checkpoint on shard1: %x", ckpt0)

	// start a cross-shard transaction and begin committing it in the
	// background; freeze shard2 so 2PC stalls with shard1 stuck in prepared
	sess, err := mongos.StartSession()
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	defer sess.EndSession(ctx)
	sc := mongo.NewSessionContext(ctx, sess)
	if err := sess.StartTransaction(); err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	if _, err := srcColl.InsertOne(sc, bson.M{"_id": idInTxn, "v": 1}); err != nil {
		t.Fatalf("insert in-txn doc (shard1): %v", err)
	}
	if _, err := mongos.Database("txnpintest2").Collection("c2").InsertOne(sc, bson.M{"_id": idInTxn2, "v": 2}); err != nil {
		t.Fatalf("insert in-txn doc (shard2): %v", err)
	}

	commitDone := make(chan error, 1)
	go func() { commitDone <- sess.CommitTransaction(sc) }()

	// give the coordinator a moment to reach the prepare phase, then freeze
	// shard2 so the commit decision cannot be delivered
	time.Sleep(1500 * time.Millisecond)
	dockerCmd(t, "pause", shard2Container)
	defer func() { _ = exec.Command("docker", "unpause", shard2Container).Run() }()

	// shard1 must have written its prepare oplog entry by now
	preparePacked := latestPrepareTs(t, shard1)
	t.Logf("shard1 prepared at packed %x, 2PC frozen", preparePacked)

	// t3: normal DML on shard1 while the transaction is buffered in memory
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": idPost, "v": 3}); err != nil {
		t.Fatalf("insert post-prepare doc: %v", err)
	}
	waitDoc(t, tgt, idPost, syncTO)
	time.Sleep(2 * time.Second) // let at least one checkpoint interval pass

	// core assertion 1: shard1's persisted checkpoint must be pinned strictly
	// below the prepare ts
	ckpt := readCheckpoint(t, shard1)
	t.Logf("shard1 checkpoint after DML synced: %x (prepare: %x)", ckpt, preparePacked)
	if ckpt >= preparePacked {
		t.Fatalf("checkpoint %x advanced past the in-flight transaction prepare %x: "+
			"a restart now would lose the transaction", ckpt, preparePacked)
	}

	// t4: hard kill before the commit arrives
	proc.kill()
	t.Log("collector killed with SIGKILL while shard1 is still prepared")

	// t5: restart; the pinned checkpoint makes the reader re-fetch the prepare
	proc = startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill() })
	time.Sleep(3 * time.Second)

	// t6: unfreeze shard2 -> 2PC completes -> commitTransaction oplog arrives
	dockerCmd(t, "unpause", shard2Container)
	select {
	case err := <-commitDone:
		t.Logf("commitTransaction finished: err=%v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("commitTransaction did not finish within 60s after unpause")
	}

	// core assertion 2: the transaction data on BOTH shards must reach the target
	waitDocIn(t, tgt, testDB, testColl, idInTxn, syncTO)
	waitDocIn(t, tgt, "txnpintest2", "c2", idInTxn2, syncTO)

	for _, tc := range []struct{ db, coll, id string }{
		{testDB, testColl, idInTxn},
		{"txnpintest2", "c2", idInTxn2},
	} {
		n, err := tgt.Database(tc.db).Collection(tc.coll).
			CountDocuments(ctx, bson.M{"_id": tc.id})
		if err != nil {
			t.Fatalf("count %s on target: %v", tc.id, err)
		}
		if n != 1 {
			t.Fatalf("expected exactly 1 %s doc on target %s.%s, got %d", tc.id, tc.db, tc.coll, n)
		}
	}
	t.Log("transaction data survived kill -9 + restart: PASS")
	fmt.Printf("checkpoint pin verified: prepare=%x\n", preparePacked)
}
