//go:build integration

// End-to-end verification for issue #1041: syncing MongoDB -> Amazon DocumentDB.
//
// DocumentDB only emulates the MongoDB API. Two of its gaps break MongoShake, and
// they break it at different stages:
//
//  1. listCollections rejects the "type" key in the filter. Full sync builds that
//     filter for the *target* in StartDropDestCollection, so with
//     full_sync.collection_exist_drop = false the run dies before moving a single
//     document: "Field 'type' is currently not supported".
//  2. applyOps does not exist. Every incremental DDL oplog (create / collMod /
//     drop / dropIndexes) was wrapped in admin.applyOps, so the first DDL after
//     full sync fails with CommandNotSupported and is retried forever. The
//     checkpoint freezes and no later write ever reaches the target - a silent,
//     permanent stall rather than a crash.
//
// special.target.db.flag = documentdb avoids both, and forces retryWrites=false
// because DocumentDB engines before 8.0.2 reject retryable writes with code 301.
//
// TestDocumentDBSyncEndToEnd drives a real sync_mode = all run and asserts full
// sync parity, then DML *and* DDL parity, then that the checkpoint moved past the
// DDL - the last one is what distinguishes a working replay from a stall.
//
// TestDocumentDBDDLStallsWithoutFlag is the red anchor: the same target with the
// flag off must still hit applyOps. Without it, a future refactor could drop the
// native path and the green test above would keep passing on a MongoDB target.
//
// Coverage limit, stated plainly: the local documentdb-local image reports 7.0.0
// and *accepts* the listCollections "type" filter, so gap 1 cannot be reproduced
// here - it is verified against real MongoDB by
// docsyncer.TestDestListCollectionFilter and documented in the AWS support matrix.
// Gap 2 does reproduce locally and is what these two tests pin down.
//
// Run with:
//
//	MSHAKE_DOCDB_URL='mongodb://user:pass@127.0.0.1:10260/?tls=true&tlsInsecure=true&authMechanism=SCRAM-SHA-256&directConnection=true' \
//	  go test -count=1 -tags integration ./integration -run TestDocumentDB -v -timeout 15m
//
// -count=1 is required: the collector runs as a `go run` subprocess, so Go's test
// cache cannot see it change and would replay a stale PASS.
//
// Environment:
//
//	MSHAKE_DOCDB_URL      DocumentDB target; the test skips when unset
//	MSHAKE_DOCDB_SRC_URL  source replica set (default mongodb://127.0.0.1:27030)
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
	ddbDB       = "ddb_it"
	ddbColl     = "events"
	ddbCkpt     = "ckpt_ddb_it"
	ddbLogFile  = "collector-ddb.log"
	ddbHTTPPort = 9430
)

func ddbEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func ddbRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve repo root")
	}
	return filepath.Dir(filepath.Dir(file))
}

func ddbDial(t *testing.T, url string) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping %s: %v", url, err)
	}
	return client
}

func ddbTargetURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("MSHAKE_DOCDB_URL")
	if url == "" {
		t.Skip("MSHAKE_DOCDB_URL not set; DocumentDB target unavailable")
	}
	return url
}

func ddbWriteConf(t *testing.T, id, srcURL, tgtURL, logDir, targetFlag string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(ddbRepoRoot(t), "conf", "collector.conf"))
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
	set("sync_mode", "all")
	set("mongo_urls", srcURL)
	set("mongo_connect_mode", "secondaryPreferred")
	set("tunnel", "direct")
	set("tunnel.address", tgtURL)
	set("tunnel.message", "raw")
	set("filter.namespace.white", ddbDB)
	set("filter.ddl_enable", "true")
	// the customer's setting, and the one that reaches the target listCollections
	set("full_sync.collection_exist_drop", "false")
	set("full_sync.create_index", "background")
	set("special.target.db.flag", targetFlag)
	set("incr_sync.mongo_fetch_method", "oplog")
	set("checkpoint.storage.url", srcURL)
	set("checkpoint.storage.collection", ddbCkpt)
	set("checkpoint.interval", "1000")
	set("full_sync.http_port", fmt.Sprintf("%d", ddbHTTPPort))
	set("incr_sync.http_port", fmt.Sprintf("%d", ddbHTTPPort+1))
	set("prom.http_port", fmt.Sprintf("%d", ddbHTTPPort+2))
	set("system_profile_port", fmt.Sprintf("%d", ddbHTTPPort+10))
	set("log.dir", logDir)
	set("log.file", ddbLogFile)
	set("log.level", "info")
	set("log.flush", "true")
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-ddb.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

type ddbProc struct {
	cmd         *exec.Cmd
	logPath     string
	consolePath string
	console     *os.File
	exited      chan error
}

func ddbStartCollector(t *testing.T, confPath, logDir string) *ddbProc {
	t.Helper()
	root := ddbRepoRoot(t)
	// A prebuilt binary is SIGKILLed by endpoint security on some macOS hosts, so
	// `go run` is the only portable launch path here.
	cmd := exec.Command("go", "run", "./cmd/collector", "-conf="+confPath)
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	console, err := os.Create(filepath.Join(logDir, "collector-ddb-console.log"))
	if err != nil {
		t.Fatalf("create console capture: %v", err)
	}
	cmd.Stdout = console
	cmd.Stderr = console
	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	p := &ddbProc{
		cmd:         cmd,
		logPath:     filepath.Join(logDir, ddbLogFile),
		consolePath: console.Name(),
		console:     console,
		exited:      make(chan error, 1),
	}
	go func() { p.exited <- cmd.Wait() }()
	return p
}

func (p *ddbProc) kill() {
	if p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	select {
	case <-p.exited:
	case <-time.After(30 * time.Second):
	}
	_ = p.console.Close()
}

func (p *ddbProc) log() string {
	body, _ := os.ReadFile(p.logPath)
	console, _ := os.ReadFile(p.consolePath)
	return string(body) + "\n" + string(console)
}

func (p *ddbProc) dumpLog(t *testing.T) {
	t.Helper()
	t.Logf("collector log:\n%s", p.log())
}

// ddbWait blocks until cond holds, failing with the collector log if it never does.
func ddbWait(t *testing.T, p *ddbProc, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.exited:
			p.dumpLog(t)
			t.Fatalf("collector exited while waiting for %s: %v", what, err)
		default:
		}
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	p.dumpLog(t)
	t.Fatalf("timed out waiting for %s", what)
}

func ddbCount(t *testing.T, client *mongo.Client, coll string) int64 {
	t.Helper()
	n, err := client.Database(ddbDB).Collection(coll).CountDocuments(context.Background(), bson.M{})
	if err != nil {
		t.Fatalf("count %s: %v", coll, err)
	}
	return n
}

func ddbIndexNames(t *testing.T, client *mongo.Client, coll string) []string {
	t.Helper()
	cur, err := client.Database(ddbDB).Collection(coll).Indexes().List(context.Background())
	if err != nil {
		t.Fatalf("list indexes %s: %v", coll, err)
	}
	var indexes []bson.M
	if err := cur.All(context.Background(), &indexes); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	names := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		names = append(names, fmt.Sprintf("%v", idx["name"]))
	}
	return names
}

func ddbCollections(t *testing.T, client *mongo.Client) []string {
	t.Helper()
	names, err := client.Database(ddbDB).ListCollectionNames(context.Background(), bson.M{})
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	return names
}

func ddbReset(t *testing.T, src, tgt *mongo.Client) {
	t.Helper()
	ctx := context.Background()
	if err := src.Database(ddbDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(ddbDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	if err := src.Database("mongoshake").Collection(ddbCkpt).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint: %v", err)
	}
}

func ddbSeed(t *testing.T, src *mongo.Client) {
	t.Helper()
	ctx := context.Background()
	coll := src.Database(ddbDB).Collection(ddbColl)
	docs := make([]interface{}, 0, 40)
	for i := 0; i < 40; i++ {
		docs = append(docs, bson.M{"_id": i, "uid": fmt.Sprintf("u%d", i), "score": i % 7})
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "uid", Value: 1}}, Options: options.Index().SetName("uid_1"),
	}); err != nil {
		t.Fatalf("seed index: %v", err)
	}
	// a TTL index, so the DDL phase can exercise collMod's expireAfterSeconds -
	// the one collMod variant DocumentDB supports on every engine version
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ts", Value: 1}},
		Options: options.Index().SetName("ttl_1").SetExpireAfterSeconds(3600),
	}); err != nil {
		t.Fatalf("seed ttl index: %v", err)
	}
}

// TestDocumentDBSyncEndToEnd is the green path: full sync, then DML and DDL, must
// all land on DocumentDB and the checkpoint must advance past the DDL.
func TestDocumentDBSyncEndToEnd(t *testing.T) {
	tgtURL := ddbTargetURL(t)
	srcURL := ddbEnvOr("MSHAKE_DOCDB_SRC_URL", "mongodb://127.0.0.1:27030")

	src := ddbDial(t, srcURL)
	defer src.Disconnect(context.Background())
	tgt := ddbDial(t, tgtURL)
	defer tgt.Disconnect(context.Background())

	ddbReset(t, src, tgt)
	ddbSeed(t, src)

	logDir := t.TempDir()
	confPath := ddbWriteConf(t, "ddb_it_green", srcURL, tgtURL, logDir, "documentdb")
	p := ddbStartCollector(t, confPath, logDir)
	defer p.kill()

	ctx := context.Background()

	// full sync: documents and secondary indexes
	ddbWait(t, p, "full sync of seeded documents", 3*time.Minute, func() bool {
		return ddbCount(t, tgt, ddbColl) == 40
	})
	ddbWait(t, p, "full sync of secondary index", time.Minute, func() bool {
		for _, n := range ddbIndexNames(t, tgt, ddbColl) {
			if n == "uid_1" {
				return true
			}
		}
		return false
	})

	// incremental DML
	coll := src.Database(ddbDB).Collection(ddbColl)
	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1000, "uid": "incr", "score": 1}); err != nil {
		t.Fatalf("incr insert: %v", err)
	}
	if _, err := coll.UpdateOne(ctx, bson.M{"_id": 3}, bson.M{"$set": bson.M{"score": 77}}); err != nil {
		t.Fatalf("incr update: %v", err)
	}
	if _, err := coll.DeleteOne(ctx, bson.M{"_id": 4}); err != nil {
		t.Fatalf("incr delete: %v", err)
	}
	ddbWait(t, p, "incremental DML", 2*time.Minute, func() bool {
		var doc bson.M
		err := tgt.Database(ddbDB).Collection(ddbColl).
			FindOne(ctx, bson.M{"_id": 3}).Decode(&doc)
		if err != nil || fmt.Sprintf("%v", doc["score"]) != "77" {
			return false
		}
		gone := tgt.Database(ddbDB).Collection(ddbColl).
			FindOne(ctx, bson.M{"_id": 4}).Err() == mongo.ErrNoDocuments
		return gone && ddbCount(t, tgt, ddbColl) == 40
	})

	// incremental DDL: implicit create, createIndexes, dropIndexes, drop
	if _, err := src.Database(ddbDB).Collection("ddl_new").
		InsertOne(ctx, bson.M{"_id": 1, "note": "created by DDL"}); err != nil {
		t.Fatalf("ddl create: %v", err)
	}
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "score", Value: 1}}, Options: options.Index().SetName("score_1"),
	}); err != nil {
		t.Fatalf("ddl createIndex: %v", err)
	}
	if _, err := coll.Indexes().DropOne(ctx, "uid_1"); err != nil {
		t.Fatalf("ddl dropIndex: %v", err)
	}
	if _, err := src.Database(ddbDB).Collection("ddl_doomed").
		InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("ddl doomed create: %v", err)
	}
	if err := src.Database(ddbDB).Collection("ddl_doomed").Drop(ctx); err != nil {
		t.Fatalf("ddl drop: %v", err)
	}
	// collMod: only expireAfterSeconds is portable across DocumentDB engines
	if err := src.Database(ddbDB).RunCommand(ctx, bson.D{
		{Key: "collMod", Value: ddbColl},
		{Key: "index", Value: bson.D{{Key: "name", Value: "ttl_1"},
			{Key: "expireAfterSeconds", Value: 7200}}},
	}).Err(); err != nil {
		t.Fatalf("ddl collMod: %v", err)
	}
	// captured before the parity wait: the checkpoint must reach past this point,
	// which is what a stalled applyOps replay can never do
	ddlTs := latestOplogTs(t, src)

	ddbWait(t, p, "incremental DDL to reach DocumentDB", 3*time.Minute, func() bool {
		names := ddbCollections(t, tgt)
		var hasNew, hasDoomed bool
		for _, n := range names {
			if n == "ddl_new" {
				hasNew = true
			}
			if n == "ddl_doomed" {
				hasDoomed = true
			}
		}
		if !hasNew || hasDoomed {
			return false
		}
		idx := ddbIndexNames(t, tgt, ddbColl)
		var hasScore, hasUID bool
		for _, n := range idx {
			if n == "score_1" {
				hasScore = true
			}
			if n == "uid_1" {
				hasUID = true
			}
		}
		return hasScore && !hasUID
	})

	// collMod must have changed the TTL on the target, not just been accepted
	ddbWait(t, p, "collMod to reach the target index", 2*time.Minute, func() bool {
		cur, err := tgt.Database(ddbDB).Collection(ddbColl).Indexes().List(ctx)
		if err != nil {
			return false
		}
		var indexes []bson.M
		if err := cur.All(ctx, &indexes); err != nil {
			return false
		}
		for _, idx := range indexes {
			if idx["name"] == "ttl_1" {
				return fmt.Sprintf("%v", idx["expireAfterSeconds"]) == "7200"
			}
		}
		return false
	})

	// The stall in #1041 was invisible in the data: the DDL simply never applied
	// and the checkpoint froze. Assert the checkpoint reached past the DDL. The
	// checkpoint document is keyed by replset name, not by the collector id.
	wantCkpt := int64(ddlTs.T)<<32 | int64(ddlTs.I)
	ddbWait(t, p, "checkpoint to advance past the DDL", 2*time.Minute, func() bool {
		var ck bson.M
		err := src.Database("mongoshake").Collection(ddbCkpt).
			FindOne(ctx, bson.M{}).Decode(&ck)
		if err != nil {
			return false
		}
		ts, ok := ck["ckpt"].(int64)
		return ok && ts >= wantCkpt
	})

	// parity, and the log must show the native path rather than applyOps
	if got, want := ddbCount(t, tgt, ddbColl), ddbCount(t, src, ddbColl); got != want {
		p.dumpLog(t)
		t.Fatalf("document count mismatch: target %d, source %d", got, want)
	}
	srcIdx, tgtIdx := ddbIndexNames(t, src, ddbColl), ddbIndexNames(t, tgt, ddbColl)
	if strings.Join(srcIdx, ",") != strings.Join(tgtIdx, ",") {
		p.dumpLog(t)
		t.Fatalf("index mismatch: source %v, target %v", srcIdx, tgtIdx)
	}
	body := p.log()
	if !strings.Contains(body, "run native create on DocumentDB target") {
		p.dumpLog(t)
		t.Fatal("expected the native DDL path to be used for create")
	}
	if strings.Contains(body, "Unknown command recieved: applyOps") ||
		strings.Contains(body, "run applyOps for create") {
		p.dumpLog(t)
		t.Fatal("DDL was still wrapped in applyOps, which DocumentDB does not implement")
	}
	if strings.Contains(body, "[CRITICAL]") || strings.Contains(body, "[ERROR]") {
		p.dumpLog(t)
		t.Fatal("collector logged errors during a run that should be clean")
	}
}

// TestDocumentDBDDLStallsWithoutFlag is the red anchor for #1041. With the flag
// off, DDL still goes through admin.applyOps and DocumentDB rejects it, so the
// collection created by DDL never appears. This is what the flag fixes; if a
// refactor removes the native path, the green test alone could still pass against
// a MongoDB target, but this one will not.
func TestDocumentDBDDLStallsWithoutFlag(t *testing.T) {
	tgtURL := ddbTargetURL(t)
	srcURL := ddbEnvOr("MSHAKE_DOCDB_SRC_URL", "mongodb://127.0.0.1:27030")

	src := ddbDial(t, srcURL)
	defer src.Disconnect(context.Background())
	tgt := ddbDial(t, tgtURL)
	defer tgt.Disconnect(context.Background())

	ddbReset(t, src, tgt)
	ddbSeed(t, src)

	logDir := t.TempDir()
	confPath := ddbWriteConf(t, "ddb_it_red", srcURL, tgtURL, logDir, "")
	p := ddbStartCollector(t, confPath, logDir)
	defer p.kill()

	ctx := context.Background()
	ddbWait(t, p, "full sync before DDL", 3*time.Minute, func() bool {
		return ddbCount(t, tgt, ddbColl) == 40
	})

	// Land one DML write first. Issuing DDL while its oplog timestamp is still
	// behind fullSyncFinishPosition trips an unrelated guard in the batcher, which
	// would mask the applyOps failure this test is here to observe.
	if _, err := src.Database(ddbDB).Collection(ddbColl).
		InsertOne(ctx, bson.M{"_id": 2000, "uid": "barrier"}); err != nil {
		t.Fatalf("barrier insert: %v", err)
	}
	ddbWait(t, p, "incremental stage to be live", 2*time.Minute, func() bool {
		return tgt.Database(ddbDB).Collection(ddbColl).
			FindOne(ctx, bson.M{"_id": 2000}).Err() == nil
	})

	if _, err := src.Database(ddbDB).Collection("ddl_new").
		InsertOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatalf("ddl create: %v", err)
	}

	ddbWait(t, p, "applyOps rejection to be logged", 2*time.Minute, func() bool {
		return strings.Contains(p.log(), "applyOps")
	})

	// give the replay a chance to (wrongly) succeed before asserting it did not
	time.Sleep(10 * time.Second)
	for _, n := range ddbCollections(t, tgt) {
		if n == "ddl_new" {
			p.dumpLog(t)
			t.Fatal("DDL applied without the flag; this target no longer reproduces #1041, " +
				"so the red anchor needs a target that rejects applyOps")
		}
	}
}
