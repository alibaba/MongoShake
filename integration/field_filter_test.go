//go:build integration

// End-to-end verification for field-level (top-level) whitelist sync:
//   - full sync projects documents to {_id + whitelisted top-level fields}
//   - full sync only creates indexes fully covered by the whitelist
//   - incremental (change stream) propagates whitelisted field updates,
//     ignores non-whitelisted updates, projects inserts, refreshes a
//     whitelisted object's sub-path, and deletes
//
// Run with:
//
//	go test -tags integration ./integration -run TestFieldFilterSync -v -timeout 10m
//
// Environment (overridable): MSHAKE_CS_SRC_URL (replica set), MSHAKE_TGT_URL,
// MSHAKE_COLLECTOR_BIN. See AGENTS.md for the local docker topology.
package integration

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	ffDB     = "ff_it"
	ffColl   = "c1"
	ffCkptDB = "mongoshake"
	ffCkpt   = "ckpt_ff_it"
	ffTO     = 120 * time.Second
	ffPoll   = 500 * time.Millisecond
)

func ffWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(csRepoRoot(t), "conf", "collector.conf"))
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
	set("sync_mode", "all")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("tunnel", "direct")
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("filter.namespace.white", ffDB+"."+ffColl)
	set("full_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("incr_sync.field.whitelist", ffDB+"."+ffColl+":a,profile")
	set("full_sync.collection_exist_drop", "true")
	set("full_sync.create_index", "foreground")
	set("checkpoint.storage.collection", ffCkpt)
	set("checkpoint.interval", "1000")
	set("full_sync.http_port", "19311")
	set("incr_sync.http_port", "19310")
	set("prom.http_port", "19312")
	set("system_profile_port", "19410")
	set("log.dir", logDir)
	set("log.file", "collector-cs986.log") // reuse csCollectorProc.dumpLog
	set("tunnel.kafka.producer.max_message_bytes", "18874368") // see issue #998

	path := filepath.Join(logDir, "collector-ff.conf")
	if err := os.WriteFile(path, []byte(confStr), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
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
	srcURL := csEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := csEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src := csDial(t, srcURL)
	tgt := csDial(t, tgtURL)
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

	// seed: one whitelisted scalar (a), one whitelisted object (profile), one secret
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10, "secret": "x",
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// indexes: covered (a_1), non-covered single (secret_1), non-covered compound (a_1_secret_1)
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "a", Value: 1}}})
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "secret", Value: 1}}})
	_, _ = srcColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "a", Value: 1}, {Key: "secret", Value: 1}}})

	bin := csBuildCollector(t)
	logDir := t.TempDir()
	confPath := ffWriteConf(t, srcURL, tgtURL, logDir)
	proc := csStartCollector(t, bin, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// 1. full sync projects the document
	got, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool {
		_, hasSecret := d["secret"]
		_, hasA := d["a"]
		_, hasProfile := d["profile"]
		return hasA && hasProfile && !hasSecret
	}, ffTO)
	if !ok {
		t.Fatalf("full sync did not project document within %v, got %v", ffTO, got)
	}
	if p, ok := got["profile"].(bson.M); !ok || p["city"] != "hz" || p["phone"] != "1" {
		t.Fatalf("whole profile sub-tree should be retained, got %v", got)
	}

	// 2. full sync index filtering
	deadline := time.Now().Add(ffTO)
	var names map[string]bool
	for time.Now().Before(deadline) {
		names = map[string]bool{}
		cur, err := tgtColl.Indexes().List(ctx)
		if err == nil {
			var specs []bson.M
			if cur.All(ctx, &specs) == nil {
				for _, s := range specs {
					if n, ok := s["name"].(string); ok {
						names[n] = true
					}
				}
			}
		}
		if names["a_1"] {
			break
		}
		time.Sleep(ffPoll)
	}
	if !names["a_1"] {
		t.Fatalf("covered index a_1 missing on target, got %v", names)
	}
	if names["secret_1"] || names["a_1_secret_1"] {
		t.Fatalf("non-covered indexes should be skipped, got %v", names)
	}

	// give the change stream a moment to be established after full sync
	time.Sleep(5 * time.Second)

	// 3. incremental: update whitelisted field -> propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"a": 99}}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 1, func(d bson.M) bool { return d["a"] == 99 }, ffTO); !ok {
		t.Fatalf("whitelisted field update did not propagate")
	}

	// 4. incremental: update non-whitelisted field -> target unchanged (still no secret)
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

	// 5. incremental: update sub-path of whitelisted object -> propagates
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

	// 6. incremental: insert new doc -> projected
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 2, "a": 5, "secret": "z"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, ok := ffWaitDoc(t, tgt, 2, func(d bson.M) bool {
		_, hasSecret := d["secret"]
		return d["a"] == 5 && !hasSecret
	}, ffTO); !ok {
		t.Fatalf("inserted doc not projected on target")
	}

	// 7. incremental: delete -> removed on target
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
