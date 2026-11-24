//go:build integration

// End-to-end verification for the incremental (change stream) field blacklist:
//   - non-blacklisted field update propagates
//   - blacklisted field update is a target no-op (field never appears)
//   - sub-path update of a blacklisted object field is excluded
//   - sub-path update of a non-blacklisted object field propagates
//   - a field added to the source later (not blacklisted) auto-syncs (open world)
//   - insert is projected to {_id + everything except blacklisted fields}
//   - delete removes the target document
//
// Incr-only mode (sync_mode=incr), mirroring TestFieldFilterSync. Full-sync exclusion
// projection and index filtering are covered by the live-mongo docsyncer tests.
//
// Run with:
//
//	go test -tags integration ./integration -run TestFieldBlacklistSync -v -timeout 10m
//
// Self-contained except for the generic helpers shared with field_filter_test.go
// (ffDial / ffEnvOr / ffStartCollector / ffNum). Uses its own database (fb_it) so it
// does not collide with the whitelist test.
//
// Environment (overridable): MSHAKE_CS_SRC_URL / MSHAKE_TGT_URL / MSHAKE_COLLECTOR_BIN.
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
	fbDB      = "fb_it"
	fbColl    = "c1"
	fbCkpt    = "ckpt_fb_it"
	fbLogFile = "collector-fb.log"
)

func fbWriteConf(t *testing.T, srcURL, tgtURL, logDir string) string {
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
	set("id", "fb_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("tunnel", "direct")
	set("incr_sync.mongo_fetch_method", "change_stream")
	set("incr_sync.change_stream.watch_full_document", "false")
	set("filter.namespace.white", fbDB+"."+fbColl)
	set("full_sync.field.blacklist", fbDB+"."+fbColl+":big,blob")
	set("incr_sync.field.blacklist", fbDB+"."+fbColl+":big,blob")
	set("checkpoint.storage.collection", fbCkpt)
	set("checkpoint.interval", "1000")
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19341")
	set("incr_sync.http_port", "19340")
	set("prom.http_port", "19342")
	set("system_profile_port", "19440")
	set("log.dir", logDir)
	set("log.file", fbLogFile)
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-fb.conf")
	if err := os.WriteFile(path, []byte(confStr), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func fbWaitDoc(t *testing.T, tgt *mongo.Client, id interface{},
	pred func(bson.M) bool, timeout time.Duration) (bson.M, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bson.M
	for time.Now().Before(deadline) {
		var got bson.M
		err := tgt.Database(fbDB).Collection(fbColl).
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

func TestFieldBlacklistSync(t *testing.T) {
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

	srcColl := src.Database(fbDB).Collection(fbColl)
	tgtColl := tgt.Database(fbDB).Collection(fbColl)
	_ = srcColl.Drop(ctx)
	_ = tgtColl.Drop(ctx)
	_ = src.Database("mongoshake").Collection(fbCkpt).Drop(ctx)

	// seed source: small scalar (a), small object (profile), and blacklisted big/blob
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10, "big": "huge",
		"blob":    bson.M{"sub": "x"},
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	// incr-only: target pre-populated with the projected (blacklist-excluded) form
	if _, err := tgtColl.InsertOne(ctx, bson.M{"_id": 1, "a": 10,
		"profile": bson.M{"city": "hz", "phone": "1"}}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	logDir := t.TempDir()
	confPath := fbWriteConf(t, srcURL, tgtURL, logDir)
	proc := ffStartCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	time.Sleep(8 * time.Second)

	// 1. non-blacklisted update propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"a": 99}}); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if _, ok := fbWaitDoc(t, tgt, 1, func(d bson.M) bool { return ffNum(d["a"]) == 99 }, ffTO); !ok {
		t.Fatalf("non-blacklisted field update did not propagate")
	}

	// 2. blacklisted update is a target no-op (big never appears)
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"big": "new"}}); err != nil {
		t.Fatalf("update big: %v", err)
	}
	time.Sleep(5 * time.Second)
	var afterBig bson.M
	if err := tgtColl.FindOne(ctx, bson.M{"_id": 1}).Decode(&afterBig); err != nil {
		t.Fatalf("read target: %v", err)
	}
	if _, has := afterBig["big"]; has {
		t.Fatalf("blacklisted field must not appear on target, got %v", afterBig)
	}

	// 2b. sub-path of a blacklisted object field is excluded (blob never appears)
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$set": bson.M{"blob.sub": "y"}}); err != nil {
		t.Fatalf("update blob.sub: %v", err)
	}
	time.Sleep(5 * time.Second)
	var afterBlob bson.M
	if err := tgtColl.FindOne(ctx, bson.M{"_id": 1}).Decode(&afterBlob); err != nil {
		t.Fatalf("read target: %v", err)
	}
	if _, has := afterBlob["blob"]; has {
		t.Fatalf("blacklisted object sub-path must not create the field on target, got %v", afterBlob)
	}

	// 3. sub-path of non-blacklisted object propagates
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$set": bson.M{"profile.city": "sh"}}); err != nil {
		t.Fatalf("update profile.city: %v", err)
	}
	if _, ok := fbWaitDoc(t, tgt, 1, func(d bson.M) bool {
		p, ok := d["profile"].(bson.M)
		return ok && p["city"] == "sh"
	}, ffTO); !ok {
		t.Fatalf("sub-path update of non-blacklisted object did not propagate")
	}

	// 3b. a field added to the source later (not blacklisted) auto-syncs (open world)
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$set": bson.M{"extra": "new"}}); err != nil {
		t.Fatalf("update extra: %v", err)
	}
	if _, ok := fbWaitDoc(t, tgt, 1, func(d bson.M) bool { return d["extra"] == "new" }, ffTO); !ok {
		t.Fatalf("newly added non-blacklisted field did not auto-sync to target")
	}

	// 4. insert new doc -> projected (big excluded)
	if _, err := srcColl.InsertOne(ctx, bson.M{"_id": 2, "a": 5, "big": "x"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, ok := fbWaitDoc(t, tgt, 2, func(d bson.M) bool {
		_, hasBig := d["big"]
		return ffNum(d["a"]) == 5 && !hasBig
	}, ffTO); !ok {
		t.Fatalf("inserted doc not projected on target (blacklisted field leaked or a missing)")
	}

	// 5. delete -> removed on target
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
