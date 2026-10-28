//go:build integration

// End-to-end verification for #1030: an incremental bulk insert whose write
// errors mix E11000 with a non-duplicate error (here: a target-side document
// validation failure, code 121) must not be acknowledged as a whole.
//
// A batch has to carry BOTH kinds of error at once, which is why the test
// writes many documents instead of two: `mongo.IsDuplicateKeyError` only
// reports that SOME write error is a duplicate, so one duplicate is enough to
// send a whole mixed batch down the duplicate-key path (ignore/convert).
//
// Scenario:
//
//	t1 the target collection carries a validator the source does not have, and
//	   already holds every "duplicate" _id
//	t2 the source inserts interleaved pairs — {_id: 1xxx, level: 1} which is a
//	   duplicate on the target, and {_id: 2xxx, level: -1} which the target
//	   validator rejects — so every executor batch mixes both errors
//	t3 the batch must keep failing: while the validator is active the
//	   checkpoint stays below the batch, so the rejected inserts are still
//	   replayable
//	t4 the validator is removed, lifting the failure condition
//	t5 the retried batches must apply every rejected document with its full
//	   contents, and the checkpoint must then pass the fence it was held below
//
// Run with:
//
//	go test -tags integration ./integration -run TestIncrMixedDupInsertNotAcked -v -timeout 10m
//
// Environment (overridable):
//
//	MSHAKE_CS_URL  replica-set source (default mongodb://127.0.0.1:27030)
//	MSHAKE_TGT_URL target mongodb (default mongodb://127.0.0.1:27019)
package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mixedDupDB       = "mixeddup1030"
	mixedDupColl     = "c1"
	mixedDupCkptColl = "ckpt_mixeddup1030_it"

	// doc pairs per side: 100 duplicated ids + 100 rejected ids
	mixedDupPairs = 100
	// small buffers keep the batches short, so every batch holds both kinds of
	// failing write; a single worker keeps the stall easy to reason about
	mixedDupFetcherCapacity = "16"
)

// writeMixedDupConf renders collector.conf for an incremental-only run whose
// duplicate-key handling is exactly the one the issue reports: ignore
// duplicates, no insert-on-dup-update conversion.
func writeMixedDupConf(t *testing.T, srcURL, tgtURL, logDir string) string {
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
	set("id", "mixeddup1030_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("incr_sync.mongo_fetch_method", "oplog")
	set("incr_sync.executor.dup_key_strategy", "ignore")
	set("incr_sync.executor.insert_on_dup_update", "false")
	set("incr_sync.worker", "1")
	set("incr_sync.fetcher.buffer_capacity", mixedDupFetcherCapacity)
	set("checkpoint.storage.collection", mixedDupCkptColl)
	set("checkpoint.interval", "1000")
	// start from now: the source keeps oplog entries from earlier runs, and
	// replaying them would only slow the test down
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	// ports must not collide with the other integration tests
	set("full_sync.http_port", "19111")
	set("incr_sync.http_port", "19110")
	set("prom.http_port", "19112")
	set("system_profile_port", "19210")
	set("log.dir", logDir)
	set("log.file", "collector-it.log")
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-mixeddup.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func readMixedCkpt(t *testing.T, src *mongo.Client) int64 {
	t.Helper()
	var doc bson.M
	err := src.Database(ckptDB).Collection(mixedDupCkptColl).
		FindOne(context.Background(), bson.M{}).Decode(&doc)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	v, ok := doc["ckpt"].(int64)
	if !ok {
		t.Fatalf("checkpoint doc has no int64 ckpt field: %v", doc)
	}
	return v
}

func waitMixedCkpt(t *testing.T, src *mongo.Client, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var doc bson.M
		err := src.Database(ckptDB).Collection(mixedDupCkptColl).
			FindOne(context.Background(), bson.M{}).Decode(&doc)
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

func waitMixedCkptAtLeast(t *testing.T, src *mongo.Client, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		last = readMixedCkpt(t, src)
		if last >= want {
			return
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("checkpoint stuck at %x, never reached fence %x within %v", last, want, timeout)
}

// oplogInsertTs returns the packed ts of the insert oplog entry for id.
func oplogInsertTs(t *testing.T, src *mongo.Client, db, coll string, id interface{}) int64 {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		var doc bson.M
		err := src.Database("local").Collection("oplog.rs").FindOne(ctx,
			bson.M{"op": "i", "ns": db + "." + coll, "o._id": id},
			options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}),
		).Decode(&doc)
		if err == nil {
			if ts, ok := doc["ts"].(primitive.Timestamp); ok {
				return packed(ts)
			}
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("no insert oplog entry for %v in %s.%s within %v", id, db, coll, syncTO)
	return 0
}

func TestIncrMixedDupInsertNotAcked(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	// source must NOT carry the validator: level:-1 has to be inserted there
	// successfully so it produces an oplog entry (this is the injected fault)
	if err := src.Database(mixedDupDB).Collection(mixedDupColl).Drop(ctx); err != nil {
		t.Fatalf("drop source collection: %v", err)
	}
	// heartbeat leftovers from earlier runs only add noise
	if err := src.Database(mixedDupDB).Collection("hb").Drop(ctx); err != nil {
		t.Fatalf("drop heartbeat collection: %v", err)
	}
	// target rejects level < 0 and already holds every duplicated _id
	if err := tgt.Database(mixedDupDB).Collection(mixedDupColl).Drop(ctx); err != nil {
		t.Fatalf("drop target collection: %v", err)
	}
	validator := bson.D{{Key: "level", Value: bson.D{{Key: "$gte", Value: 0}}}}
	if err := tgt.Database(mixedDupDB).CreateCollection(ctx, mixedDupColl,
		options.CreateCollection().SetValidator(validator)); err != nil {
		t.Fatalf("create target collection with validator: %v", err)
	}
	seeds := make([]interface{}, 0, mixedDupPairs)
	for i := 0; i < mixedDupPairs; i++ {
		seeds = append(seeds, bson.D{
			{Key: "_id", Value: dupID(i)}, {Key: "level", Value: 1}})
	}
	if _, err := tgt.Database(mixedDupDB).Collection(mixedDupColl).InsertMany(ctx, seeds); err != nil {
		t.Fatalf("seed target duplicates: %v", err)
	}
	if err := src.Database(ckptDB).Collection(mixedDupCkptColl).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint collection: %v", err)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	logDir := t.TempDir()
	confPath := writeMixedDupConf(t, srcURL, tgtURL, logDir)

	// heartbeat keeps the oplog (and therefore the checkpoint) moving
	stopHb := startHeartbeat(t, src.Database(mixedDupDB).Collection("hb"), runID)
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	ckpt0 := waitMixedCkpt(t, src, startupTO)
	t.Logf("initial checkpoint: %x", ckpt0)

	// interleaved pairs: every executor batch carries both a duplicate key and
	// a document that the target validator rejects
	docs := make([]interface{}, 0, mixedDupPairs*2)
	for i := 0; i < mixedDupPairs; i++ {
		docs = append(docs,
			bson.D{{Key: "_id", Value: dupID(i)}, {Key: "level", Value: 1}},
			bson.D{{Key: "_id", Value: rejectedID(i)}, {Key: "level", Value: -1}},
		)
	}
	if _, err := src.Database(mixedDupDB).Collection(mixedDupColl).InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert source pairs: %v", err)
	}
	fence := oplogInsertTs(t, src, mixedDupDB, mixedDupColl, rejectedID(mixedDupPairs-1))
	t.Logf("failing fence (ts of the last rejected insert): %x", fence)

	// let the batches run and several checkpoint intervals pass
	time.Sleep(20 * time.Second)

	// the rejection itself is correct while the validator is in place
	if n, err := tgt.Database(mixedDupDB).Collection(mixedDupColl).
		CountDocuments(ctx, bson.M{"level": bson.M{"$lt": 0}}); err != nil {
		t.Fatalf("count rejected docs: %v", err)
	} else if n != 0 {
		t.Fatalf("target must reject level<0 documents while the validator is active, found %d", n)
	}

	// core assertion 1: a mixed batch must not be acknowledged, so the
	// checkpoint has to stay below it and the rejected positions stay replayable
	ckpt := readMixedCkpt(t, src)
	t.Logf("checkpoint while the batches fail: %x (fence %x)", ckpt, fence)
	if ckpt >= fence {
		t.Fatalf("checkpoint %x advanced past the rejected inserts %x: replaying from here "+
			"would never write them even after the validator is fixed", ckpt, fence)
	}

	// t4: lift the failure condition on the target
	if err := tgt.Database(mixedDupDB).RunCommand(ctx, bson.D{
		{Key: "collMod", Value: mixedDupColl},
		{Key: "validator", Value: bson.D{}},
	}).Err(); err != nil {
		t.Fatalf("remove target validator: %v", err)
	}

	// t5: the retried batches must apply every rejected document, with its
	// contents, and let the checkpoint pass the fence it was held below
	waitDocIn(t, tgt, mixedDupDB, mixedDupColl, rejectedID(mixedDupPairs-1), syncTO)
	var restored bson.M
	if err := tgt.Database(mixedDupDB).Collection(mixedDupColl).
		FindOne(ctx, bson.M{"_id": rejectedID(0)}).Decode(&restored); err != nil {
		t.Fatalf("read restored doc: %v", err)
	}
	if lvl, ok := restored["level"].(int32); !ok || lvl != -1 {
		t.Fatalf("restored doc lost its contents: %v", restored)
	}
	// no rejected document may be missing: that is what an acknowledged
	// checkpoint past a rejected insert would cost
	n, err := tgt.Database(mixedDupDB).Collection(mixedDupColl).
		CountDocuments(ctx, bson.M{"level": bson.M{"$lt": 0}})
	if err != nil {
		t.Fatalf("count restored docs: %v", err)
	}
	if n != mixedDupPairs {
		t.Fatalf("expected %d restored documents after lifting the validation failure, got %d: "+
			"some rejected inserts were acknowledged and skipped", mixedDupPairs, n)
	}
	waitMixedCkptAtLeast(t, src, fence, syncTO)
	t.Logf("mixed batches held below the fence, then fully replayed: PASS")
	fmt.Printf("mixed dup/non-dup batches held at %x then replayed\n", fence)
}

func dupID(i int) interface{}      { return 1000 + i }
func rejectedID(i int) interface{} { return 2000 + i }
