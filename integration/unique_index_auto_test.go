//go:build integration

// End-to-end verification for #1015: with shard_key=auto, a namespace that
// carries a unique index besides _id must replay in order, so a document is
// never lost to a reordered replay.
//
// The hazard needs the target to apply the "insert a unique key value" before
// the update that released it. The scenario makes that ordering deterministic
// instead of hoping for a race: the collector runs with two workers, the update
// and the blocking inserts all hash to worker 0, and the second insert hashes
// to worker 1. Worker 0 therefore still has a backlog when worker 1 applies the
// insert, whose unique key is still held by the not-yet-updated document.
//
//	t1 the source moves {a: 2r} to {a: 2r+1}, then inserts a second document {a: 2r}
//	t2 between them the source writes uniqFillerCount documents whose _id keeps
//	   worker 0 busy
//	t3 worker 1 replays the insert while worker 0 has not applied the update yet,
//	   so the target answers E11000 and the default dup_key_strategy=ignore drops
//	   the document
//	t4 with shard_key=auto the whole namespace is on one worker and the source
//	   order is preserved: both documents have to reach the target
//
// The second test covers a unique index created after the collector started: the
// createIndexes oplog must move the namespace to ordered replay at the DDL
// barrier. Before the fix shard_key=auto decided once at startup, so a unique
// index created later left the namespace on the _id dispatch.
//
// Run with:
//
//	go test -tags integration ./integration -run TestAutoUniqueIndex -v -timeout 15m
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
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	uniqDB   = "uniq1015"
	uniqColl = "orders"
	uniqCkpt = "ckpt_uniq1015_it"

	// worker 0 must still be draining these when the racing insert is replayed
	uniqFillerCount = 200
	uniqRounds      = 3
)

func writeUniqueConf(t *testing.T, srcURL, tgtURL, logDir string, ddlEnable bool) string {
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
	set("id", "uniq1015_it")
	set("sync_mode", "incr")
	set("mongo_urls", srcURL)
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("incr_sync.mongo_fetch_method", "oplog")
	// MSHAKE_UNIQ_SHARD_KEY=id reproduces the original data loss: the _id
	// dispatch replays the insert before the update that released its key
	set("incr_sync.shard_key", envOr("MSHAKE_UNIQ_SHARD_KEY", "auto"))
	set("incr_sync.worker", "2")
	set("incr_sync.executor", "1")
	set("filter.ddl_enable", fmt.Sprintf("%t", ddlEnable))
	set("incr_sync.executor.dup_key_strategy", "ignore")
	set("incr_sync.executor.insert_on_dup_update", "false")
	set("checkpoint.storage.collection", uniqCkpt)
	set("checkpoint.interval", "1000")
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19121")
	set("incr_sync.http_port", "19120")
	set("prom.http_port", "19122")
	set("system_profile_port", "19220")
	set("log.dir", logDir)
	set("log.file", "collector-it.log")
	set("tunnel.kafka.producer.max_message_bytes", "18874368")

	path := filepath.Join(logDir, "collector-uniq.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func uniqueIndexSpec() bson.D {
	return bson.D{{Key: "a", Value: 1}}
}

func createUniqueIndex(t *testing.T, client *mongo.Client, db, coll, name string) {
	t.Helper()
	_, err := client.Database(db).Collection(coll).Indexes().CreateOne(context.Background(),
		mongo.IndexModel{Keys: uniqueIndexSpec(), Options: options.Index().SetUnique(true).SetName(name)})
	if err != nil {
		t.Fatalf("create unique index %s on %s.%s: %v", name, db, coll, err)
	}
}

func waitForUniqueIndex(t *testing.T, client *mongo.Client, db, coll, name string, timeout time.Duration) {
	t.Helper()
	waitForIndex(t, client, db, coll, name, true, timeout)
}

// waitForIndex waits for the index to reach the target, optionally requiring
// that the target already enforces it as unique.
func waitForIndex(t *testing.T, client *mongo.Client, db, coll, name string, requireUnique bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cursor, err := client.Database(db).Collection(coll).Indexes().List(context.Background())
		if err == nil {
			for cursor.Next(context.Background()) {
				indexName, _ := cursor.Current.Lookup("name").StringValueOK()
				unique, _ := cursor.Current.Lookup("unique").BooleanOK()
				if indexName == name && (!requireUnique || unique) {
					cursor.Close(context.Background())
					return
				}
			}
			cursor.Close(context.Background())
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("index %s (unique=%t) on %s.%s not found on the target within %v",
		name, requireUnique, db, coll, timeout)
}

// shardKeyUnderTest reports the shard key the run uses, so the red run with
// MSHAKE_UNIQ_SHARD_KEY=id reaches the data assertions instead of stopping at a
// log line that only the auto mode prints.
func shardKeyUnderTest() string {
	return envOr("MSHAKE_UNIQ_SHARD_KEY", "auto")
}

func requireOrderedReplayLog(t *testing.T, logDir string, timeout time.Duration, wants ...string) {
	t.Helper()
	if shardKeyUnderTest() != "auto" {
		t.Logf("shard_key=%s does not switch namespaces, skipping the log assertion", shardKeyUnderTest())
		return
	}
	if !uniqueLogLineContains(t, logDir, timeout, wants...) {
		t.Fatalf("the collector did not log a line containing %q", wants)
	}
}

// uniqueLogLineContains matches the wanted substrings against a single line:
// the startup summary lists every ordered namespace of the whole server, so
// exact matching on the full list would depend on other test databases.
func uniqueLogLineContains(t *testing.T, logDir string, timeout time.Duration, wants ...string) bool {
	t.Helper()
	// log.dir is used as the directory of log.file, and older helpers look for a
	// "logs" subdirectory, so accept both layouts
	paths := []string{
		filepath.Join(logDir, "collector-it.log"),
		filepath.Join(logDir, "logs", "collector-it.log"),
	}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			last = string(data)
			for _, line := range strings.Split(last, "\n") {
				matched := true
				for _, want := range wants {
					if !strings.Contains(line, want) {
						matched = false
						break
					}
				}
				if matched {
					return true
				}
			}
		}
		time.Sleep(pollPeriod)
	}
	if last != "" {
		lines := strings.Split(last, "\n")
		if len(lines) > 40 {
			lines = lines[len(lines)-40:]
		}
		t.Logf("collector log tail while looking for %q:\n%s", wants, strings.Join(lines, "\n"))
	} else {
		t.Logf("collector log is empty while looking for %q", wants)
	}
	return false
}

// runUniqueHazard replays the issue's sequence with a deterministic backlog on
// worker 0. Every round owns a disjoint range of document keys and values.
func runUniqueHazard(t *testing.T, coll *mongo.Collection, round int) {
	t.Helper()
	ctx := context.Background()

	// even ids land on worker 0, odd ids on worker 1 (PrimaryKeyHasher, mod 2)
	moved := int64(2 * round)       // the document that moves off the key
	held := int64(100001 + 2*round) // odd: worker 1
	base := int64(1_000_000 * (round + 1))

	if _, err := coll.InsertOne(ctx, bson.D{
		{Key: "_id", Value: moved},
		{Key: "a", Value: base + 1},
		{Key: "b", Value: 1},
	}); err != nil {
		t.Fatalf("insert the moving document: %v", err)
	}

	fillers := make([]interface{}, 0, uniqFillerCount)
	for i := 0; i < uniqFillerCount; i++ {
		fillers = append(fillers, bson.D{
			{Key: "_id", Value: int64(1_000_000 + 2*(round*uniqFillerCount+i))},
			{Key: "a", Value: base + 1000 + int64(i)},
		})
	}
	if _, err := coll.InsertMany(ctx, fillers); err != nil {
		t.Fatalf("insert the worker-0 backlog: %v", err)
	}

	// the update releases base+1 and has to be replayed before the insert below
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: moved}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: base + 2}}}}); err != nil {
		t.Fatalf("move the unique key: %v", err)
	}
	if _, err := coll.InsertOne(ctx, bson.D{
		{Key: "_id", Value: held},
		{Key: "a", Value: base + 1},
		{Key: "b", Value: 2},
	}); err != nil {
		t.Fatalf("insert the document that takes over the unique key: %v", err)
	}
}

func assertHazardResult(t *testing.T, tgt *mongo.Client, rounds int) {
	t.Helper()
	ctx := context.Background()
	for round := 0; round < rounds; round++ {
		moved := int64(2 * round)
		held := int64(100001 + 2*round)
		base := int64(1_000_000 * (round + 1))

		waitDocIn(t, tgt, uniqDB, uniqColl, moved, syncTO)

		// the update sits at the end of worker 0's backlog, so wait for it
		deadline := time.Now().Add(30 * time.Second)
		var got int64
		for time.Now().Before(deadline) {
			var movedDoc bson.M
			if err := tgt.Database(uniqDB).Collection(uniqColl).
				FindOne(ctx, bson.M{"_id": moved}).Decode(&movedDoc); err == nil {
				got = toInt64(t, movedDoc["a"])
				if got == base+2 {
					break
				}
			}
			time.Sleep(pollPeriod)
		}
		if got != base+2 {
			t.Fatalf("round %d: the moved document holds a=%d, want %d", round, got, base+2)
		}

		// a document missing here is the reported data loss: its insert hit the
		// unique key of a document the replay had not updated yet
		deadline = time.Now().Add(20 * time.Second)
		found := false
		for time.Now().Before(deadline) {
			n, err := tgt.Database(uniqDB).Collection(uniqColl).
				CountDocuments(ctx, bson.M{"_id": held})
			if err == nil && n > 0 {
				found = true
				break
			}
			time.Sleep(pollPeriod)
		}
		if !found {
			t.Fatalf("round %d: document _id=%d (a=%d) never reached the target: its insert was "+
				"dropped because a unique key was released after it was replayed", round, held, base+1)
		}
	}
}

func toInt64(t *testing.T, value interface{}) int64 {
	t.Helper()
	switch v := value.(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	t.Fatalf("not an integer: %T %v", value, value)
	return 0
}

// TestAutoUniqueIndexOrderedReplayAtStartup: the index exists before the run,
// so the startup scan has to put the namespace on ordered replay.
func TestAutoUniqueIndexOrderedReplayAtStartup(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	if err := src.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	if err := src.Database(ckptDB).Collection(uniqCkpt).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint collection: %v", err)
	}
	// the unique index must exist on both sides: only the source side matters
	// for the routing decision, the target side turns the misplaced insert into
	// the E11000 that drops the document
	createUniqueIndex(t, src, uniqDB, uniqColl, "a_1")
	createUniqueIndex(t, tgt, uniqDB, uniqColl, "a_1")

	logDir := t.TempDir()
	confPath := writeUniqueConf(t, srcURL, tgtURL, logDir, false)

	stopHb := startHeartbeat(t, src.Database(uniqDB).Collection("hb"), fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	waitUniqueCkpt(t, src, uniqCkpt, startupTO)

	requireOrderedReplayLog(t, logDir, 30*time.Second, "replay in order", "uniq1015.orders")

	for round := 0; round < uniqRounds; round++ {
		runUniqueHazard(t, src.Database(uniqDB).Collection(uniqColl), round)
	}
	assertHazardResult(t, tgt, uniqRounds)
	t.Logf("every document of %d hazard rounds reached the target in order", uniqRounds)
}

// TestAutoUniqueIndexCreatedAtRuntime: no unique index at startup, one created
// while the collector runs. The createIndexes oplog has to move the namespace
// to ordered replay at the DDL barrier.
func TestAutoUniqueIndexCreatedAtRuntime(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	if err := src.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	if err := src.Database(ckptDB).Collection(uniqCkpt).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint collection: %v", err)
	}
	logDir := t.TempDir()
	confPath := writeUniqueConf(t, srcURL, tgtURL, logDir, true)

	stopHb := startHeartbeat(t, src.Database(uniqDB).Collection("hb"), fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	waitUniqueCkpt(t, src, uniqCkpt, startupTO)

	// everything below happens after checkpoint.start_position is fixed, so it
	// cannot be skipped: the reader queries ts > (startSec, 0), and a write from
	// an earlier second than the start position is never replayed
	if _, err := src.Database(uniqDB).Collection(uniqColl).InsertOne(ctx, bson.D{
		{Key: "_id", Value: int64(1)},
		{Key: "a", Value: int64(1)},
	}); err != nil {
		t.Fatalf("seed source collection: %v", err)
	}
	waitDocIn(t, tgt, uniqDB, uniqColl, int64(1), syncTO)

	createUniqueIndex(t, src, uniqDB, uniqColl, "a_1")
	// the index has to reach the target before the hazard runs, otherwise the
	// target accepts the misplaced insert and the scenario proves nothing
	waitForUniqueIndex(t, tgt, uniqDB, uniqColl, "a_1", syncTO)

	for round := 0; round < uniqRounds; round++ {
		runUniqueHazard(t, src.Database(uniqDB).Collection(uniqColl), round)
	}
	assertHazardResult(t, tgt, uniqRounds)

	requireOrderedReplayLog(t, logDir, 30*time.Second, "switched to ordered replay", "uniq1015.orders")
	t.Logf("the namespace created without a unique index kept every document after the DDL switch")
}

// TestAutoUniqueIndexOnEmptyCollection: an index built on an empty collection is
// recorded with its only specification inlined in the oplog instead of an
// "indexes" array, which is the shape an application hits when it creates a
// collection, indexes it and only then starts writing.
func TestAutoUniqueIndexOnEmptyCollection(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	if err := src.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	if err := src.Database(ckptDB).Collection(uniqCkpt).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint collection: %v", err)
	}
	logDir := t.TempDir()
	confPath := writeUniqueConf(t, srcURL, tgtURL, logDir, true)

	stopHb := startHeartbeat(t, src.Database(uniqDB).Collection("hb"), fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	waitUniqueCkpt(t, src, uniqCkpt, startupTO)

	// the collection has to exist and stay empty when the index is built, and
	// both have to happen after checkpoint.start_position is fixed, or the
	// reader would never replay them
	if err := src.Database(uniqDB).CreateCollection(ctx, uniqColl); err != nil {
		t.Fatalf("create an empty source collection: %v", err)
	}
	createUniqueIndex(t, src, uniqDB, uniqColl, "a_1")
	waitForUniqueIndex(t, tgt, uniqDB, uniqColl, "a_1", syncTO)

	for round := 0; round < uniqRounds; round++ {
		runUniqueHazard(t, src.Database(uniqDB).Collection(uniqColl), round)
	}
	assertHazardResult(t, tgt, uniqRounds)

	requireOrderedReplayLog(t, logDir, 30*time.Second, "switched to ordered replay", "uniq1015.orders")
	t.Logf("the namespace indexed while empty kept every document")
}

// TestAutoUniqueIndexConvertedByCollMod: MongoDB 7.0 can turn an existing index
// into a unique one, which must also switch the namespace.
func TestAutoUniqueIndexConvertedByCollMod(t *testing.T) {
	srcURL := envOr("MSHAKE_CS_URL", "mongodb://127.0.0.1:27030")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	src := dial(t, srcURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()
	defer func() {
		_ = src.Disconnect(context.Background())
		_ = tgt.Disconnect(context.Background())
	}()

	if !serverVersionAtLeast(t, src, 7, 0) {
		t.Skip("collMod index conversion requires MongoDB 7.0 or newer")
	}

	if err := src.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := tgt.Database(uniqDB).Drop(ctx); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	if err := src.Database(ckptDB).Collection(uniqCkpt).Drop(ctx); err != nil {
		t.Fatalf("drop checkpoint collection: %v", err)
	}
	logDir := t.TempDir()
	confPath := writeUniqueConf(t, srcURL, tgtURL, logDir, true)

	stopHb := startHeartbeat(t, src.Database(uniqDB).Collection("hb"), fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Cleanup(stopHb)

	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	waitUniqueCkpt(t, src, uniqCkpt, startupTO)

	// seed and the plain index must be replayed before the conversion, and both
	// have to happen after checkpoint.start_position is fixed, or the reader
	// would never replay them
	if _, err := src.Database(uniqDB).Collection(uniqColl).InsertOne(ctx, bson.D{
		{Key: "_id", Value: int64(1)},
		{Key: "a", Value: int64(1)},
	}); err != nil {
		t.Fatalf("seed source collection: %v", err)
	}
	if _, err := src.Database(uniqDB).Collection(uniqColl).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: uniqueIndexSpec(), Options: options.Index().SetName("a_1")}); err != nil {
		t.Fatalf("create the plain index: %v", err)
	}
	waitDocIn(t, tgt, uniqDB, uniqColl, int64(1), syncTO)
	waitForIndex(t, tgt, uniqDB, uniqColl, "a_1", false, syncTO)

	for _, index := range []bson.D{
		{{Key: "keyPattern", Value: uniqueIndexSpec()}, {Key: "prepareUnique", Value: true}},
		{{Key: "keyPattern", Value: uniqueIndexSpec()}, {Key: "unique", Value: true}},
	} {
		if err := src.Database(uniqDB).RunCommand(ctx, bson.D{
			{Key: "collMod", Value: uniqColl},
			{Key: "index", Value: index},
		}).Err(); err != nil {
			t.Fatalf("collMod %v: %v", index, err)
		}
	}
	waitForUniqueIndex(t, tgt, uniqDB, uniqColl, "a_1", syncTO)

	for round := 0; round < uniqRounds; round++ {
		runUniqueHazard(t, src.Database(uniqDB).Collection(uniqColl), round)
	}
	assertHazardResult(t, tgt, uniqRounds)

	requireOrderedReplayLog(t, logDir, 30*time.Second, "switched to ordered replay", "uniq1015.orders")
	t.Logf("the index converted by collMod switched the namespace to ordered replay")
}

// serverVersionAtLeast parses the major.minor of buildInfo.version.
func serverVersionAtLeast(t *testing.T, client *mongo.Client, major, minor int) bool {
	t.Helper()
	var buildInfo bson.M
	if err := client.Database("admin").RunCommand(context.Background(),
		bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		t.Fatalf("read buildInfo: %v", err)
	}
	version, _ := buildInfo["version"].(string)
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		t.Fatalf("unexpected server version %q", version)
	}
	serverMajor, err := strconv.Atoi(parts[0])
	if err != nil {
		t.Fatalf("unexpected server version %q: %v", version, err)
	}
	serverMinor, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("unexpected server version %q: %v", version, err)
	}
	return serverMajor > major || (serverMajor == major && serverMinor >= minor)
}

// waitUniqueCkpt waits for the checkpoint document of a dedicated
// checkpoint collection.
func waitUniqueCkpt(t *testing.T, src *mongo.Client, collection string, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var doc bson.M
		err := src.Database(ckptDB).Collection(collection).FindOne(context.Background(), bson.M{}).Decode(&doc)
		if err == nil {
			if v, ok := doc["ckpt"].(int64); ok {
				return v
			}
		}
		time.Sleep(pollPeriod)
	}
	t.Fatalf("checkpoint document of %s not created within %v", collection, timeout)
	return 0
}
