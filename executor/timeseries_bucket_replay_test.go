package executor

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

// TestTimeSeriesBucketBinaryColumnReplay replays a real MongoDB 8.x time-series
// bucket diff through MongoShake's writer and checks the measurements come out
// right (#1009).
//
// The fabricated `sdata.b` cases in TestTimeSeriesIntegration only prove routing:
// that ConvertV2Oplog declines the binary column section and the op is handed to
// applyOps instead of being mangled. Their blobs are invented bytes, so the server
// rejects them on every version - 4770503 "Unexpected section: b" below 8.0, where
// `b` is not a diff section at all, and 6067608 "Invalid BSON Column encoding" on
// 8.x. Nothing there ever showed a column-store diff being applied successfully,
// which is the behaviour real 8.x time-series replication depends on: in 8.0's
// timeseries_write_util.cpp the `b` section carries every updated existing column
// of a write batch, so any insert that extends a bucket produces one.
//
// This test closes that gap with a diff the server itself generated. It writes two
// batches into one bucket on the source, lifts the resulting entries straight out
// of local.oplog.rs, decodes them with oplog.ParseRaw (the production entry point,
// so the lazy-decode path is exercised too), rewrites the namespace and feeds them
// to the writer. The target bucket is then compared against the source's, and both
// logical views are compared measurement by measurement.
//
// Requires MongoDB 8.0+ AND a replica set: `b` does not exist before 8.0, and a
// standalone keeps no oplog to capture from. Anything else skips, naming the
// precondition that was unmet. Run against 8.x with:
//
//	docker run -d --name mshake-ts82 --network host mongo:8.2 --replSet ts82 \
//	  --bind_ip 127.0.0.1 --port 27033
//	docker exec mshake-ts82 mongosh --quiet --port 27033 \
//	  --eval 'rs.initiate({_id:"ts82",members:[{_id:0,host:"127.0.0.1:27033"}]})'
//	MONGOSHAKE_TEST_URL="mongodb://127.0.0.1:27033" go test ./executor \
//	  -run TestTimeSeriesBucketBinaryColumnReplay -v -count=1
//
// Note that mongo:8.0 images cannot start on Docker Desktop with a Linux kernel
// 6.19 or newer (SERVER-121912); mongo:8.2 works.
func TestTimeSeriesBucketBinaryColumnReplay(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	const (
		srcDB   = "ts_bsrc"
		tgtDB   = "ts_btgt"
		tsColl  = "weather"
		buckets = "system.buckets." + tsColl
	)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil || conn == nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()
	// Registered after conn.Close so it runs first (defers are LIFO). This server is
	// shared with other tests, so the databases this one creates must not outlive it.
	defer func() {
		_ = conn.Client.Database(srcDB).Drop(nil)
		_ = conn.Client.Database(tgtDB).Drop(nil)
	}()

	// precondition 1: `b` is not a document diff section before 8.0, so there is
	// nothing for this test to capture on an older server. The version is read
	// directly because GetAndCompareVersion reports "below threshold" through its
	// error return rather than a false bool.
	version, err := utils.GetDBVersion(conn)
	if err != nil {
		t.Fatalf("read server version: %v", err)
	}
	major, err := strconv.Atoi(strings.SplitN(version, ".", 2)[0])
	if err != nil {
		t.Fatalf("parse server version %q: %v", version, err)
	}
	if major < 8 {
		t.Skipf("server is MongoDB %s, older than 8.0, where time-series bucket "+
			"diffs carry no binary column ('b') section at all", version)
	}

	// precondition 2: the real diff is lifted out of the oplog, and a standalone
	// keeps none
	var hello bson.M
	if err := conn.Client.Database("admin").RunCommand(nil,
		bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("run hello: %v", err)
	}
	if _, ok := hello["setName"]; !ok {
		t.Skipf("server is not a replica set, so there is no local.oplog.rs to " +
			"capture a real bucket diff from")
	}

	// ---- source: two batches landing in one bucket ----
	if err := conn.Client.Database(srcDB).Drop(nil); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := conn.Client.Database(tgtDB).Drop(nil); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	// The oplog outlives the databases this test drops, so a rerun would otherwise
	// also pick up the previous run's entries for the same namespace and replay
	// stale buckets. Everything captured below must be newer than this point.
	startTs, err := latestOplogTs(conn.Client)
	if err != nil {
		t.Fatalf("read the current oplog position: %v", err)
	}
	if err := createTsCollection(conn.Client, srcDB, tsColl); err != nil {
		t.Skipf("server cannot create time-series collections: %v", err)
	}
	// a fixed base timestamp keeps both batches inside one "hours" bucket with the
	// same metadata, which is what makes the second batch extend the first bucket
	// instead of opening a new one
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	measurement := func(i int) bson.D {
		return bson.D{
			{Key: "timestamp", Value: base.Add(time.Duration(i) * time.Minute)},
			{Key: "metadata", Value: bson.D{{Key: "sensorId", Value: 1}}},
			{Key: "temperature", Value: 20.0 + float64(i)},
		}
	}
	srcColl := conn.Client.Database(srcDB).Collection(tsColl)
	if _, err := srcColl.InsertMany(nil, []interface{}{measurement(0), measurement(1),
		measurement(2)}); err != nil {
		t.Fatalf("insert source batch 1: %v", err)
	}
	if _, err := srcColl.InsertMany(nil, []interface{}{measurement(3),
		measurement(4)}); err != nil {
		t.Fatalf("insert source batch 2: %v", err)
	}

	// ---- capture the bucket ops the server actually wrote ----
	// The bytes are copied out of the cursor: oplog.ParseRaw borrows its input, and
	// the driver reuses the cursor buffer between documents.
	type capturedOp struct {
		raw  []byte
		op   string
		hasB bool
	}
	cur, err := conn.Client.Database("local").Collection("oplog.rs").Find(nil,
		bson.M{"ns": srcDB + "." + buckets, "ts": bson.M{"$gt": startTs}},
		options.Find().SetSort(bson.D{{Key: "ts", Value: 1}}))
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	var captured []capturedOp
	for cur.Next(nil) {
		raw := bson.Raw(append([]byte(nil), cur.Current...))
		op := raw.Lookup("op").StringValue()
		_, lookupErr := raw.LookupErr("o", "diff", "sdata", "b")
		captured = append(captured, capturedOp{raw: raw, op: op, hasB: lookupErr == nil})
	}
	if err := cur.Close(nil); err != nil {
		t.Fatalf("close oplog cursor: %v", err)
	}

	// Positive anchor: without a `b`-carrying update this test would only prove that
	// bucket inserts replay, which is not the gap being closed.
	sawBinaryColumn := false
	for _, c := range captured {
		if c.hasB {
			sawBinaryColumn = true
		}
	}
	if !sawBinaryColumn {
		t.Fatalf("captured %d bucket op(s) from the oplog but none carried a "+
			"diff.sdata.b binary column section, so there is nothing to verify; the "+
			"fixture no longer produces the shape this test is for", len(captured))
	}
	t.Logf("captured %d bucket op(s) from local.oplog.rs, at least one with an "+
		"sdata.b binary column section", len(captured))

	// ---- replay them into a separate target collection ----
	// MongoShake replays the `create` DDL ahead of the data ops when
	// filter.ddl_enable is on, so the target namespace already exists by the time a
	// bucket diff arrives. Recreate that precondition, without which applyOps fails
	// with NamespaceNotFound rather than telling us anything about `b`.
	if err := createTsCollection(conn.Client, tgtDB, tsColl); err != nil {
		t.Fatalf("create target time-series collection: %v", err)
	}
	writer := NewDbWriter(conn, bson.E{}, false, 0)
	for i, c := range captured {
		partialLog, err := oplog.ParseRaw(c.raw)
		if err != nil {
			t.Fatalf("ParseRaw of captured bucket op[%d]: %v", i, err)
		}
		partialLog.Namespace = tgtDB + "." + buckets
		records := []*OplogRecord{{
			original: &PartialLogWithCallback{partialLog: partialLog, callback: func() {}},
			wait:     nil,
		}}

		var writeErr error
		switch partialLog.Operation {
		case "i":
			writeErr = writer.doInsert(tgtDB, buckets, bson.E{}, records, false)
		case "u":
			writeErr = writer.doUpdate(tgtDB, buckets, bson.E{}, records, false)
		default:
			t.Fatalf("unexpected bucket op[%d] type %q", i, partialLog.Operation)
		}
		if writeErr != nil {
			t.Fatalf("replaying captured bucket op[%d] (op=%q, binary column=%v) "+
				"failed: %v", i, c.op, c.hasB, writeErr)
		}
	}

	// ---- the target bucket must match the source's ----
	srcBuckets, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, srcDB, buckets, nil)
	if err != nil {
		t.Fatalf("read source buckets: %v", err)
	}
	tgtBuckets, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, tgtDB, buckets, nil)
	if err != nil {
		t.Fatalf("read target buckets: %v", err)
	}
	if len(srcBuckets) == 0 {
		t.Fatalf("source has no bucket, the fixture did not write anything")
	}
	if len(srcBuckets) != len(tgtBuckets) {
		t.Fatalf("bucket count differs: source %d, target %d", len(srcBuckets), len(tgtBuckets))
	}
	sort.Slice(srcBuckets, func(i, j int) bool {
		return fmt.Sprint(srcBuckets[i]["_id"]) < fmt.Sprint(srcBuckets[j]["_id"])
	})
	sort.Slice(tgtBuckets, func(i, j int) bool {
		return fmt.Sprint(tgtBuckets[i]["_id"]) < fmt.Sprint(tgtBuckets[j]["_id"])
	})
	for i := range srcBuckets {
		if tsbCanonical(srcBuckets[i]) != tsbCanonical(tgtBuckets[i]) {
			t.Errorf("bucket %v differs after replay:\n  src=%s\n  tgt=%s",
				srcBuckets[i]["_id"], tsbCanonical(srcBuckets[i]), tsbCanonical(tgtBuckets[i]))
		}
	}

	// ---- and so must the measurements read back through the logical view ----
	srcDocs, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, srcDB, tsColl, nil)
	if err != nil {
		t.Fatalf("read source measurements: %v", err)
	}
	tgtDocs, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, tgtDB, tsColl, nil)
	if err != nil {
		t.Fatalf("read target measurements: %v", err)
	}
	if len(srcDocs) != 5 {
		t.Fatalf("source fixture holds %d measurements, want 5", len(srcDocs))
	}
	if len(srcDocs) != len(tgtDocs) {
		t.Fatalf("measurement count differs: source %d, target %d", len(srcDocs), len(tgtDocs))
	}
	srcSet := tsbCanonicalSet(srcDocs)
	tgtSet := tsbCanonicalSet(tgtDocs)
	for i := range srcSet {
		if srcSet[i] != tgtSet[i] {
			t.Errorf("measurement %d differs:\n  src=%s\n  tgt=%s", i, srcSet[i], tgtSet[i])
		}
	}
}

// TestSystemViewsApplyOpsReplay replays a real system.views insert through the
// writer and checks the view definition actually lands on the target.
//
// system.views is a protected collection, so the writer routes inserts to it
// through applyOps. That path used to forward the whole ParsedLog, ui included,
// and applyOps rejects a uuid that does not belong to the namespace it is applying
// to - Location3318200 "Namespace ... and UUID ... point to different
// collections" - so the view definition never reached the target and a
// time-series collection created there stayed invisible.
//
// TestTimeSeriesIntegration's system.views case does not catch this because it
// hand-builds a PartialLog with no UI set, while every production decode path
// populates UI: ParseRaw's metadata whitelist, the non-lazy bson.Unmarshal, the
// change stream conversion and the transaction buffer. Asserting UI is present is
// therefore part of the test - without it this would degrade into the same
// hand-built fixture and quietly stop covering the bug.
//
// Needs a replica set, to read the op the server really wrote, and time-series
// support (5.0+) to produce one; otherwise it skips naming the unmet precondition.
func TestSystemViewsApplyOpsReplay(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	const (
		srcDB   = "sv_src"
		tgtDB   = "sv_tgt"
		tsColl  = "weather"
		viewsNS = "system.views"
	)

	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil || conn == nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()
	// Registered after conn.Close so it runs first (defers are LIFO). The replayed
	// view definition keeps the source's _id, so without this the target database is
	// left holding a view whose name does not belong to it - harmless here, but this
	// server is shared with other tests.
	defer func() {
		_ = conn.Client.Database(srcDB).Drop(nil)
		_ = conn.Client.Database(tgtDB).Drop(nil)
	}()

	var hello bson.M
	if err := conn.Client.Database("admin").RunCommand(nil,
		bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Fatalf("run hello: %v", err)
	}
	if _, ok := hello["setName"]; !ok {
		t.Skipf("server is not a replica set, so there is no local.oplog.rs to " +
			"capture a real system.views insert from")
	}

	if err := conn.Client.Database(srcDB).Drop(nil); err != nil {
		t.Fatalf("drop source db: %v", err)
	}
	if err := conn.Client.Database(tgtDB).Drop(nil); err != nil {
		t.Fatalf("drop target db: %v", err)
	}
	startTs, err := latestOplogTs(conn.Client)
	if err != nil {
		t.Fatalf("read the current oplog position: %v", err)
	}

	// creating a time-series collection is what makes the server write the
	// system.views entry this test replays
	if err := createTsCollection(conn.Client, srcDB, tsColl); err != nil {
		t.Skipf("server cannot create time-series collections (needs 5.0+): %v", err)
	}
	// In real replay the `create` DDL runs ahead of the data ops, so the target's
	// system.views already exists by the time this insert arrives. Creating it under
	// the same name also keeps the view catalog valid: the replayed definition keeps
	// the source's _id and viewOn, so it resolves against
	// <tgtDB>.system.buckets.weather. Without that collection the target is left with
	// a dangling view, and a single invalid view definition makes the server reject
	// listCollections for the entire database - which then breaks every other test
	// that enumerates databases on this server.
	if err := createTsCollection(conn.Client, tgtDB, tsColl); err != nil {
		t.Fatalf("create target time-series collection: %v", err)
	}

	// capture the insert the server actually wrote
	cur, err := conn.Client.Database("local").Collection("oplog.rs").Find(nil,
		bson.M{"ns": srcDB + "." + viewsNS, "op": "i", "ts": bson.M{"$gt": startTs}},
		options.Find().SetSort(bson.D{{Key: "ts", Value: 1}}))
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	var raws [][]byte
	for cur.Next(nil) {
		// copied out: ParseRaw borrows its input and the cursor reuses its buffer
		raws = append(raws, append([]byte(nil), cur.Current...))
	}
	if err := cur.Close(nil); err != nil {
		t.Fatalf("close oplog cursor: %v", err)
	}
	if len(raws) == 0 {
		t.Fatalf("creating a time-series collection wrote no %s.%s insert to the "+
			"oplog, so there is nothing to replay", srcDB, viewsNS)
	}

	viewsBefore, err := conn.Client.Database(tgtDB).Collection(viewsNS).
		CountDocuments(nil, bson.M{})
	if err != nil {
		t.Fatalf("count target system.views before replay: %v", err)
	}

	writer := NewDbWriter(conn, bson.E{}, false, 0)
	for i, raw := range raws {
		partialLog, err := oplog.ParseRaw(raw)
		if err != nil {
			t.Fatalf("ParseRaw of captured system.views op[%d]: %v", i, err)
		}
		if partialLog.UI == nil {
			t.Fatalf("captured system.views op[%d] carries no ui, so this run would "+
				"not exercise the bug: applyOps only rejects the op once a source "+
				"uuid is forwarded", i)
		}
		partialLog.Namespace = tgtDB + "." + viewsNS
		records := []*OplogRecord{{
			original: &PartialLogWithCallback{partialLog: partialLog, callback: func() {}},
			wait:     nil,
		}}
		if err := writer.doInsert(tgtDB, viewsNS, bson.E{}, records, false); err != nil {
			t.Errorf("replaying captured system.views op[%d] failed: %v", i, err)
		}
	}

	viewsAfter, err := conn.Client.Database(tgtDB).Collection(viewsNS).
		CountDocuments(nil, bson.M{})
	if err != nil {
		t.Fatalf("count target system.views after replay: %v", err)
	}
	if int(viewsAfter-viewsBefore) != len(raws) {
		t.Errorf("target %s.%s gained %d document(s), want %d", tgtDB, viewsNS,
			viewsAfter-viewsBefore, len(raws))
	}

	// and the definition that landed must be the source's
	srcViews, err := unit_test_common.FetchAllDocumentBsonM(conn.Client, srcDB, viewsNS, nil)
	if err != nil {
		t.Fatalf("read source system.views: %v", err)
	}
	if len(srcViews) == 0 {
		t.Fatalf("source %s.%s is empty", srcDB, viewsNS)
	}
	for _, want := range srcViews {
		id, _ := want["_id"].(string)
		var got bson.M
		if err := conn.Client.Database(tgtDB).Collection(viewsNS).
			FindOne(nil, bson.M{"_id": id}).Decode(&got); err != nil {
			t.Errorf("view definition %q did not reach the target: %v", id, err)
			continue
		}
		if tsbCanonical(want) != tsbCanonical(got) {
			t.Errorf("view definition %q differs:\n  src=%s\n  tgt=%s",
				id, tsbCanonical(want), tsbCanonical(got))
		}
	}
}

// latestOplogTs returns the timestamp of the newest oplog entry, or a zero
// timestamp when the oplog is still empty.
func latestOplogTs(client *mongo.Client) (primitive.Timestamp, error) {
	var entry struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	err := client.Database("local").Collection("oplog.rs").FindOne(nil, bson.M{},
		options.FindOne().SetSort(bson.D{{Key: "ts", Value: -1}})).Decode(&entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return primitive.Timestamp{}, nil
	}
	if err != nil {
		return primitive.Timestamp{}, err
	}
	return entry.Ts, nil
}

func createTsCollection(client *mongo.Client, db, coll string) error {
	return client.Database(db).RunCommand(nil, bson.D{
		{Key: "create", Value: coll},
		{Key: "timeseries", Value: bson.D{
			{Key: "timeField", Value: "timestamp"},
			{Key: "metaField", Value: "metadata"},
			{Key: "granularity", Value: "hours"},
		}},
	}).Err()
}

func tsbCanonicalSet(docs []bson.M) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, tsbCanonical(d))
	}
	sort.Strings(out)
	return out
}

// tsbCanonical renders a value as a stable string with every document's keys
// sorted. bson.Marshal of a bson.M follows Go's randomized map iteration order, so
// comparing marshalled bytes would report differences between identical documents.
func tsbCanonical(v interface{}) string {
	switch t := v.(type) {
	case bson.M:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+"="+tsbCanonical(t[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case bson.D:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, e.Key+"="+tsbCanonical(e.Value))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case bson.A:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, tsbCanonical(e))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case primitive.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case primitive.ObjectID:
		return t.Hex()
	case primitive.Binary:
		return fmt.Sprintf("bin(%d):%x", t.Subtype, t.Data)
	case []byte:
		return fmt.Sprintf("%x", t)
	default:
		return fmt.Sprintf("%v", v)
	}
}
