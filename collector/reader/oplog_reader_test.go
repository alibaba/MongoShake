package sourceReader

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

func TestCloneOplogRawDetachesCursorBuffer(t *testing.T) {
	original := bson.Raw{1, 2, 3, 4}
	cloned := cloneOplogRaw(original)

	original[0] = 9

	assert.Equal(t, bson.Raw{1, 2, 3, 4}, cloned, "should keep the original oplog bytes")
}

// A real position no live oplog can still contain (2000-01-01), so its seconds
// part is far above the sentinel range and it is unambiguously "lost".
const windowTestLostTs = int64(946684800) << 32

func newWindowTestReader(t *testing.T, ts int64) *OplogReader {
	t.Helper()
	oldBatch, oldMode := conf.Options.IncrSyncReaderFetchBatchSize, conf.Options.MongoConnectMode
	conf.Options.IncrSyncReaderFetchBatchSize = 8192
	conf.Options.MongoConnectMode = utils.VarMongoConnectModePrimary
	or := NewOplogReader(unit_test_common.TestUrl, "ut_oplog_window")
	or.UpdateQueryTimestamp(ts)
	or.SetAnchorPersistHook(func(int64) error { return nil })
	t.Cleanup(func() {
		or.releaseCursor()
		if or.conn != nil {
			or.conn.Close()
		}
		conf.Options.IncrSyncReaderFetchBatchSize, conf.Options.MongoConnectMode = oldBatch, oldMode
	})
	return or
}

// requireUsableOldest makes a broken fixture diagnose itself. getOldestTimestamp
// discards the probe error, so a failed probe (or missing read permission on
// local) yields oldest=0 and the window check is skipped entirely -- which would
// otherwise surface as a confusing "position was accepted" failure.
func requireUsableOldest(t *testing.T, or *OplogReader) {
	t.Helper()
	if or.conn == nil {
		t.Fatal("no connection was established, cannot judge the oplog window")
	}
	if oldest := or.getOldestTimestamp(); oldest <= 0 {
		t.Fatalf("oplog probe returned oldest=%v; the window check would be skipped", oldest)
	}
}

// The first read used to only warn when the position had fallen out of the
// serving member's oplog window and then continue from that member's oldest
// entry, silently skipping the changes in between while the syncer reported
// state "normal" (#1046). A lost real position must fail like later reads do.
func TestOplogReaderFirstReadRejectsLostPosition(t *testing.T) {
	or := newWindowTestReader(t, windowTestLostTs)

	err := or.EnsureNetwork()

	requireUsableOldest(t, or)
	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("first read accepted a position outside the oplog window: err=%v, want CollectionCappedError", err)
	}
}

// The exemption is bounded by the packed value, not by "looks old": the smallest
// position above the sentinel range must already be rejected. Without this case
// the threshold can be shifted (e.g. to 1<<33) with every other test still green.
func TestOplogReaderFirstReadRejectsPositionJustAboveSentinel(t *testing.T) {
	or := newWindowTestReader(t, 2<<32)

	err := or.EnsureNetwork()

	requireUsableOldest(t, or)
	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("first read accepted position 2<<32, just above the sentinel range: err=%v", err)
	}
}

// A sentinel position means "no usable checkpoint", which legitimately starts
// from the earliest available oplog. Rejecting it would break
// sync_mode=incr without checkpoint.start_position.
func TestOplogReaderFirstReadKeepsSentinelPosition(t *testing.T) {
	for _, ts := range []int64{1, 1 << 32} {
		or := newWindowTestReader(t, ts)

		err := or.EnsureNetwork()

		requireUsableOldest(t, or)
		if err != nil {
			t.Fatalf("sentinel position %v was rejected: %v", ts, err)
		}
		if or.oplogsCursor == nil {
			t.Fatalf("sentinel position %v did not open a cursor", ts)
		}
	}
}

// Later reads already rejected a lost position; that must not change.
func TestOplogReaderLaterReadStillRejectsLostPosition(t *testing.T) {
	or := newWindowTestReader(t, 1<<32)
	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("first read with a sentinel position failed: %v", err)
	}
	or.releaseCursor()
	or.UpdateQueryTimestamp(windowTestLostTs)

	err := or.EnsureNetwork()

	requireUsableOldest(t, or)
	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("later read accepted a lost position: err=%v, want CollectionCappedError", err)
	}
}

// The "start from earliest" exemption belongs to the first read only. A later
// read must reject even the sentinel position, which is reachable when the
// connection drops after the first cursor was opened but before the position
// advanced. This is the only case that exercises the !firstRead disjunct on its
// own: without it, dropping that disjunct leaves every other test green.
func TestOplogReaderLaterReadRejectsSentinelPosition(t *testing.T) {
	or := newWindowTestReader(t, 1<<32)
	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("first read with a sentinel position failed: %v", err)
	}
	or.releaseCursor()

	err := or.EnsureNetwork()

	requireUsableOldest(t, or)
	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("later read accepted a sentinel position: err=%v, want CollectionCappedError", err)
	}
}

func TestClampFuturePosition(t *testing.T) {
	serving := int64(1700000000) << 32
	primary := serving + 50
	cases := []struct {
		name    string
		query   int64
		serving int64
		primary int64
		want    int64
	}{
		{"genuinely future clamps to serving newest", primary + 100, serving, primary, serving},
		{"equal position unchanged", serving, serving, primary, serving},
		{"past position unchanged", serving - 100, serving, primary, serving - 100},
		// getOplogTimestamp returns 0 when a probe fails; clamping on a failed
		// probe would rewind the start to zero and replay the whole oplog.
		{"failed serving probe never rewinds", primary + 100, 0, primary, primary + 100},
		{"failed primary probe never rewrites", primary + 100, serving, 0, primary + 100},
		// A valid checkpoint ahead of a lagging serving member is acknowledged
		// progress, not a future position: wait for catch-up, do not rewrite.
		{"lagging serving member keeps valid checkpoint", serving + 100, serving, serving + 200, serving + 100},
		{"sentinel unchanged", 1 << 32, serving, primary, 1 << 32},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clampFuturePosition(c.query, c.serving, c.primary); got != c.want {
				t.Fatalf("clampFuturePosition(%v, %v, %v) = %v, want %v", c.query, c.serving, c.primary, got, c.want)
			}
		})
	}
}

// A future starting point must be clamped to the newest oplog entry instead of
// silently waiting: waiting skips every change between newest and the configured
// point (#1049). The adjusted anchor must be written back to or.query so a later
// cursor rebuild keeps it.
func TestOplogReaderFirstReadClampsFuturePosition(t *testing.T) {
	future := (time.Now().Unix() + 3600) << 32
	or := newWindowTestReader(t, future)
	var persisted int64
	or.SetAnchorPersistHook(func(ts int64) error { persisted = ts; return nil })

	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("first read with a future position failed: %v", err)
	}
	if or.oplogsCursor == nil {
		t.Fatal("future position did not open a cursor")
	}
	clamped := or.getQueryTimestamp()
	// Self-diagnose a broken fixture like requireUsableOldest does: a failed
	// newest probe (0) skips the clamp by design, which must not be reported
	// as a code regression.
	if newest := or.getNewestTimestamp(); newest <= 0 {
		t.Fatalf("newest oplog probe returned %v; the clamp would be skipped by the guard", newest)
	}
	if clamped >= future {
		t.Fatalf("starting point was not clamped: query ts %v still >= future %v", clamped, future)
	}
	if clamped <= 0 {
		t.Fatalf("clamped starting point is invalid: %v", clamped)
	}
	if persisted != clamped {
		t.Fatalf("adjusted anchor was not persisted before flow: persisted=%v clamped=%v", persisted, clamped)
	}

	// A change written now (ts far below the future point) must be delivered:
	// this is exactly the interval the unclamped query would silently skip.
	// Pin the probe document's namespace so foreign traffic on a shared cluster
	// cannot satisfy the assertion in place of the probe write.
	dbName := "ut_oplog_clamp_1049"
	if _, err := or.conn.Client.Database(dbName).Collection("c").InsertOne(context.Background(), bson.M{"x": 1}); err != nil {
		t.Fatalf("insert probe document failed: %v", err)
	}
	t.Cleanup(func() {
		_ = or.conn.Client.Database(dbName).Drop(context.Background())
	})
	probeNS := dbName + ".c"
	deadline := time.Now().Add(15 * time.Second)
	for {
		if !or.oplogsCursor.TryNext(context.Background()) {
			if err := or.oplogsCursor.Err(); err != nil {
				t.Fatalf("cursor error while waiting for the probe oplog: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("probe oplog[%s] not delivered within 15s: the reader is waiting for the future point instead of replaying from newest", probeNS)
			}
			continue
		}
		var entry bson.M
		if err := bson.Unmarshal(or.oplogsCursor.Current, &entry); err != nil {
			t.Fatalf("decode delivered oplog failed: %v", err)
		}
		if entry["ns"] != probeNS {
			if time.Now().After(deadline) {
				t.Fatalf("probe oplog[%s] not delivered within 15s: the reader is waiting for the future point instead of replaying from newest", probeNS)
			}
			continue
		}
		gotTs := utils.TimeStampToInt64(entry["ts"].(primitive.Timestamp))
		if gotTs >= future {
			t.Fatalf("delivered probe oplog ts %v is not below the future point %v", gotTs, future)
		}
		break
	}

	or.releaseCursor()
	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("cursor rebuild after clamp failed: %v", err)
	}
	if again := or.getQueryTimestamp(); again != clamped {
		t.Fatalf("cursor rebuild reset the anchor: %v != %v", again, clamped)
	}
}

// The clamp belongs to the first read only: later reads take their anchor from
// fetched progress, and re-clamping on every rebuild would mask an anchor that
// was legitimately set ahead of the serving member (e.g. after a failover to a
// lagging node). Pin the boundary so moving the clamp out of the firstRead
// branch cannot pass unnoticed.
func TestOplogReaderLaterReadDoesNotClampFuturePosition(t *testing.T) {
	or := newWindowTestReader(t, 1<<32)
	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("first read with a sentinel position failed: %v", err)
	}
	or.releaseCursor()
	future := (time.Now().Unix() + 3600) << 32
	or.UpdateQueryTimestamp(future)

	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("later read with a future position failed: %v", err)
	}
	if again := or.getQueryTimestamp(); again != future {
		t.Fatalf("later read clamped the anchor: %v != %v", again, future)
	}
}

// Persistence failure must block the stream, not fall back to a volatile-only
// clamp: the fetcher retries EnsureNetwork, and a later attempt with a working
// hook must succeed without losing the adjustment (#1052 review P1).
func TestOplogReaderFirstReadClampBlocksOnPersistFailure(t *testing.T) {
	future := (time.Now().Unix() + 3600) << 32
	or := newWindowTestReader(t, future)
	or.SetAnchorPersistHook(func(int64) error { return errors.New("checkpoint storage unavailable") })

	err := or.EnsureNetwork()
	if err == nil {
		t.Fatal("first read accepted a future position although persisting the adjusted anchor failed")
	}
	if or.oplogsCursor != nil {
		t.Fatal("a cursor was opened although the adjusted anchor was not persisted")
	}
	if again := or.getQueryTimestamp(); again != future {
		t.Fatalf("query anchor changed without successful persistence: %v != %v", again, future)
	}
	if !or.firstRead {
		t.Fatal("firstRead must stay true so the next retry clamps again")
	}

	var persisted int64
	or.SetAnchorPersistHook(func(ts int64) error { persisted = ts; return nil })
	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("retry after persistence recovery failed: %v", err)
	}
	clamped := or.getQueryTimestamp()
	if clamped >= future || persisted != clamped {
		t.Fatalf("retry did not clamp and persist: clamped=%v persisted=%v future=%v", clamped, persisted, future)
	}
}

// Without a persist hook the reader must keep the future position (wait
// behavior) instead of clamping volatile-only: a restart would forget the
// adjustment and skip the gap (#1052 review P1).
func TestOplogReaderFirstReadNoHookKeepsFuturePosition(t *testing.T) {
	future := (time.Now().Unix() + 3600) << 32
	or := newWindowTestReader(t, future)
	or.SetAnchorPersistHook(nil)

	if err := or.EnsureNetwork(); err != nil {
		t.Fatalf("first read without a hook failed: %v", err)
	}
	if again := or.getQueryTimestamp(); again != future {
		t.Fatalf("clamp happened without a persist hook: %v != %v", again, future)
	}
	if or.oplogsCursor == nil {
		t.Fatal("future position without a hook did not open a waiting cursor")
	}
}

func newDecisionTestReader(t *testing.T, query int64, hook func(int64) error) *OplogReader {
	t.Helper()
	if l.Logger == nil {
		assert.NoError(t, utils.InitialLogger("", "", "error", true, 2))
	}
	or := NewOplogReader("mongodb://127.0.0.1:1", "ut_decision")
	or.UpdateQueryTimestamp(query)
	or.SetAnchorPersistHook(hook)
	return or
}

// The decision core without probes: pin every branch, including the two
// blocked-retry paths (primary probe failure, persist failure) that must NOT
// fall through to a volatile cursor at the future point.
func TestApplyFuturePositionDecision(t *testing.T) {
	serving := int64(1700000000) << 32
	primary := serving + 50
	cases := []struct {
		name          string
		query         int64
		serving       int64
		primary       int64
		hookFails     bool
		noHook        bool
		wantErr       bool
		wantQuery     int64
		wantPersisted int64
	}{
		{"lagging serving keeps valid checkpoint", serving + 100, serving, serving + 200, false, false, false, serving + 100, 0},
		{"genuinely future clamps and persists", primary + 100, serving, primary, false, false, false, serving, serving},
		{"primary probe failure blocks for retry", primary + 100, serving, 0, false, false, true, primary + 100, 0},
		{"serving probe failure fails open", primary + 100, 0, primary, false, false, false, primary + 100, 0},
		{"persist failure blocks for retry", primary + 100, serving, primary, true, false, true, primary + 100, 0},
		{"no hook keeps future position", primary + 100, serving, primary, false, true, false, primary + 100, 0},
		{"past position untouched", serving - 100, serving, primary, false, false, false, serving - 100, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var persisted int64
			hook := func(ts int64) error {
				if c.hookFails {
					return errors.New("storage unavailable")
				}
				persisted = ts
				return nil
			}
			if c.noHook {
				hook = nil
			}
			or := newDecisionTestReader(t, c.query, hook)
			err := or.applyFuturePositionDecision(c.query, c.serving, c.primary)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if again := or.getQueryTimestamp(); again != c.wantQuery {
				t.Fatalf("query anchor=%v, want %v", again, c.wantQuery)
			}
			if persisted != c.wantPersisted {
				t.Fatalf("persisted=%v, want %v", persisted, c.wantPersisted)
			}
		})
	}
}

// Wiring-level coverage with injected probes: replacing the primary probe with
// the serving probe (round-1 behavior) must fail the lagging-serving case.
func TestAdjustFutureStartPositionUsesPrimaryProbe(t *testing.T) {
	serving := int64(1700000000) << 32
	cases := []struct {
		name          string
		query         int64
		primary       int64
		wantErr       bool
		wantQuery     int64
		wantPersisted int64
	}{
		{"lagging serving keeps valid checkpoint", serving + 100, serving + 200, false, serving + 100, 0},
		{"genuinely future clamps and persists", serving + 300, serving + 200, false, serving, serving},
		{"primary probe failure blocks for retry", serving + 300, 0, true, serving + 300, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var persisted int64
			or := newDecisionTestReader(t, c.query, func(ts int64) error { persisted = ts; return nil })
			or.servingProbe = func() int64 { return serving }
			or.primaryProbe = func() int64 { return c.primary }
			err := or.adjustFutureStartPosition()
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if c.wantErr && !strings.Contains(err.Error(), "primary newest probe failed") {
				t.Fatalf("retry error lacks diagnostic context: %v", err)
			}
			if again := or.getQueryTimestamp(); again != c.wantQuery {
				t.Fatalf("query anchor=%v, want %v", again, c.wantQuery)
			}
			if persisted != c.wantPersisted {
				t.Fatalf("persisted=%v, want %v", persisted, c.wantPersisted)
			}
		})
	}
}

// The generated probe URI must survive the FULL downstream pipeline —
// NewMongoCommunityConn runs EncodeMongoURI on it before the driver parses it,
// so credentials must be raw in the probe URL (single-encoding convention).
// Asserting on connstring.Parse alone misses double-encoding (review B1).
// Path-derived authSource, ';' separators and mechanism must also survive
// (review round 4).
func TestPrimaryProbeURLPreservesCredentials(t *testing.T) {
	cases := []struct {
		name        string
		src         string
		addr        string
		wantUser    string
		wantPass    string
		wantAuthSrc string
		wantTLS     bool
		wantMech    string
	}{
		{name: "no auth", src: "mongodb://127.0.0.1:27030", addr: "10.0.0.1:27017"},
		// raw special chars incl. '@' in the password: two unescaped '@' in the
		// URI, mirroring the .env.test fixture convention (EncodeMongoURI splits
		// on the last '@' and percent-encodes the password).
		{name: "special char password", src: "mongodb://root_special_char3:#$^&*()_-=@@127.0.0.1:27030", addr: "10.0.0.1:27017", wantUser: "root_special_char3", wantPass: "#$^&*()_-=@"},
		{name: "directConnection source", src: "mongodb://127.0.0.1:27031/?directConnection=true", addr: "10.0.0.1:27017"},
		{name: "authSource and tls inherited", src: "mongodb://u:pw@127.0.0.1:27031/?authSource=mydb&tls=true&directConnection=true", addr: "10.0.0.1:27017", wantUser: "u", wantPass: "pw", wantAuthSrc: "mydb", wantTLS: true},
		{name: "path derived authSource", src: "mongodb://u:pw@127.0.0.1:27031/mydb", addr: "10.0.0.1:27017", wantUser: "u", wantPass: "pw", wantAuthSrc: "mydb"},
		{name: "semicolon separators", src: "mongodb://u:pw@127.0.0.1:27031/?authSource=mydb;tls=true", addr: "10.0.0.1:27017", wantUser: "u", wantPass: "pw", wantAuthSrc: "mydb", wantTLS: true},
		{name: "authMechanism inherited", src: "mongodb://u:pw@127.0.0.1:27031/?authMechanism=SCRAM-SHA-1&authSource=mydb", addr: "10.0.0.1:27017", wantUser: "u", wantPass: "pw", wantAuthSrc: "mydb", wantMech: "SCRAM-SHA-1"},
		{name: "topology options stripped", src: "mongodb://127.0.0.1:27031/?connect=automatic&loadBalanced=true&directConnection=true", addr: "10.0.0.1:27017"},
		{name: "replicaSet stripped", src: "mongodb://127.0.0.1:27031/?replicaSet=rs0&directConnection=true", addr: "10.0.0.1:27017"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := primaryProbeURL(c.src, c.addr)
			encoded, err := utils.EncodeMongoURI(got)
			if err != nil {
				t.Fatalf("EncodeMongoURI rejected the probe URL: %v (%s)", err, got)
			}
			// ParseAndValidate, not Parse: the real pipeline (ApplyURI) runs
			// conflict validation too (e.g. loadBalanced + directConnection).
			cs, err := connstring.ParseAndValidate(encoded)
			if err != nil {
				t.Fatalf("generated URI rejected by the driver parser: %v (%s)", err, encoded)
			}
			if cs.Username != c.wantUser || cs.Password != c.wantPass {
				t.Fatalf("credentials lost in the composed pipeline: user=%q pass=%q, want %q/%q", cs.Username, cs.Password, c.wantUser, c.wantPass)
			}
			if !cs.DirectConnection || len(cs.Hosts) != 1 || cs.Hosts[0] != c.addr {
				t.Fatalf("probe must be a direct connection to %s: hosts=%v direct=%v", c.addr, cs.Hosts, cs.DirectConnection)
			}
			if c.name == "replicaSet stripped" && cs.ReplicaSet != "" {
				t.Fatalf("replicaSet not stripped: %q", cs.ReplicaSet)
			}
			if c.wantAuthSrc != "" && cs.AuthSource != c.wantAuthSrc {
				t.Fatalf("authSource lost: %q, want %q", cs.AuthSource, c.wantAuthSrc)
			}
			if c.wantTLS && !cs.SSL {
				t.Fatal("tls option lost")
			}
			if c.wantMech != "" && cs.AuthMechanism != c.wantMech {
				t.Fatalf("authMechanism lost: %q, want %q", cs.AuthMechanism, c.wantMech)
			}
		})
	}
}
