package sourceReader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
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
	newest := int64(1700000000) << 32
	cases := []struct {
		name   string
		query  int64
		newest int64
		want   int64
	}{
		{"future position clamps to newest", newest + 100, newest, newest},
		{"equal position unchanged", newest, newest, newest},
		{"past position unchanged", newest - 100, newest, newest - 100},
		// getOplogTimestamp returns 0 when the probe fails; clamping to it
		// would rewind the start to zero and replay the whole oplog.
		{"failed newest probe never rewinds", newest + 100, 0, newest + 100},
		{"sentinel unchanged", 1 << 32, newest, 1 << 32},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clampFuturePosition(c.query, c.newest); got != c.want {
				t.Fatalf("clampFuturePosition(%v, %v) = %v, want %v", c.query, c.newest, got, c.want)
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
