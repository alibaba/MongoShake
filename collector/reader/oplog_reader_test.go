package sourceReader

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

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

// The first read used to only warn when the position had fallen out of the
// serving member's oplog window and then continue from that member's oldest
// entry, silently skipping the changes in between while the syncer reported
// state "normal" (#1046). A lost real position must fail like later reads do.
func TestOplogReaderFirstReadRejectsLostPosition(t *testing.T) {
	or := newWindowTestReader(t, windowTestLostTs)

	err := or.EnsureNetwork()

	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("first read accepted a position outside the oplog window: err=%v, want CollectionCappedError", err)
	}
}

// A sentinel position means "no checkpoint was ever given", which legitimately
// starts from the earliest available oplog. Rejecting it would break
// sync_mode=incr without checkpoint.start_position.
func TestOplogReaderFirstReadKeepsSentinelPosition(t *testing.T) {
	for _, ts := range []int64{1, 1 << 32} {
		or := newWindowTestReader(t, ts)

		err := or.EnsureNetwork()

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

	if !errors.Is(err, CollectionCappedError) {
		t.Fatalf("later read accepted a sentinel position: err=%v, want CollectionCappedError", err)
	}
}
