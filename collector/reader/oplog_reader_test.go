package sourceReader

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestCloneOplogRawDetachesCursorBuffer(t *testing.T) {
	original := bson.Raw{1, 2, 3, 4}
	cloned := cloneOplogRaw(original)

	original[0] = 9

	assert.Equal(t, bson.Raw{1, 2, 3, 4}, cloned, "should keep the original oplog bytes")
}

// withOrderingGuarantee turns on the DDL/DML ordering guarantee for the duration of a test: the
// fetch watermark is part of it, and the guarantee needs both tunnel = direct and the
// incr_sync.barrier.ordering_enable switch.
func withOrderingGuarantee(t *testing.T) func() {
	t.Helper()
	origTunnel, origEnable := conf.Options.Tunnel, conf.Options.IncrSyncBarrierOrderingEnable
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true
	return func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origEnable
	}
}

// mockRawOplog builds a raw document with the same field layout the source cursor hands back.
func mockRawOplog(ts int64) bson.Raw {
	raw, err := bson.Marshal(bson.D{
		{Key: "ts", Value: utils.Int64ToTimestamp(ts)},
		{Key: "op", Value: "u"},
		{Key: "ns", Value: "a.b"},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

// TestTrackFetchedTsRaisesWatermark pins the fetch watermark to the highest ts actually handed to
// the pipeline. It must rise, must never fall back (a rebuild must not resume below an oplog that
// was already delivered, which would re-deliver it behind a barrier), and must ignore documents it
// cannot read a ts from rather than panic or lower the watermark.
func TestTrackFetchedTsRaisesWatermark(t *testing.T) {
	// given
	defer withOrderingGuarantee(t)()
	or := NewOplogReader("mongodb://127.0.0.1:27017", "test")

	// then: nothing has been fetched yet
	assert.Equal(t, int64(0), or.lastFetchedTs, "should be equal")

	// when: an oplog is read
	or.trackFetchedTs(mockRawOplog(100))

	// then: the watermark follows it
	assert.Equal(t, int64(100), or.lastFetchedTs, "should be equal")

	// when: an older oplog is read afterwards
	or.trackFetchedTs(mockRawOplog(50))

	// then: the watermark does not move back
	assert.Equal(t, int64(100), or.lastFetchedTs, "should be equal")

	// when: a document without a ts is read
	or.trackFetchedTs(bson.Raw{})

	// then: it is ignored
	assert.Equal(t, int64(100), or.lastFetchedTs, "should be equal")
}

// TestAdvanceQueryBaseToFetchWatermark covers the cursor rebuild base. query[QueryTs] is driven by
// the batcher's last dispatched timestamp and trails the fetch watermark, so a rebuild has to be
// lifted to the watermark. The capped-oplog check reads the same base, so the local value the caller
// compares against has to follow -- otherwise a lagging dispatched timestamp looks like "the source
// oplog rolled past us" and is escalated to a fatal error.
func TestAdvanceQueryBaseToFetchWatermark(t *testing.T) {
	// present: the watermark is ahead of the query base, as it is in production
	defer withOrderingGuarantee(t)()
	or := NewOplogReader("mongodb://127.0.0.1:27017", "test")
	or.SetQueryTimestampOnEmpty(int64(100))
	or.trackFetchedTs(mockRawOplog(300))

	// when
	moved := or.advanceQueryBaseToFetchWatermark()

	// then: the base is lifted to the watermark
	assert.Equal(t, true, moved, "should be equal")
	assert.Equal(t, int64(300), or.getQueryTimestamp(), "should be equal")

	// when: called again, with nothing new read
	moved = or.advanceQueryBaseToFetchWatermark()

	// then: no further move
	assert.Equal(t, false, moved, "should be equal")
	assert.Equal(t, int64(300), or.getQueryTimestamp(), "should be equal")

	// given: a checkpoint ahead of what has been fetched
	behind := NewOplogReader("mongodb://127.0.0.1:27017", "test")
	behind.SetQueryTimestampOnEmpty(int64(500))
	behind.trackFetchedTs(mockRawOplog(300))

	// when
	moved = behind.advanceQueryBaseToFetchWatermark()

	// then: the base is never pulled backwards
	assert.Equal(t, false, moved, "should be equal")
	assert.Equal(t, int64(500), behind.getQueryTimestamp(), "should be equal")
}

// TestWatermarkIsInertWithoutOrderingGuarantee pins the scope of the fetch watermark: it exists
// only to serve the ordering guarantee, so with the guarantee off -- a non-direct tunnel, or the
// incr_sync.barrier.ordering_enable switch left at its default -- the cursor base is rebuilt from
// the last dispatched timestamp, and no watermark is tracked at all.
func TestWatermarkIsInertWithoutOrderingGuarantee(t *testing.T) {
	origTunnel, origEnable := conf.Options.Tunnel, conf.Options.IncrSyncBarrierOrderingEnable
	defer func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origEnable
	}()

	for _, tc := range []struct {
		tunnel string
		enable bool
	}{
		{"", true}, // an unset tunnel is non-direct
		{utils.VarTunnelKafka, true},
		{utils.VarTunnelFile, true},
		{utils.VarTunnelDirect, false}, // the switch defaults to false: direct alone is not enough
	} {
		conf.Options.Tunnel = tc.tunnel
		conf.Options.IncrSyncBarrierOrderingEnable = tc.enable

		or := NewOplogReader("mongodb://127.0.0.1:27017", "test")
		or.SetQueryTimestampOnEmpty(int64(100))
		or.trackFetchedTs(mockRawOplog(300))

		desc := fmt.Sprintf("tunnel %q, ordering_enable %v", tc.tunnel, tc.enable)

		// then: nothing is tracked...
		assert.Equal(t, int64(0), or.lastFetchedTs, "%s must not track the watermark", desc)

		// ...and a rebuild leaves the base where the dispatcher put it
		assert.Equal(t, false, or.advanceQueryBaseToFetchWatermark(), "%s must not advance", desc)
		assert.Equal(t, int64(100), or.getQueryTimestamp(), "%s must keep the base", desc)
	}
}
