package ckpt

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// legacyCheckpointDoc builds a document exactly as the mgo-based releases stored
// it. BOTH ckpt and oplog_disk_queue_apply_finish_ts were typed
// bson.MongoTimestamp (verified against release-v2.6.6-20220323), and mgo encodes
// that as a BSON timestamp (0x11). A fixture that converts only ckpt is not a
// faithful legacy document and hides half the bug.
func legacyCheckpointDoc(ts primitive.Timestamp) bson.M {
	return bson.M{
		"name":                             "rs0",
		"ckpt":                             ts,
		"version":                          2,
		"fetch_method":                     "oplog",
		"oplog_disk_queue":                 "queue-name",
		"oplog_disk_queue_apply_finish_ts": ts,
	}
}

// TestCheckpointContextDecodesFullLegacyDocument is the regression anchor for the
// upgrade blocker hit on a real deployment: full sync completes, then the collector
// exits at the incremental handover with "cannot decode timestamp into an integer
// type" followed by "current ckpt context is empty".
func TestCheckpointContextDecodesFullLegacyDocument(t *testing.T) {
	ts := primitive.Timestamp{T: 1794476555, I: 11}
	want := int64(ts.T)<<32 | int64(ts.I)

	data, err := bson.Marshal(legacyCheckpointDoc(ts))
	require.NoError(t, err)

	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, CheckpointContext{
		Name:                   "rs0",
		Timestamp:              want,
		Version:                2,
		FetchMethod:            "oplog",
		OplogDiskQueue:         "queue-name",
		OplogDiskQueueFinishTs: want,
	}, got)
}

// TestCheckpointContextDecodesReportedDocument pins the exact document a real
// deployment could not read: ckpt is a legacy timestamp and
// oplog_disk_queue_apply_finish_ts is Timestamp(0, 0), the mgo-era EmptyCheckpoint.
// Zero must decode to 0, which collector/checkpoint.go treats the same as
// InitCheckpoint (both fall in the "no disk queue" branch).
//
// Note Timestamp(0, 0) cannot be produced by an insert - the server rewrites it to
// the current time - so it only ever arrives from an update written by old code.
func TestCheckpointContextDecodesReportedDocument(t *testing.T) {
	data, err := bson.Marshal(bson.M{
		"name":                             "mgset-7435",
		"ckpt":                             primitive.Timestamp{T: 1794564662, I: 1},
		"fetch_method":                     "oplog",
		"oplog_disk_queue":                 "",
		"oplog_disk_queue_apply_finish_ts": primitive.Timestamp{T: 0, I: 0},
		"version":                          2,
	})
	require.NoError(t, err)

	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, CheckpointContext{
		Name:                   "mgset-7435",
		Timestamp:              int64(1794564662)<<32 | int64(1),
		Version:                2,
		FetchMethod:            "oplog",
		OplogDiskQueue:         "",
		OplogDiskQueueFinishTs: 0,
	}, got)
}

// TestCheckpointContextDecodesEitherEncoding covers each field independently, so a
// half-converted document (only one field still legacy) is handled too.
func TestCheckpointContextDecodesEitherEncoding(t *testing.T) {
	const want = int64(1794476555)<<32 | int64(11)
	legacy := primitive.Timestamp{T: 1794476555, I: 11}

	cases := []struct {
		desc     string
		ckpt     interface{}
		finishTs interface{}
		wantCkpt int64
		wantFin  int64
	}{
		{"both legacy", legacy, legacy, want, want},
		{"both current", want, want, want, want},
		{"only ckpt legacy", legacy, want, want, want},
		{"only finish_ts legacy", want, legacy, want, want},
		{"int32 ckpt", int32(42), int64(7), int64(42), int64(7)},
		{"zero timestamp", primitive.Timestamp{T: 0, I: 1}, primitive.Timestamp{T: 0, I: 1}, 1, 1},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			doc := legacyCheckpointDoc(legacy)
			doc["ckpt"] = c.ckpt
			doc["oplog_disk_queue_apply_finish_ts"] = c.finishTs
			data, err := bson.Marshal(doc)
			require.NoError(t, err)

			var got CheckpointContext
			require.NoError(t, bson.Unmarshal(data, &got))
			assert.Equal(t, c.wantCkpt, got.Timestamp)
			assert.Equal(t, c.wantFin, got.OplogDiskQueueFinishTs)
		})
	}
}

// TestCheckpointContextDecodeKeepsAllFields guards the custom decoder against
// drifting from the struct. The fixture is produced by marshalling the struct
// itself, so a newly added field is carried automatically and a decoder that drops
// it fails here instead of comparing two zero values.
func TestCheckpointContextDecodeKeepsAllFields(t *testing.T) {
	src := CheckpointContext{
		Name:                   "rs-all",
		Timestamp:              int64(1794476555)<<32 | int64(11),
		Version:                2,
		FetchMethod:            "oplog",
		OplogDiskQueue:         "queue-name",
		OplogDiskQueueFinishTs: 12345,
	}
	data, err := bson.Marshal(&src)
	require.NoError(t, err)

	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, src, got)

	// every bson-tagged field must be present in the document, which is what the
	// aux struct inside UnmarshalBSON has to mirror
	raw := bson.Raw(data)
	typ := reflect.TypeOf(src)
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("bson")
		require.NotEmpty(t, tag, "field %s has no bson tag", typ.Field(i).Name)
		_, err := raw.LookupErr(tag)
		assert.NoError(t, err, "field %s (bson %q) missing from document", typ.Field(i).Name, tag)
	}
}

// TestCheckpointContextMissingFields: absent numeric fields decode as 0, matching
// the zero value the struct would otherwise get.
func TestCheckpointContextMissingFields(t *testing.T) {
	data, err := bson.Marshal(bson.M{"name": "rs0", "version": 2})
	require.NoError(t, err)
	var got CheckpointContext
	require.NoError(t, bson.Unmarshal(data, &got))
	assert.Equal(t, int64(0), got.Timestamp)
	assert.Equal(t, int64(0), got.OplogDiskQueueFinishTs)
}

// TestCheckpointContextRejectsUnusableValues: a type that cannot denote a timestamp
// must be reported, not silently turned into 0 - that would restart replication
// from the beginning.
func TestCheckpointContextRejectsUnusableValues(t *testing.T) {
	for _, key := range []string{"ckpt", "oplog_disk_queue_apply_finish_ts"} {
		t.Run(key, func(t *testing.T) {
			doc := legacyCheckpointDoc(primitive.Timestamp{T: 1, I: 1})
			doc[key] = "not-a-timestamp"
			data, err := bson.Marshal(doc)
			require.NoError(t, err)
			var got CheckpointContext
			err = bson.Unmarshal(data, &got)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported")
		})
	}
}

// TestCheckpointContextMarshalStaysInt64 pins the write encoding: after the first
// update the stored document is normalized, so the legacy form disappears.
func TestCheckpointContextMarshalStaysInt64(t *testing.T) {
	cc := CheckpointContext{Name: "rs0", Timestamp: 7707548275594756097, OplogDiskQueueFinishTs: 1}
	data, err := bson.Marshal(&cc)
	require.NoError(t, err)
	raw := bson.Raw(data)
	for _, key := range []string{"ckpt", "oplog_disk_queue_apply_finish_ts"} {
		v, err := raw.LookupErr(key)
		require.NoError(t, err)
		assert.Equal(t, bson.TypeInt64, v.Type, "%s must be written as int64", key)
	}
}
