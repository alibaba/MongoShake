package oplog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func mockCommitOnlyOplog(ts int64) *GenericOplog {
	txnNumber := int64(1)
	lsid, _ := bson.Marshal(bson.D{
		{"id", primitive.Binary{Subtype: 4, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}}},
	})
	prev, _ := bson.Marshal(bson.D{{"ts", primitive.Timestamp{T: uint32(ts - 2)}}})
	return &GenericOplog{
		Parsed: &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.Timestamp{T: uint32(ts)},
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"commitTransaction", int32(1)},
					{"commitTimestamp", primitive.Timestamp{T: uint32(ts - 2)}},
				},
				LSID:       lsid,
				TxnNumber:  &txnNumber,
				PrevOpTime: prev,
			},
		},
	}
}

// After a restart, the prepare/partialTxn oplogs of a transaction may never be
// re-fetched (they only lived in memory). The commitTransaction oplog then
// arrives for a transaction the buffer has never seen. GetTxnStream must fail
// loudly instead of silently streaming an empty transaction, which would mean
// silent data loss.
func TestGetTxnStreamOrphanCommitReturnsError(t *testing.T) {
	b := NewBuffer()
	op := mockCommitOnlyOplog(100)
	meta, err := NewTxnMeta(op.Parsed.ParsedLog)
	assert.Nil(t, err)
	assert.True(t, meta.IsCommitOp())

	// AddOp creates a brand-new empty state for the orphan commit
	assert.Nil(t, b.AddOp(meta, op))

	ops, errs := b.GetTxnStream(meta)
	var streamed int
	for range ops {
		streamed++
	}
	gotErr := <-errs

	assert.Equal(t, 0, streamed, "orphan commit must not stream any op")
	assert.NotNil(t, gotErr, "orphan commit must return an error instead of an empty transaction")
}

// A legitimately empty prepared transaction (applyOps may be empty, see
// SERVER-50769) must still stream successfully: its state saw a data entry
// (the prepare op), so it is NOT an orphan.
func TestGetTxnStreamEmptyPreparedTxnStillWorks(t *testing.T) {
	b := NewBuffer()
	txnNumber := int64(1)
	lsid, _ := bson.Marshal(bson.D{
		{"id", primitive.Binary{Subtype: 4, Data: []byte{9, 9, 9, 9, 9, 9, 9, 9}}},
	})
	prepare := &GenericOplog{
		Parsed: &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.Timestamp{T: 100},
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"applyOps", bson.A{}},
					{"prepare", true},
				},
				LSID:      lsid,
				TxnNumber: &txnNumber,
			},
		},
	}
	prepareMeta, err := NewTxnMeta(prepare.Parsed.ParsedLog)
	assert.Nil(t, err)
	assert.Nil(t, b.AddOp(prepareMeta, prepare))

	commit := mockCommitOnlyOplog(102)
	commit.Parsed.ParsedLog.LSID = lsid
	commitMeta, err := NewTxnMeta(commit.Parsed.ParsedLog)
	assert.Nil(t, err)
	assert.Nil(t, b.AddOp(commitMeta, commit))

	ops, errs := b.GetTxnStream(commitMeta)
	for range ops {
	}
	gotErr := <-errs
	assert.Nil(t, gotErr, "empty but known prepared txn must not error")
}
