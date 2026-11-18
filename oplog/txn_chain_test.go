package oplog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func chainTestEntry(t *testing.T, inc uint32, prev uint32, phase string, data bool) *GenericOplog {
	t.Helper()
	txn, term := int64(745), int64(8)
	ops := bson.A{}
	if data {
		ops = append(ops, bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "settlement.docs"}, {Key: "o", Value: bson.D{{Key: "_id", Value: int32(1)}, {Key: "nested", Value: bson.D{{Key: "value", Value: int64(42)}}}}}})
	}
	cmd := bson.D{{Key: "applyOps", Value: ops}}
	switch phase {
	case "prepare", "partialTxn":
		cmd = append(cmd, bson.E{Key: phase, Value: true})
	case "commitTransaction", "abortTransaction":
		cmd = bson.D{{Key: phase, Value: 1}}
	}
	previous, previousTerm := primitive.Timestamp{}, int64(-1)
	if prev != 0 {
		previous = primitive.Timestamp{T: 100, I: prev}
		previousTerm = 8
	}
	rawPrev, err := bson.Marshal(bson.D{{Key: "ts", Value: previous}, {Key: "t", Value: previousTerm}})
	require.NoError(t, err)
	lsid, err := bson.Marshal(bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}})
	require.NoError(t, err)
	return &GenericOplog{Parsed: &PartialLog{ParsedLog: ParsedLog{Timestamp: primitive.Timestamp{T: 100, I: inc}, Term: &term,
		Namespace: "admin.$cmd", Operation: "c", LSID: lsid, TxnNumber: &txn, PrevOpTime: rawPrev, Object: cmd,
	}}}
}

func TestTxnChainRejectsMissingOrInvalidPredecessors(t *testing.T) {
	for _, kind := range []string{"missing_first_prepare", "missing_first_final", "missing_middle", "wrong_term", "wrong_commit_predecessor", "absent_predecessor_after_data", "malformed_prev", "missing_prev_ts", "wrong_prev_ts_type", "wrong_prev_term_type", "missing_prev_term"} {
		t.Run(kind, func(t *testing.T) {
			partial := chainTestEntry(t, 30, 0, "partialTxn", true)
			prepare := chainTestEntry(t, 33, 30, "prepare", false)
			commit := chainTestEntry(t, 36, 33, "commitTransaction", false)
			input := []*GenericOplog{partial, prepare, commit}
			switch kind {
			case "missing_first_prepare":
				input = input[1:]
			case "missing_first_final":
				input = []*GenericOplog{chainTestEntry(t, 36, 30, "final", false)}
			case "missing_middle":
				input[0] = chainTestEntry(t, 20, 0, "partialTxn", true)
			case "wrong_term":
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}, {Key: "t", Value: int64(9)}})
			case "wrong_commit_predecessor":
				commit.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}, {Key: "t", Value: int64(8)}})
			case "absent_predecessor_after_data":
				commit.Parsed.PrevOpTime = nil
			case "malformed_prev":
				prepare.Parsed.PrevOpTime = bson.Raw{1, 2, 3}
			case "missing_prev_ts":
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "t", Value: int64(8)}})
			case "wrong_prev_ts_type":
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: int64(30)}, {Key: "t", Value: int64(8)}})
			case "wrong_prev_term_type":
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}, {Key: "t", Value: "8"}})
			case "missing_prev_term":
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}})
			}
			buffer := NewBuffer()
			t.Cleanup(func() { require.NoError(t, buffer.Stop()) })
			var final TxnMeta
			var chainErr error
			for _, entry := range input {
				meta, err := NewTxnMeta(entry.Parsed.ParsedLog)
				require.NoError(t, err)
				chainErr = buffer.AddOp(meta, entry)
				if chainErr != nil {
					break
				}
				final = meta
			}
			if chainErr != nil {
				require.Contains(t, chainErr.Error(), "transaction oplog chain")
				return
			}
			ops, errs := buffer.GetTxnStream(final)
			var got []*GenericOplog
			for op := range ops {
				got = append(got, op)
			}
			err := <-errs
			require.Error(t, err, "incomplete history must not become a successful empty/partial transaction")
			require.Contains(t, err.Error(), "transaction oplog chain")
			require.Empty(t, got, "validate the entire chain before streaming any selected business operation")
		})
	}
}

func TestTxnChainAcceptsCompleteAndLegacyTransactions(t *testing.T) {
	for _, kind := range []string{"empty_prepare", "empty_multipart", "data_then_empty_prepare", "legacy_without_prev", "termless_chain"} {
		t.Run(kind, func(t *testing.T) {
			partial := chainTestEntry(t, 30, 0, "partialTxn", false)
			prepare := chainTestEntry(t, 33, 30, "prepare", false)
			commit := chainTestEntry(t, 36, 33, "commitTransaction", false)
			input := []*GenericOplog{partial, prepare, commit}
			want := 0
			switch kind {
			case "empty_prepare":
				input = input[1:]
				input[0] = chainTestEntry(t, 33, 0, "prepare", false)
			case "data_then_empty_prepare":
				input[0] = chainTestEntry(t, 30, 0, "partialTxn", true)
				hash := int64(123)
				input[0].Parsed.Hash = &hash
				want = 1
			case "legacy_without_prev":
				input = []*GenericOplog{chainTestEntry(t, 30, 0, "final", true)}
				input[0].Parsed.PrevOpTime = nil
				want = 1
			case "termless_chain":
				for _, entry := range input {
					entry.Parsed.Term = nil
				}
				prepare.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}})
				commit.Parsed.PrevOpTime, _ = bson.Marshal(bson.D{{Key: "ts", Value: prepare.Parsed.Timestamp}})
			}
			buffer := NewBuffer()
			t.Cleanup(func() { require.NoError(t, buffer.Stop()) })
			var final TxnMeta
			for _, entry := range input {
				meta, err := NewTxnMeta(entry.Parsed.ParsedLog)
				require.NoError(t, err)
				require.NoError(t, buffer.AddOp(meta, entry))
				final = meta
			}
			ops, errs := buffer.GetTxnStream(final)
			var got []*GenericOplog
			for op := range ops {
				got = append(got, op)
			}
			require.NoError(t, <-errs)
			require.Len(t, got, want)
			if want > 0 {
				require.Equal(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "nested", Value: bson.D{{Key: "value", Value: int64(42)}}}}, got[0].Parsed.Object)
			}
			require.NoError(t, buffer.PurgeTxn(final))
			require.Zero(t, buffer.Size())
		})
	}
}

func TestTxnChainAbortDoesNotRequireMissingBusinessHistory(t *testing.T) {
	buffer := NewBuffer()
	t.Cleanup(func() { require.NoError(t, buffer.Stop()) })
	abort := chainTestEntry(t, 36, 33, "abortTransaction", false)
	meta, err := NewTxnMeta(abort.Parsed.ParsedLog)
	require.NoError(t, err)
	require.NoError(t, buffer.AddOp(meta, abort))
	require.NoError(t, buffer.PurgeTxn(meta))
	require.Zero(t, buffer.Size())
}
