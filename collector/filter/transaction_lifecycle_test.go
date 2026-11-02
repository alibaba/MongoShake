package filter

import (
	"testing"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Empty transaction envelopes must survive payload filtering: a later commit
// still needs evidence that its prepare/partialTxn was received.
func TestFiltersPreserveTransactionEnvelopes(t *testing.T) {
	require.NoError(t, utils.InitialLogger("", "", "error", true, 1))
	for _, kind := range []string{"namespace_white", "namespace_black", "op_type", "combined"} {
		for _, phase := range []string{"prepare", "partialTxn", "unprepared_final"} {
			for _, empty := range []bool{true, false} {
				name := kind + "/" + phase
				if empty {
					name += "/source_empty"
				} else {
					name += "/filtered_empty"
				}
				t.Run(name, func(t *testing.T) {
					var chain OplogFilterChain
					switch kind {
					case "namespace_white":
						chain = OplogFilterChain{NewNamespaceFilter([]string{"settlement"}, nil)}
					case "namespace_black":
						chain = OplogFilterChain{NewNamespaceFilter(nil, []string{"other"})}
					case "op_type":
						chain = OplogFilterChain{NewOpTypeFilter([]string{"i"})}
					case "combined":
						chain = OplogFilterChain{NewOpTypeFilter([]string{"i"}), NewNamespaceFilter([]string{"settlement"}, nil)}
					}
					ops := bson.A{}
					if !empty {
						ops = append(ops, bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "other.docs"}, {Key: "o", Value: bson.D{{Key: "_id", Value: int32(1)}}}})
					}
					txn := int64(745)
					log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
						Operation: "c", Namespace: "admin.$cmd", Timestamp: primitive.Timestamp{T: 1793302669, I: 33},
						LSID:      utils.MarshalData(bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}}),
						TxnNumber: &txn, Object: bson.D{{Key: "applyOps", Value: ops}},
					}}
					if phase != "unprepared_final" {
						log.Object = append(log.Object, bson.E{Key: phase, Value: true})
					}
					before, err := oplog.NewTxnMeta(log.ParsedLog)
					require.NoError(t, err)
					require.False(t, chain.IterateFilter(log), "retain lifecycle even when there is no selected business operation")
					kept, err := oplog.NormalizeApplyOps(log.Object)
					require.NoError(t, err)
					require.Empty(t, kept, "excluded operations must not leak into the transaction")
					after, err := oplog.NewTxnMeta(log.ParsedLog)
					require.NoError(t, err)
					require.Equal(t, before, after, "filtering must retain transaction identity and phase")
				})
			}
		}
	}
}

func TestFiltersStillDropNonTransactionApplyOps(t *testing.T) {
	require.NoError(t, utils.InitialLogger("", "", "error", true, 1))
	for _, kind := range []string{"namespace", "op_type", "vectored_insert", "explicit_command_filter"} {
		t.Run(kind, func(t *testing.T) {
			log := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{Operation: "c", Namespace: "admin.$cmd",
				Object: bson.D{{Key: "applyOps", Value: bson.A{bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "other.docs"}, {Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}}}}},
			}}
			var f OplogFilter = NewNamespaceFilter([]string{"settlement"}, nil)
			if kind == "op_type" || kind == "explicit_command_filter" {
				f = NewOpTypeFilter([]string{"i"})
			}
			if kind == "vectored_insert" || kind == "explicit_command_filter" {
				txn := int64(745)
				log.TxnNumber = &txn
				log.LSID = utils.MarshalData(bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}})
			}
			if kind == "vectored_insert" {
				multi := int(1)
				log.MultiOpType = &multi
			}
			if kind == "explicit_command_filter" {
				f = NewOpTypeFilter([]string{"c"})
			}
			require.True(t, f.Filter(log), "keep existing filtering for non-transactions and explicit command exclusion")
		})
	}
}
