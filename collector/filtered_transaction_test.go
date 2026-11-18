package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/alibaba/MongoShake/v2/tunnel"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func newFilteredTxnBatcher(t *testing.T, mode string) *Batcher {
	t.Helper()
	saved := conf.Options
	savedSentinel := utils.IncrSentinelOptions
	t.Cleanup(func() { utils.IncrSentinelOptions = savedSentinel })
	utils.IncrSentinelOptions.OplogDump = 0
	utils.IncrSentinelOptions.TargetDelay = -1
	utils.IncrSentinelOptions.ExitPoint = -1
	t.Cleanup(func() { conf.Options = saved })
	conf.Options.FilterDDLEnable = true
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true
	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
	conf.Options.IncrSyncTargetDelay = 0
	chain := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	if mode == "op_type" || mode == "combined" {
		chain = append(chain, filter.NewOpTypeFilter([]string{"i"}))
	}
	if mode != "none" && mode != "op_type" {
		chain = append(chain, filter.NewNamespaceFilter([]string{"settlement"}, nil))
	}
	// Keep this fixture independent of process-global logger initialization and
	// live metric tickers; neither is needed to exercise transaction replay.
	const queues = 3
	syncer := &OplogSyncer{
		PendingQueue:           make([]chan [][]byte, queues),
		logsQueue:              make([]chan []*oplog.GenericOplog, queues),
		hasher:                 &oplog.PrimaryKeyHasher{},
		fullSyncFinishPosition: utils.TimeToTimestamp(0),
		replMetric:             &utils.ReplicationMetric{NAME: "test", TableOperations: utils.NewTableOps()},
	}
	for i := 0; i < queues; i++ {
		syncer.logsQueue[i] = make(chan []*oplog.GenericOplog, 100)
	}
	batcher := NewBatcher(syncer, chain, syncer, []*Worker{new(Worker)})
	syncer.batcher = batcher
	batcher.batchGroup = make([][]*oplog.GenericOplog, 1)
	t.Cleanup(batcher.Fini)
	return batcher
}

// Match the MongoDB 8.2.7 production shape: empty prepare at (1793302669,33),
// commit at (...,36), with prevOpTime pointing to the empty prepare.
func filteredTxnEntry(t *testing.T, inc, prev uint32, txn int64, command bson.D) *oplog.GenericOplog {
	t.Helper()
	prevTime := primitive.Timestamp{}
	term := int64(-1)
	if prev > 0 {
		prevTime = primitive.Timestamp{T: 1793302669, I: prev}
		term = 8
	}
	opTerm := int64(8)
	log := oplog.ParsedLog{
		Timestamp: primitive.Timestamp{T: 1793302669, I: inc}, Term: &opTerm, Operation: "c", Namespace: "admin.$cmd", Object: command,
		LSID:      utils.MarshalData(bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: []byte{0xdf, 0x58, 0x22, 0xe0, 0x25, 0x29, 0x42, 0xb1, 0x9d, 0x8f, 0x4e, 0x2f, 0x17, 0x67, 0x2e, 0xe7}}}}),
		TxnNumber: &txn, PrevOpTime: utils.MarshalData(bson.D{{Key: "ts", Value: prevTime}, {Key: "t", Value: term}}),
	}
	raw, err := bson.Marshal(log)
	require.NoError(t, err)
	return &oplog.GenericOplog{Raw: raw, Parsed: &oplog.PartialLog{ParsedLog: log}, SourceTime: time.Unix(1793302669, 0)}
}

func filteredTxnInsert(ns string, id int32) bson.D {
	return bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: ns}, {Key: "o", Value: bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: bson.D{{Key: "typed", Value: int64(42)}}}}}}
}

func filteredTxnTs(inc uint32) int64 {
	return utils.TimeStampToInt64(primitive.Timestamp{T: 1793302669, I: inc})
}

func TestFilteredTransactionEmptyCompletion(t *testing.T) {
	for _, mode := range []string{"namespace", "namespace_filtered", "op_type", "combined", "none"} {
		for _, phase := range []string{"prepare", "partialTxn", "unprepared_final"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				batcher := newFilteredTxnBatcher(t, mode)
				ops := bson.A{}
				if mode != "none" {
					ops = append(ops, filteredTxnInsert("other.docs", 1))
				}
				// Namespace mode additionally covers the exact source-empty incident.
				if mode == "namespace" {
					ops = bson.A{}
				}
				cmd := bson.D{{Key: "applyOps", Value: ops}}
				if phase != "unprepared_final" {
					cmd = append(cmd, bson.E{Key: phase, Value: true})
				}
				first := filteredTxnEntry(t, 33, 0, 745, cmd)
				require.NotPanics(t, func() { require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{first})) })
				if phase != "unprepared_final" {
					require.Equal(t, filteredTxnTs(33), batcher.OldestUnackedTxnPinTs(0), "retain the empty envelope until the final transaction record")
					require.Equal(t, filteredTxnTs(33)-1, batcher.syncer.clampCheckpointByTxnPin(filteredTxnTs(35), 0))
					finalCmd := bson.D{{Key: "commitTransaction", Value: 1}, {Key: "commitTimestamp", Value: primitive.Timestamp{T: 1793302669, I: 34}}}
					if phase == "partialTxn" {
						finalCmd = bson.D{{Key: "applyOps", Value: bson.A{}}}
					}
					final := filteredTxnEntry(t, 36, 33, 745, finalCmd)
					require.NotPanics(t, func() { require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{final})) })
				}
				require.Zero(t, batcher.txnBuffer.Size())
				require.Zero(t, batcher.OldestUnackedTxnPinTs(0), "no worker ACK exists for an empty transaction")
				require.Empty(t, batcher.barrierOplogs)
				require.Empty(t, batcher.batchGroup[0])
				wantInc := uint32(36)
				if phase == "unprepared_final" {
					wantInc = 33
				}
				require.Equal(t, filteredTxnTs(wantInc), utils.TimeStampToInt64(batcher.lastFilterOplog.Timestamp), "record safe filtered progress at the final oplog")
				require.Equal(t, filteredTxnTs(40), batcher.syncer.clampCheckpointByTxnPin(filteredTxnTs(40), 0))
			})
		}
	}
}

func TestFilteredTransactionMultipartKeepsSelectedData(t *testing.T) {
	batcher := newFilteredTxnBatcher(t, "namespace")
	kept := filteredTxnInsert("settlement.docs", 2)
	partial := filteredTxnEntry(t, 30, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{filteredTxnInsert("other.docs", 1), kept}}, {Key: "partialTxn", Value: true}})
	emptyPrepare := filteredTxnEntry(t, 33, 30, 745, bson.D{{Key: "applyOps", Value: bson.A{}}, {Key: "prepare", Value: true}})
	require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{partial, emptyPrepare}))
	require.Equal(t, filteredTxnTs(30), batcher.OldestUnackedTxnPinTs(0))
	commit := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: "commitTransaction", Value: 1}})
	require.Equal(t, 0, batcher.processMergeBatch([]*oplog.GenericOplog{commit}))
	require.Len(t, batcher.barrierOplogs, 1)
	replayed := batcher.barrierOplogs[0]
	require.Equal(t, "settlement.docs", replayed.Parsed.Namespace)
	require.Equal(t, oplog.GetKey(kept, "o"), replayed.Parsed.Object, "compare full selected BSON, including nested numeric types")
	require.Equal(t, partial.SourceTime, replayed.SourceTime)
	// Use the real dispatch -> Worker.transfer -> WriteController.Send path.
	// The tunnel is an in-process sink; the production controller derives the
	// ACK from the actual last dispatched operation rather than a synthetic commit ACK.
	posted := newFilteredTxnCheckpoint(t, batcher, filteredTxnTs(20))
	worker := batcher.workerGroup[0]
	worker.syncer = batcher.syncer
	worker.queue = make(chan []*oplog.GenericOplog, 1)
	worker.writeController = &WriteController{worker: worker, tunnel: &tunnel.MockWriter{}}
	groups, barrier, empty, exit := batcher.BatchMore()
	require.True(t, barrier)
	require.False(t, empty)
	require.False(t, exit)
	require.True(t, batcher.dispatchBatches(groups))
	worker.transfer(<-worker.queue)
	actualAck := worker.GetAck()
	require.Equal(t, utils.TimeStampToInt64(replayed.Parsed.Timestamp), actualAck)
	require.Equal(t, filteredTxnTs(30), actualAck)
	require.Equal(t, filteredTxnTs(30), batcher.OldestUnackedTxnPinTs(actualAck))
	require.True(t, batcher.syncer.checkpoint(true, 0))
	require.Equal(t, []int64{filteredTxnTs(30) - 1}, posted())
	// Existing nonempty prepared tail ACK/pin convergence remains outside this
	// change. Record its real protected checkpoint; do not invent ACK @36.
	require.Less(t, batcher.syncer.ckptManager.GetInMemory().Timestamp, filteredTxnTs(30))
}

func TestFilteredTransactionEmptyFinalKeepsOtherPins(t *testing.T) {
	for _, command := range []string{"abortTransaction", "commitTransaction"} {
		t.Run(command, func(t *testing.T) {
			batcher := newFilteredTxnBatcher(t, "namespace")
			pending := filteredTxnEntry(t, 20, 0, 744, bson.D{{Key: "applyOps", Value: bson.A{filteredTxnInsert("settlement.docs", 1)}}, {Key: "partialTxn", Value: true}})
			require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{pending}))
			empty := filteredTxnEntry(t, 33, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{}}, {Key: "prepare", Value: true}})
			final := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: command, Value: 1}})
			require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{empty, final}))
			require.Equal(t, 1, batcher.txnBuffer.Size())
			require.Equal(t, filteredTxnTs(20), batcher.OldestUnackedTxnPinTs(0), "empty completion must not release another transaction")
			require.Equal(t, filteredTxnTs(20)-1, batcher.syncer.clampCheckpointByTxnPin(filteredTxnTs(40), 0))
			require.Equal(t, final.Parsed, batcher.lastFilterOplog)
		})
	}
}

func TestFilteredTransactionOrphanCommitRejected(t *testing.T) {
	// Both channels are ready on an orphan error. Check repeatedly that a
	// closed ops channel cannot hide the error queued on the error channel.
	for i := 0; i < 16; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			batcher := newFilteredTxnBatcher(t, "namespace")
			commit := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: "commitTransaction", Value: 1}})
			var failure any
			func() {
				defer func() { failure = recover() }()
				batcher.processMergeBatch([]*oplog.GenericOplog{commit})
			}()
			require.NotNil(t, failure, "a real orphan commit must never be acknowledged as a filtered empty transaction")
			require.Contains(t, fmt.Sprint(failure), "has no buffered data entries")
			require.Same(t, fakeOplog.Parsed, batcher.lastFilterOplog)
			require.NotZero(t, batcher.OldestUnackedTxnPinTs(0))
		})
	}
}

func TestFilteredTransactionEmptyCommitContinuesDDL(t *testing.T) {
	batcher := newFilteredTxnBatcher(t, "namespace")
	normal := func(inc uint32) *oplog.GenericOplog {
		return &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Timestamp: primitive.Timestamp{T: 1793302669, I: inc}, Operation: "i", Namespace: "settlement.docs",
			Object: bson.D{{Key: "_id", Value: int32(inc)}},
		}}}
	}
	before, after := normal(31), normal(40)
	prepare := filteredTxnEntry(t, 33, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{}}, {Key: "prepare", Value: true}})
	commit := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: "commitTransaction", Value: 1}})
	ddl := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: primitive.Timestamp{T: 1793302669, I: 38}, Operation: "c", Namespace: "settlement.$cmd", Object: bson.D{{Key: "drop", Value: "docs"}},
	}}}
	batcher.syncer.logsQueue[0] <- []*oplog.GenericOplog{before, prepare, commit, ddl, after}
	for i, want := range []*oplog.GenericOplog{before, ddl, after} {
		var groups [][]*oplog.GenericOplog
		var barrier, empty, exit bool
		require.NotPanics(t, func() { groups, barrier, empty, exit = batcher.BatchMore() })
		require.False(t, exit)
		require.False(t, empty)
		require.Equal(t, i < 2, barrier)
		require.Len(t, groups[0], 1)
		require.Same(t, want, groups[0][0], "empty commit must not stall or reorder the DML/DDL boundary")
		require.Zero(t, batcher.OldestUnackedTxnPinTs(0))
	}
	require.Equal(t, commit.Parsed, batcher.lastFilterOplog)
}

func TestFilteredTransactionMalformedDataRejected(t *testing.T) {
	batcher := newFilteredTxnBatcher(t, "namespace")
	broken := filteredTxnEntry(t, 33, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{"not an oplog document"}}, {Key: "prepare", Value: true}})
	commit := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: "commitTransaction", Value: 1}})
	require.Equal(t, -1, batcher.processMergeBatch([]*oplog.GenericOplog{broken}))
	var failure any
	func() {
		defer func() { failure = recover() }()
		batcher.processMergeBatch([]*oplog.GenericOplog{commit})
	}()
	require.NotNil(t, failure)
	require.Contains(t, fmt.Sprint(failure), "applyOps op: not a BSON document")
	require.Same(t, fakeOplog.Parsed, batcher.lastFilterOplog)
	require.Equal(t, filteredTxnTs(33), batcher.OldestUnackedTxnPinTs(0))
}

// This loopback HTTP stub exercises the checkpoint manager's write path without
// a MongoDB instance. It is not evidence of real database durability.
func newFilteredTxnCheckpoint(t *testing.T, batcher *Batcher, initial int64) func() []int64 {
	t.Helper()
	var mu sync.Mutex
	persisted := initial
	var writes []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{Name: "filtered-chain", Timestamp: persisted, Version: utils.FcvCheckpoint.CurrentVersion, FetchMethod: "oplog"})
		case http.MethodPost:
			var next ckpt.CheckpointContext
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			persisted = next.Timestamp
			writes = append(writes, next.Timestamp)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = false
	conf.Options.CheckpointInterval = 0
	manager := ckpt.NewCheckpointManager("filtered-chain", initial)
	_, _, err := manager.Get()
	require.NoError(t, err)
	batcher.syncer.ckptManager = manager
	return func() []int64 { mu.Lock(); defer mu.Unlock(); return append([]int64(nil), writes...) }
}

func TestFilteredTransactionMissingHistoryBlocksCheckpoint(t *testing.T) {
	for _, kind := range []string{"missing_first_prepare", "missing_first_final", "missing_middle", "wrong_term", "wrong_commit_predecessor"} {
		t.Run(kind, func(t *testing.T) {
			batcher := newFilteredTxnBatcher(t, "namespace")
			initial := filteredTxnTs(10)
			writes := newFilteredTxnCheckpoint(t, batcher, initial)
			partial := filteredTxnEntry(t, 30, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{filteredTxnInsert("settlement.docs", 1)}}, {Key: "partialTxn", Value: true}})
			prepare := filteredTxnEntry(t, 33, 30, 745, bson.D{{Key: "applyOps", Value: bson.A{}}, {Key: "prepare", Value: true}})
			commit := filteredTxnEntry(t, 36, 33, 745, bson.D{{Key: "commitTransaction", Value: 1}})
			input := []*oplog.GenericOplog{partial, prepare, commit}
			switch kind {
			case "missing_first_prepare":
				input = input[1:]
			case "missing_first_final":
				input = []*oplog.GenericOplog{filteredTxnEntry(t, 36, 30, 745, bson.D{{Key: "applyOps", Value: bson.A{}}})}
			case "missing_middle":
				input[0] = filteredTxnEntry(t, 20, 0, 745, bson.D{{Key: "applyOps", Value: bson.A{filteredTxnInsert("settlement.docs", 1)}}, {Key: "partialTxn", Value: true}})
			case "wrong_term":
				prepare.Parsed.PrevOpTime = utils.MarshalData(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}, {Key: "t", Value: int64(9)}})
			case "wrong_commit_predecessor":
				commit.Parsed.PrevOpTime = utils.MarshalData(bson.D{{Key: "ts", Value: partial.Parsed.Timestamp}, {Key: "t", Value: int64(8)}})
			}
			var failure any
			func() { defer func() { failure = recover() }(); batcher.processMergeBatch(input) }()
			if failure == nil {
				// Emulate the dangerous filtered-progress flush only when the
				// collector did not reject the missing history.
				batcher.syncer.checkpoint(true, filteredTxnTs(36))
			}
			require.Empty(t, writes(), "a broken chain must not reach checkpoint persistence")
			require.Equal(t, initial, batcher.syncer.ckptManager.GetInMemory().Timestamp)
			require.NotNil(t, failure)
			require.Contains(t, fmt.Sprint(failure), "transaction oplog chain")
			require.Same(t, fakeOplog.Parsed, batcher.lastFilterOplog)
			require.Empty(t, batcher.barrierOplogs)
			if kind == "missing_first_prepare" || kind == "missing_first_final" {
				require.Zero(t, batcher.txnBuffer.Size(), "reject a broken first envelope before registering it")
				require.Zero(t, batcher.OldestUnackedTxnPinTs(0))
			} else {
				require.NotZero(t, batcher.OldestUnackedTxnPinTs(0), "keep the already buffered prefix protected")
			}
		})
	}
}

func TestFilteredTransactionMissingHistoryFailsBeforeCommit(t *testing.T) {
	batcher := newFilteredTxnBatcher(t, "namespace")
	initial := filteredTxnTs(10)
	writes := newFilteredTxnCheckpoint(t, batcher, initial)
	prepare := filteredTxnEntry(t, 33, 30, 745, bson.D{{Key: "applyOps", Value: bson.A{}}, {Key: "prepare", Value: true}})
	var failure any
	func() {
		defer func() { failure = recover() }()
		batcher.processMergeBatch([]*oplog.GenericOplog{prepare})
	}()
	if failure == nil {
		batcher.syncer.checkpoint(true, filteredTxnTs(35))
	}
	require.Empty(t, writes(), "missing predecessor must be rejected before intervening DML can drive checkpoint")
	require.Equal(t, initial, batcher.syncer.ckptManager.GetInMemory().Timestamp)
	require.NotNil(t, failure, "do not defer a known broken chain until a commit that may never arrive")
	require.Contains(t, fmt.Sprint(failure), "transaction oplog chain")
}

// Test fixtures must leave the shared logger owned by the surrounding suite.
func TestFilteredTxnBatcherPreservesSharedLogger(t *testing.T) {
	shared := l.Logger
	newFilteredTxnBatcher(t, "namespace")
	require.Same(t, shared, l.Logger, "creating a transaction fixture must not replace the shared logger")
}

// An isolated child keeps unrelated legacy metric goroutines out of this
// fixture-lifetime assertion. The child runs the same helper and its cleanup.
func TestFilteredTxnBatcherDoesNotLeaveMetricGoroutines(t *testing.T) {
	const childEnv = "MONGOSHAKE_FILTERED_TXN_HELPER_CHILD"
	if os.Getenv(childEnv) == "1" {
		t.Run("fixture_lifetime", func(t *testing.T) {
			newFilteredTxnBatcher(t, "namespace")
		})
		var stacks bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
		require.NotContains(t, stacks.String(), "(*ReplicationMetric).startup.func1",
			"fixture cleanup must leave no ticking metric goroutine that can race a later logger reset")
		return
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, binary, "-test.run=^TestFilteredTxnBatcherDoesNotLeaveMetricGoroutines$", "-test.count=1", "-test.timeout=10s")
	child.Env = append(os.Environ(), childEnv+"=1")
	output, err := child.CombinedOutput()
	require.NoError(t, err, "isolated fixture-lifetime check: %s", output)
}
