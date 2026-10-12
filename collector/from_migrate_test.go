package collector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func newFromMigrateTestBatcher(t *testing.T) *Batcher {
	t.Helper()
	if l.Logger == nil {
		require.NoError(t, utils.InitialLogger("", "", "error", true, 2))
	}
	oldOptions := conf.Options
	oldTargetDelay := utils.IncrSentinelOptions.TargetDelay
	oldExitPoint := utils.IncrSentinelOptions.ExitPoint
	t.Cleanup(func() {
		conf.Options = oldOptions
		utils.IncrSentinelOptions.TargetDelay = oldTargetDelay
		utils.IncrSentinelOptions.ExitPoint = oldExitPoint
	})
	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncTargetDelay = 0
	utils.IncrSentinelOptions.TargetDelay = -1
	utils.IncrSentinelOptions.ExitPoint = -1

	syncer := &OplogSyncer{
		logsQueue:  []chan []*oplog.GenericOplog{make(chan []*oplog.GenericOplog, 1)},
		hasher:     &oplog.PrimaryKeyHasher{},
		replMetric: &utils.ReplicationMetric{},
	}
	batcher := NewBatcher(syncer, filter.OplogFilterChain{
		new(filter.AutologousFilter), filter.NewNamespaceFilter(nil, nil), new(filter.NoopFilter),
	}, syncer, []*Worker{new(Worker)})
	syncer.batcher = batcher
	t.Cleanup(batcher.Fini)
	return batcher
}

func TestBatchMoreFiltersTopLevelFromMigrate(t *testing.T) {
	for _, operation := range []string{"i", "u", "d", "c"} {
		t.Run(operation, func(t *testing.T) {
			batcher := newFromMigrateTestBatcher(t)
			migrated := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(100), Operation: operation,
				Namespace: "migration.documents", FromMigrate: true,
				Object: bson.D{{Key: "_id", Value: 1}},
			}}
			if operation == "c" {
				migrated.Namespace = "migration.$cmd"
				migrated.Object = bson.D{{Key: "create", Value: "documents"}}
			}
			batcher.syncer.logsQueue[0] <- []*oplog.GenericOplog{{Parsed: migrated}}
			groups, _, allEmpty, _ := batcher.BatchMore()
			require.True(t, allEmpty, "source migration must not dispatch a target write")
			require.Empty(t, groups[0])
			require.Equal(t, migrated.Timestamp, batcher.lastFilterOplog.Timestamp,
				"filtered migration must still advance the checkpoint candidate")
			require.Equal(t, uint64(1), batcher.syncer.replMetric.OplogFilter.Value)
		})
	}
}

func TestBatchMorePreservesUserWritesWithoutFromMigrate(t *testing.T) {
	for _, flag := range []string{"absent", "false"} {
		for _, operation := range []string{"i", "u", "d"} {
			t.Run(flag+"/"+operation, func(t *testing.T) {
				batcher := newFromMigrateTestBatcher(t)
				document := bson.D{
					{Key: "ts", Value: utils.TimeToTimestamp(100)},
					{Key: "op", Value: operation}, {Key: "ns", Value: "migration.documents"},
					{Key: "o", Value: bson.D{{Key: "_id", Value: 2}}},
				}
				if flag == "false" {
					document = append(document, bson.E{Key: "fromMigrate", Value: false})
				}
				raw, err := bson.Marshal(document)
				require.NoError(t, err)
				parsed, err := oplog.ParseRaw(raw)
				require.NoError(t, err)
				batcher.syncer.logsQueue[0] <- []*oplog.GenericOplog{{Raw: raw, Parsed: parsed}}
				groups, _, allEmpty, _ := batcher.BatchMore()
				require.False(t, allEmpty)
				require.Len(t, groups[0], 1)
				require.Equal(t, operation, groups[0][0].Parsed.Operation)
				require.Equal(t, uint64(0), batcher.syncer.replMetric.OplogFilter.Value)
			})
		}
	}
}

func TestBatchMoreFiltersFromMigrateInsideApplyOps(t *testing.T) {
	userInsert := bson.D{
		{Key: "op", Value: "i"}, {Key: "ns", Value: "migration.documents"},
		{Key: "o", Value: bson.D{{Key: "_id", Value: 2}}},
	}
	cleanupDelete := bson.D{
		{Key: "op", Value: "d"}, {Key: "ns", Value: "migration.documents"},
		{Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "fromMigrate", Value: true},
	}
	for _, path := range []string{"applyOps", "single transaction", "prepared transaction"} {
		for _, test := range []struct {
			name string
			ops  bson.A
			want int
		}{
			{"migration first", bson.A{cleanupDelete, userInsert}, 1},
			{"migration last", bson.A{userInsert, cleanupDelete}, 1},
			{"all migration", bson.A{cleanupDelete}, 0},
			{"user writes only", bson.A{userInsert}, 1},
		} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				batcher := newFromMigrateTestBatcher(t)
				sourceTime := time.Unix(1_700_000_000, 0).UTC()
				parent := &oplog.GenericOplog{SourceTime: sourceTime, Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: primitive.Timestamp{T: 100, I: 2}, Operation: "c", Namespace: "admin.$cmd",
						Object: bson.D{{Key: "applyOps", Value: test.ops}},
					},
				}}
				input := []*oplog.GenericOplog{parent}
				checkpointTs := parent.Parsed.Timestamp
				if path != "applyOps" {
					txnNumber := int64(1)
					parent.Parsed.LSID = utils.MarshalData(bson.D{{Key: "id", Value: primitive.NewObjectID()}})
					parent.Parsed.TxnNumber = &txnNumber
					parent.Parsed.PrevOpTime = append(bson.Raw(nil), emptyPrevRaw...)
					if path == "prepared transaction" {
						parent.Parsed.Object = append(parent.Parsed.Object, bson.E{Key: "prepare", Value: true})
						commitTs := primitive.Timestamp{T: 101, I: 1}
						input = append(input, &oplog.GenericOplog{Parsed: &oplog.PartialLog{
							ParsedLog: oplog.ParsedLog{
								Timestamp: commitTs, Operation: "c", Namespace: "admin.$cmd",
								Object: bson.D{{Key: "commitTransaction", Value: 1},
									{Key: "commitTimestamp", Value: parent.Parsed.Timestamp}},
								LSID: parent.Parsed.LSID, TxnNumber: &txnNumber,
								PrevOpTime: utils.MarshalData(bson.D{{Key: "ts", Value: parent.Parsed.Timestamp},
									{Key: "t", Value: int64(-1)}}),
							},
						}})
					}
				}
				batcher.syncer.logsQueue[0] <- input
				groups, _, allEmpty, _ := batcher.BatchMore()
				dispatched := append([]*oplog.GenericOplog(nil), groups[0]...)
				// Prepared transaction operations are dispatched on the barrier's next pass.
				if len(batcher.barrierOplogs) > 0 {
					groups, _, allEmpty, _ = batcher.BatchMore()
					dispatched = append(dispatched, groups[0]...)
				}
				require.Len(t, dispatched, test.want,
					"only user writes may be dispatched; migration cleanup must stay on the source")
				require.Equal(t, test.want == 0, allEmpty)
				for _, entry := range dispatched {
					require.False(t, entry.Parsed.FromMigrate)
					require.Equal(t, "i", entry.Parsed.Operation)
					require.Equal(t, sourceTime, entry.SourceTime)
				}
				if len(dispatched) > 0 {
					encoded := oplog.LogEntryEncode(dispatched)
					var lastWireOp oplog.ParsedLog
					require.NoError(t, bson.Unmarshal(encoded[len(encoded)-1], &lastWireOp))
					require.Equal(t, checkpointTs, lastWireOp.Timestamp,
						"the parent boundary must also survive serialization to the worker's target")
					require.False(t, lastWireOp.FromMigrate)
					worker := batcher.workerGroup[0]
					worker.ack = utils.TimeStampToInt64(dispatched[len(dispatched)-1].Parsed.Timestamp)
					worker.unack = worker.ack
					checkpoint, err := batcher.syncer.calculateWorkerLowestCheckpoint()
					require.NoError(t, err)
					require.Equal(t, utils.TimeStampToInt64(checkpointTs), checkpoint,
						"after user writes are acknowledged, the worker checkpoint must cover the filtered tail")
				}
				lastLog, lastFiltered := batcher.getLastOplog()
				latest := lastLog.Timestamp
				if primitive.CompareTimestamp(lastFiltered.Timestamp, latest) > 0 {
					latest = lastFiltered.Timestamp
				}
				require.Equal(t, checkpointTs, latest,
					"filtering the last or all inner operations must not lose the parent checkpoint boundary")
				require.Equal(t, uint64(len(test.ops)-test.want), batcher.syncer.replMetric.OplogFilter.Value)
				require.Zero(t, batcher.txnBuffer.Size(), "completed transactions must release their buffer")
			})
		}
	}
}

func TestBatchMoreAllMigrationTransactionKeepsEarlierUserWrite(t *testing.T) {
	batcher := newFromMigrateTestBatcher(t)
	txnNumber := int64(1)
	lsid := utils.MarshalData(bson.D{{Key: "id", Value: primitive.NewObjectID()}})
	prepareTs := primitive.Timestamp{T: 100, I: 2}
	userWrite := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: primitive.Timestamp{T: 99, I: 1}, Operation: "i", Namespace: "migration.documents",
		Object: bson.D{{Key: "_id", Value: 2}},
	}}}
	prepare := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: prepareTs, Operation: "c", Namespace: "admin.$cmd",
		LSID: lsid, TxnNumber: &txnNumber, PrevOpTime: append(bson.Raw(nil), emptyPrevRaw...),
		Object: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "migration.documents"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "fromMigrate", Value: true}},
		}}, {Key: "prepare", Value: true}},
	}}}
	commit := &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Timestamp: primitive.Timestamp{T: 101, I: 1}, Operation: "c", Namespace: "admin.$cmd",
		LSID: lsid, TxnNumber: &txnNumber,
		PrevOpTime: utils.MarshalData(bson.D{{Key: "ts", Value: prepareTs}, {Key: "t", Value: int64(-1)}}),
		Object:     bson.D{{Key: "commitTransaction", Value: 1}, {Key: "commitTimestamp", Value: prepareTs}},
	}}}
	batcher.syncer.logsQueue[0] <- []*oplog.GenericOplog{userWrite, prepare, commit}
	require.NotPanics(t, func() {
		groups, barrier, allEmpty, _ := batcher.BatchMore()
		require.False(t, barrier, "a completely filtered transaction has no target operation to serialize")
		require.False(t, allEmpty)
		require.Equal(t, []*oplog.GenericOplog{userWrite}, groups[0])
	})
	require.Empty(t, batcher.barrierOplogs)
	require.Zero(t, batcher.txnBuffer.Size())
}
