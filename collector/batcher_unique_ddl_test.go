package collector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

type recordingHandler struct {
	logs []*oplog.PartialLog
}

func (h *recordingHandler) Handle(log *oplog.PartialLog) {
	h.logs = append(h.logs, log)
}

func uniqueDDLTestSyncer(hasher oplog.Hasher) *OplogSyncer {
	return &OplogSyncer{
		hasher:                 hasher,
		fullSyncFinishPosition: utils.TimeToTimestamp(0),
		replMetric:             utils.NewMetric("test", "", 0),
	}
}

func uniqueDDLTestBatcher(syncer *OplogSyncer) *Batcher {
	// other tests of this package leave a sentinel exit point behind, which
	// would make getBatchWithDelay cut the injected batch before its DDL
	utils.IncrSentinelOptions.ExitPoint = -1
	utils.IncrSentinelOptions.TargetDelay = -1

	workers := []*Worker{
		{queue: make(chan []*oplog.GenericOplog, 10)},
		{queue: make(chan []*oplog.GenericOplog, 10)},
	}
	batcher := NewBatcher(syncer, filter.OplogFilterChain{new(filter.NoopFilter)}, &recordingHandler{}, workers)
	batcher.utBatchesDelay.flag = true
	return batcher
}

func uniqueCreateIndexesDDL() *oplog.PartialLog {
	return &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
		Operation: "c",
		Namespace: "db.$cmd",
		Timestamp: utils.TimeToTimestamp(time.Now().Unix()),
		Object: bson.D{
			{Key: "createIndexes", Value: "ordered"},
			{Key: "indexes", Value: bson.A{
				bson.D{
					{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
					{Key: "name", Value: "a_1"},
					{Key: "unique", Value: true},
				},
			}},
		},
	}}
}

// TestBatchMoreSwitchesNamespaceOnUniqueIndexDDL pins the DDL hook: a unique
// index created at runtime must move the namespace to ordered replay. The DDL
// is a barrier, so every oplog dispatched before it has been acknowledged by
// the time the next batch is routed.
func TestBatchMoreSwitchesNamespaceOnUniqueIndexDDL(t *testing.T) {
	oldDDL, oldDelay := conf.Options.FilterDDLEnable, conf.Options.IncrSyncTargetDelay
	defer func() {
		conf.Options.FilterDDLEnable = oldDDL
		conf.Options.IncrSyncTargetDelay = oldDelay
	}()
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncTargetDelay = 0

	hasher := oplog.NewUniqueIndexAwareHasher(nil)
	require.False(t, hasher.IsOrdered("db.ordered"))

	batcher := uniqueDDLTestBatcher(uniqueDDLTestSyncer(hasher))
	batcher.utBatchesDelay.injectBatch = []*oplog.GenericOplog{{Parsed: uniqueCreateIndexesDDL()}}

	batchGroup, barrier, _, _ := batcher.BatchMore()

	require.True(t, barrier, "a DDL must be reported as a barrier")
	assert.True(t, hasher.IsOrdered("db.ordered"),
		"the namespace must replay in order from the barrier onwards")
	assert.Equal(t, []string{"db.ordered"}, hasher.OrderedNamespaces())
	for _, group := range batchGroup {
		assert.Empty(t, group, "the DDL is held back until the barrier round")
	}

	// the barrier round hands the DDL to worker 0
	batchGroup, barrier, _, _ = batcher.BatchMore()
	require.True(t, barrier)
	require.Len(t, batchGroup, 2)
	assert.Len(t, batchGroup[0], 1, "the barrier DDL goes to worker 0")
	assert.Empty(t, batchGroup[1])
}

// TestBatchMoreDoesNotSwitchWhenDDLFiltered: with DDL replay disabled the index
// never reaches the target, so the namespace must keep the _id dispatch.
func TestBatchMoreDoesNotSwitchWhenDDLFiltered(t *testing.T) {
	oldDDL, oldDelay := conf.Options.FilterDDLEnable, conf.Options.IncrSyncTargetDelay
	defer func() {
		conf.Options.FilterDDLEnable = oldDDL
		conf.Options.IncrSyncTargetDelay = oldDelay
	}()
	conf.Options.FilterDDLEnable = false
	conf.Options.IncrSyncTargetDelay = 0

	hasher := oplog.NewUniqueIndexAwareHasher(nil)
	batcher := uniqueDDLTestBatcher(uniqueDDLTestSyncer(hasher))
	batcher.utBatchesDelay.injectBatch = []*oplog.GenericOplog{{Parsed: uniqueCreateIndexesDDL()}}

	batchGroup, barrier, _, _ := batcher.BatchMore()

	assert.False(t, barrier)
	assert.False(t, hasher.IsOrdered("db.ordered"))
	for _, group := range batchGroup {
		assert.Empty(t, group)
	}
}

// TestBatchMoreDDLWithLegacyHasher: shard_key=collection/id install other
// hashers. A DDL must keep working instead of failing on a type assertion.
func TestBatchMoreDDLWithLegacyHasher(t *testing.T) {
	oldDDL, oldDelay := conf.Options.FilterDDLEnable, conf.Options.IncrSyncTargetDelay
	defer func() {
		conf.Options.FilterDDLEnable = oldDDL
		conf.Options.IncrSyncTargetDelay = oldDelay
	}()
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncTargetDelay = 0

	for _, hasher := range []oplog.Hasher{&oplog.PrimaryKeyHasher{}, &oplog.TableHasher{}} {
		batcher := uniqueDDLTestBatcher(uniqueDDLTestSyncer(hasher))
		batcher.utBatchesDelay.injectBatch = []*oplog.GenericOplog{{Parsed: uniqueCreateIndexesDDL()}}

		assert.NotPanics(t, func() { batcher.BatchMore() })
	}
}

// TestBatchMoreApplyOpsUniqueIndexDoesNotSwitch: applyOps inner ops are
// dispatched without a barrier, so the switch is refused (and logged) instead
// of routing the rest of the batch under a new hasher.
func TestBatchMoreApplyOpsUniqueIndexDoesNotSwitch(t *testing.T) {
	oldDDL, oldDelay := conf.Options.FilterDDLEnable, conf.Options.IncrSyncTargetDelay
	defer func() {
		conf.Options.FilterDDLEnable = oldDDL
		conf.Options.IncrSyncTargetDelay = oldDelay
	}()
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncTargetDelay = 0

	hasher := oplog.NewUniqueIndexAwareHasher(nil)
	batcher := uniqueDDLTestBatcher(uniqueDDLTestSyncer(hasher))
	batcher.utBatchesDelay.injectBatch = []*oplog.GenericOplog{{Parsed: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{
			Operation: "c",
			Namespace: "admin.$cmd",
			Timestamp: utils.TimeToTimestamp(time.Now().Unix()),
			Object: bson.D{
				{Key: "applyOps", Value: bson.A{
					bson.D{
						{Key: "op", Value: "c"},
						{Key: "ns", Value: "db.$cmd"},
						{Key: "o", Value: bson.D{
							{Key: "createIndexes", Value: "c"},
							{Key: "indexes", Value: bson.A{
								bson.D{
									{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
									{Key: "name", Value: "a_1"},
									{Key: "unique", Value: true},
								},
							}},
						}},
					},
				}},
			},
		},
	}}}

	require.NotPanics(t, func() { batcher.BatchMore() })
	assert.False(t, hasher.IsOrdered("db.c"),
		"an index build inside applyOps cannot take effect at a barrier, so the routing must not change")
}
