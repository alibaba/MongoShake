package collector

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestWorkerACKNotificationsAreCoalesced(t *testing.T) {
	syncer := &OplogSyncer{workerAckNotify: make(chan struct{}, 1)}
	worker := &Worker{syncer: syncer}
	for ack := int64(1); ack <= 100; ack++ {
		worker.setAck(ack)
	}
	require.Equal(t, int64(100), worker.GetAck())
	require.Len(t, syncer.workerAckNotify, 1, "a slow batcher must not block ACK updates")
	<-syncer.workerAckNotify
	worker.setAck(100)
	require.Empty(t, syncer.workerAckNotify, "unchanged ACKs need no notification")
	worker.setAck(101)
	require.Len(t, syncer.workerAckNotify, 1)
	// Workers used without a notification channel still update ACK normally.
	worker.syncer = &OplogSyncer{}
	worker.setAck(102)
	require.Equal(t, int64(102), worker.GetAck())
}

func TestWorkerProbeIntervalIndependentOfBarrier(t *testing.T) {
	probeInterval := WorkerProbeInterval
	oldInterval := BarrierCheckpointInterval
	BarrierCheckpointInterval = time.Millisecond
	t.Cleanup(func() { BarrierCheckpointInterval = oldInterval })
	worker := &Worker{queue: make(chan []*oplog.GenericOplog, 1)}
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		worker.findFirstAvailableBatch()
		done <- time.Since(start)
	}()
	select {
	case elapsed := <-done:
		require.GreaterOrEqual(t, elapsed, probeInterval)
		require.Equal(t, probeInterval, WorkerProbeInterval)
	case <-time.After(time.Second):
		worker.queue <- nil
		<-done
		t.Fatal("idle worker did not reach its probe interval")
	}
}

func TestObservePutDelayUsesLatestAckedOplog(t *testing.T) {
	now := time.Unix(1_700_000_200, 0).UTC()
	firstTS := now.Add(-20 * time.Second)
	secondTS := now.Add(-30 * time.Second)
	thirdTS := now.Add(-2 * time.Second)
	secondSourceTime := now.Add(-8 * time.Second)

	metric := utils.NewMetric("rs-put-delay", utils.TypeIncr, 0)
	defer metric.Close()

	syncer := &OplogSyncer{
		replMetric: metric,
	}
	worker := &Worker{
		syncer: syncer,
		listUnACK: []*oplog.GenericOplog{
			{
				SourceTime: firstTS,
				Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: primitive.Timestamp{T: uint32(firstTS.Unix()), I: 1},
					},
				},
			},
			{
				SourceTime: secondSourceTime,
				Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: primitive.Timestamp{T: uint32(secondTS.Unix()), I: 2},
					},
				},
			},
			{
				SourceTime: thirdTS,
				Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: primitive.Timestamp{T: uint32(thirdTS.Unix()), I: 3},
					},
				},
			},
		},
	}

	ack := utils.TimeStampToInt64(primitive.Timestamp{T: uint32(secondTS.Unix()), I: 2})
	worker.observePutDelay(ack, now)

	assert.Equal(t, int64(8_000), atomic.LoadInt64(&metric.OplogPutDelay), "should be equal")
}

func TestObservePutDelayFallsBackToTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_260, 0).UTC()
	secondTS := now.Add(-6 * time.Second)

	metric := utils.NewMetric("rs-put-delay-fallback", utils.TypeIncr, 0)
	defer metric.Close()

	worker := &Worker{
		syncer: &OplogSyncer{
			replMetric: metric,
		},
		listUnACK: []*oplog.GenericOplog{
			{
				Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: primitive.Timestamp{T: uint32(secondTS.Unix()), I: 2},
					},
				},
			},
		},
	}

	ack := utils.TimeStampToInt64(primitive.Timestamp{T: uint32(secondTS.Unix()), I: 2})
	worker.observePutDelay(ack, now)

	assert.Equal(t, int64(6_000), atomic.LoadInt64(&metric.OplogPutDelay), "should be equal")
}
