package collector

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

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
