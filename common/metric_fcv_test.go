package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestReplicationMetricCounters(t *testing.T) {
	m := NewMetric("it", "incr", 0)
	require.NotNil(t, m.TableOperations)

	m.AddGet(3)
	m.AddApply(2)
	m.AddSuccess(2)
	m.AddFailed(1)
	m.AddRetransmission(4)
	m.AddTunnelTraffic(1024)
	m.AddFilter(5)
	m.AddConsume(6)
	m.AddCheckpoint(7)
	m.AddWriteFailed(8)
	m.AddTableOps("db.coll", 9)

	assert.Equal(t, uint64(3), m.Get())
	assert.Equal(t, uint64(2), m.Apply())
	assert.Equal(t, uint64(2), m.Success())
	assert.Equal(t, uint64(4), m.Retransmission)
	assert.Equal(t, uint64(7), m.CheckpointTimes)

	// Tps reads the delta, not the cumulative value
	m.OplogSuccess.Delta = 11
	assert.Equal(t, uint64(11), m.Tps())

	ops := m.TableOps()
	assert.Equal(t, uint64(9), ops["db.coll"])
	// TableOps returns a copy: mutating it must not affect the metric
	ops["db.coll"] = 999
	assert.Equal(t, uint64(9), m.TableOps()["db.coll"])

	assert.NotEmpty(t, m.getTunnelTraffic())
}

func TestMetricLSNForwardOnly(t *testing.T) {
	m := NewMetric("it", "incr", 0)
	m.SetLSN(100)
	m.SetLSN(50) // must not move backwards
	assert.Equal(t, int64(100), m.LSN)
	m.SetLSNACK(80)
	m.SetLSNCheckpoint(60)
	m.SetLSNCheckpoint(90)
	assert.Equal(t, int64(90), m.LSNCheckpoint)
	m.SetOplogMax(10)
	m.SetOplogMax(5)
	assert.Equal(t, int64(10), m.OplogMaxSize)
	// running average: (prev + new) / 2, integer division
	m.SetOplogAvg(10)
	assert.Equal(t, int64(5), m.OplogAvgSize)
	m.SetOplogAvg(20)
	assert.Equal(t, int64(12), m.OplogAvgSize)
}

func TestFcvIsCompatible(t *testing.T) {
	c := Checkpoint{CurrentVersion: 1, FeatureCompatibleVersion: 0}
	assert.True(t, c.IsCompatible(0))
	assert.True(t, c.IsCompatible(1))
	assert.False(t, c.IsCompatible(2))
	assert.False(t, c.IsCompatible(-1))

	cfg := Configuration{CurrentVersion: 1, FeatureCompatibleVersion: 1}
	assert.True(t, cfg.IsCompatible(1))
	assert.False(t, cfg.IsCompatible(0))
	assert.False(t, cfg.IsCompatible(2))
}

func TestIsCollectionCappedError(t *testing.T) {
	assert.True(t, IsCollectionCappedError(errors.New("x "+CollectionCapped+" y")))
	assert.True(t, IsCollectionCappedError(errors.New(CollectionCappedLowVersion)))
	assert.False(t, IsCollectionCappedError(errors.New("some other error")))
}

func TestFindFirstErrorIndexAndMessageN(t *testing.T) {
	idx, msg, isDup := FindFirstErrorIndexAndMessageN(nil)
	assert.Equal(t, 0, idx)
	assert.Empty(t, msg)
	assert.False(t, isDup)

	idx, msg, isDup = FindFirstErrorIndexAndMessageN(errors.New("plain"))
	assert.Equal(t, 0, idx)
	assert.Equal(t, "plain", msg)
	assert.False(t, isDup)

	bulk := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 2, Code: 11000, Message: "E11000 duplicate key"}},
	}}
	idx, msg, isDup = FindFirstErrorIndexAndMessageN(bulk)
	assert.Equal(t, 2, idx)
	assert.Equal(t, "E11000 duplicate key", msg)
	assert.True(t, isDup)

	bulk = mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 1, Code: 26, Message: "ns not found"}},
	}}
	idx, _, isDup = FindFirstErrorIndexAndMessageN(bulk)
	assert.Equal(t, 1, idx)
	assert.False(t, isDup)

	idx, msg, isDup = FindFirstErrorIndexAndMessageN(mongo.BulkWriteException{})
	assert.Equal(t, 0, idx)
	assert.Empty(t, msg)
	assert.False(t, isDup)
}

func TestGetMetricWithSize(t *testing.T) {
	assert.NotEmpty(t, GetMetricWithSize(uint64(1024)))
	assert.NotEmpty(t, GetMetricWithSize(uint32(1024)))
	assert.NotEmpty(t, GetMetricWithSize(uint16(1024)))
	assert.NotEmpty(t, GetMetricWithSize(int64(1024)))
	assert.NotEmpty(t, GetMetricWithSize("1024"))
}
