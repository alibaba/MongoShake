package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestGetKeyAndIndex(t *testing.T) {
	doc := bson.D{
		primitive.E{Key: "_id", Value: 7},
		primitive.E{Key: "x", Value: 9},
	}
	assert.Equal(t, 7, GetKey(doc, "_id"))
	assert.Equal(t, 9, GetKey(doc, "x"))
	assert.Nil(t, GetKey(doc, "missing"))

	// empty wanted defaults to _id
	v, idx := GetKeyWithIndex(doc, "")
	assert.Equal(t, 7, v)
	assert.Equal(t, 0, idx)

	v, idx = GetKeyWithIndex(doc, "x")
	assert.Equal(t, 9, v)
	assert.Equal(t, 1, idx)

	v, idx = GetKeyWithIndex(doc, "nope")
	assert.Nil(t, v)
	assert.Equal(t, 0, idx)
}

func TestSetFiledUpdatesExisting(t *testing.T) {
	doc := bson.D{primitive.E{Key: "x", Value: 1}}
	SetFiled(doc, "x", 42, false)
	assert.Equal(t, 42, doc[0].Value)
}

func TestMarshalData(t *testing.T) {
	raw := MarshalData(bson.D{primitive.E{Key: "a", Value: 1}})
	require.NotNil(t, raw)
	var back bson.M
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, int32(1), back["a"])
}

func TestInt64ToString(t *testing.T) {
	assert.Equal(t, "123", Int64ToString(123))
	assert.Equal(t, "-5", Int64ToString(-5))
	assert.Equal(t, "0", Int64ToString(0))
}

func TestRunStatusMessage(t *testing.T) {
	assert.Equal(t, "Good", RunStatusMessage(WorkGood))
	assert.Equal(t, "prepare for ready", RunStatusMessage(GetReady))
	assert.Equal(t, "can't fetch oplog from source MongoDB", RunStatusMessage(FetchBad))
	assert.Equal(t, "collector send oplog to tunnel failed", RunStatusMessage(TunnelSendBad))
	assert.Equal(t, "receiver fetch from tunnel failed", RunStatusMessage(TunnelSyncBad))
	assert.Equal(t, "receiver replica executed failed", RunStatusMessage(ReplicaExecBad))
	assert.Equal(t, "unknown", RunStatusMessage(9999))
}
