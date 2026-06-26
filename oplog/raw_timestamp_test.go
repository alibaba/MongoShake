package oplog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestExtractRawTimestamp(t *testing.T) {
	data, err := bson.Marshal(bson.D{
		{Key: "ts", Value: primitive.Timestamp{T: 1234, I: 56}},
		{Key: "op", Value: "n"},
	})
	require.NoError(t, err)

	ts, err := ExtractRawTimestamp(data, "ts")
	require.NoError(t, err)
	assert.Equal(t, primitive.Timestamp{T: 1234, I: 56}, ts, "should be equal")
}

func TestExtractRawTimestampRejectsMissingTimestamp(t *testing.T) {
	data, err := bson.Marshal(bson.D{{Key: "op", Value: "n"}})
	require.NoError(t, err)

	_, err = ExtractRawTimestamp(data, "ts")
	require.ErrorContains(t, err, "timestamp field \"ts\" not found")
}

func TestExtractRawTimestampRejectsEmptyTimestamp(t *testing.T) {
	data, err := bson.Marshal(bson.D{{Key: "ts", Value: primitive.Timestamp{}}})
	require.NoError(t, err)

	_, err = ExtractRawTimestamp(data, "ts")
	require.ErrorContains(t, err, "timestamp field \"ts\" is zero")
}
