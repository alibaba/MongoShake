package oplog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestExtractInnerOpsPreservesFromMigrate(t *testing.T) {
	for _, test := range []struct {
		name  string
		field bson.D
		want  bool
	}{
		{"absent", nil, false},
		{"false", bson.D{{Key: "fromMigrate", Value: false}}, false},
		{"true", bson.D{{Key: "fromMigrate", Value: true}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "migration.documents"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}}
			inner = append(inner, test.field...)
			parent := ParsedLog{
				Timestamp: primitive.Timestamp{T: 100, I: 1},
				Object:    bson.D{{Key: "applyOps", Value: bson.A{inner}}},
			}
			ops, err := ExtractInnerOps(&parent)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			require.Equal(t, test.want, ops[0].FromMigrate)
			require.Equal(t, parent.Timestamp, ops[0].Timestamp)
		})
	}
}

func TestExtractInnerOpsRejectsMalformedFromMigrate(t *testing.T) {
	parent := ParsedLog{
		Timestamp: primitive.Timestamp{T: 100, I: 1},
		Object: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "d"}, {Key: "ns", Value: "migration.documents"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "fromMigrate", Value: "true"}},
		}}},
	}
	_, err := ExtractInnerOps(&parent)
	require.ErrorContains(t, err, "fromMigrate field: not a bool")
}
