package oplog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestNormalizeApplyOps(t *testing.T) {
	// test NormalizeApplyOps

	var nr int
	newApplyOpsLog := func(value any) bson.D {
		return bson.D{
			{
				Key:   "applyOps",
				Value: value,
			},
		}
	}

	expected := []bson.D{
		{
			bson.E{Key: "op", Value: "i"},
			bson.E{Key: "ns", Value: "db.col"},
			bson.E{Key: "o", Value: bson.D{
				bson.E{Key: "_id", Value: "doc-1"},
			}},
		},
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog(expected))
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, expected, ops, "should be equal")
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog([]bson.M{
			{
				"op": "i",
				"ns": "db.col",
				"o":  bson.M{"_id": "doc-1"},
			},
		}))
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, "i", GetKey(ops[0], "op"), "should be equal")
		assert.Equal(t, "db.col", GetKey(ops[0], "ns"), "should be equal")
		inner, ok := GetKey(ops[0], "o").(bson.M)
		assert.True(t, ok, "should be equal")
		assert.Equal(t, "doc-1", inner["_id"], "should be equal")
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog([]any{expected[0]}))
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, expected, ops, "should be equal")
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog(primitive.A{expected[0]}))
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, expected, ops, "should be equal")
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog(1))
		assert.Nil(t, ops, "should be equal")
		assert.EqualError(t, err, "applyOps field has unsupported type int")
	}

	{
		fmt.Printf("TestNormalizeApplyOps case %d.\n", nr)
		nr++

		ops, err := NormalizeApplyOps(newApplyOpsLog([]any{"illegal"}))
		assert.Nil(t, ops, "should be equal")
		assert.EqualError(t, err, "applyOps element has unsupported type string")
	}
}
