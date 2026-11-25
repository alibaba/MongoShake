package oplog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestOplogGetKeyAndIndex(t *testing.T) {
	doc := bson.D{
		primitive.E{Key: "_id", Value: 3},
		primitive.E{Key: "ns", Value: "db.c"},
	}
	assert.Equal(t, 3, GetKey(doc, "_id"))
	assert.Equal(t, "db.c", GetKey(doc, "ns"))
	assert.Nil(t, GetKey(doc, "absent"))

	v, idx := GetKeyWithIndex(doc, "ns")
	assert.Equal(t, "db.c", v)
	assert.Equal(t, 1, idx)
}

func TestOplogSetFiled(t *testing.T) {
	doc := bson.D{primitive.E{Key: "x", Value: 1}}
	SetFiled(doc, "x", 5)
	assert.Equal(t, 5, doc[0].Value)
}
