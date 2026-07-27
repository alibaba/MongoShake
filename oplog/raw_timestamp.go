package oplog

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ExtractRawTimestamp reads a BSON timestamp field directly from raw BSON.
func ExtractRawTimestamp(data []byte, field string) (primitive.Timestamp, error) {
	t, i, ok := bson.Raw(data).Lookup(field).TimestampOK()
	if !ok {
		return primitive.Timestamp{}, fmt.Errorf("timestamp field %q not found", field)
	}
	if t == 0 && i == 0 {
		return primitive.Timestamp{}, fmt.Errorf("timestamp field %q is zero", field)
	}
	return primitive.Timestamp{T: t, I: i}, nil
}
