package oplog

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// NormalizeApplyOps extracts and normalizes applyOps into []bson.D so callers
// can share one BSON-container compatibility path while keeping their own
// applyOps-specific behavior.
func NormalizeApplyOps(logObject bson.D) ([]bson.D, error) {
	raw := GetKey(logObject, "applyOps")
	if raw == nil {
		return nil, fmt.Errorf("applyOps field is missing")
	}

	switch v := raw.(type) {
	case []bson.D:
		return v, nil
	case []bson.M:
		ops := make([]bson.D, 0, len(v))
		for _, ele := range v {
			ops = append(ops, ConvertBsonM2D(ele))
		}
		return ops, nil
	case []any:
		ops := make([]bson.D, 0, len(v))
		for _, ele := range v {
			doc, ok := ele.(bson.D)
			if !ok {
				return nil, fmt.Errorf("applyOps element has unsupported type %T", ele)
			}
			ops = append(ops, doc)
		}
		return ops, nil
	case primitive.A:
		ops := make([]bson.D, 0, len(v))
		for _, ele := range v {
			doc, ok := ele.(bson.D)
			if !ok {
				return nil, fmt.Errorf("applyOps element has unsupported type %T", ele)
			}
			ops = append(ops, doc)
		}
		return ops, nil
	default:
		return nil, fmt.Errorf("applyOps field has unsupported type %T", raw)
	}
}
