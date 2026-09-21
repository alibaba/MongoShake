package oplog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func lazyFixture(t testing.TB, op string, object, query interface{}) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{"ts", primitive.Timestamp{T: 123, I: 4}}, {"t", int64(7)},
		{"h", int64(8)}, {"v", int32(2)}, {"op", op}, {"ns", "source.coll"},
		{"g", "group"}, {"o", object}, {"o2", query},
		{"lsid", bson.D{{"id", primitive.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
		{"txnNumber", int64(12)}, {"prevOpTime", bson.D{{"ts", primitive.Timestamp{}}}},
		{"ui", primitive.Binary{Subtype: 4, Data: make([]byte, 16)}},
		{"b", true}, {"fromMigrate", true}, {"multiOpType", int32(1)},
		{"documentKey", bson.D{{"shard", int32(9)}}},
	})
	require.NoError(t, err)
	return raw
}

func TestParseRawRoundTrip(t *testing.T) {
	object := bson.D{{"_id", primitive.NewObjectID()}, {"nested", bson.D{
		{"binary", primitive.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}},
		{"array", bson.A{int32(1), int64(2), nil, bson.D{{"x", "value"}}}},
		{"date", primitive.DateTime(123456)}, {"regex", primitive.Regex{Pattern: "a", Options: "i"}},
		{"decimal", primitive.NewDecimal128(1, 2)}, {"null", nil},
		{"code", primitive.CodeWithScope{Code: "return x", Scope: bson.D{{"x", int32(1)}}}},
		{"min", primitive.MinKey{}}, {"max", primitive.MaxKey{}},
	}}}
	for _, op := range []string{"i", "u", "d"} {
		t.Run(op, func(t *testing.T) {
			raw := lazyFixture(t, op, object, bson.D{{"_id", int64(23)}})
			log, err := ParseRaw(raw)
			require.NoError(t, err)
			require.Nil(t, log.Object)
			require.Nil(t, log.Query)
			require.Same(t, &bson.Raw(raw).Lookup("o").Document()[0], &log.objectRaw[0])
			require.Same(t, &bson.Raw(raw).Lookup("o2").Document()[0], &log.queryRaw[0])
			var eager ParsedLog
			require.NoError(t, bson.Unmarshal(raw, &eager))
			encoded, err := bson.Marshal(log)
			require.NoError(t, err)
			var decoded ParsedLog
			require.NoError(t, bson.Unmarshal(encoded, &decoded))
			require.Equal(t, eager, decoded)
			expectedJSON, err := json.Marshal(encodedLog(eager))
			require.NoError(t, err)
			actualJSON, err := json.Marshal(log.ParsedLog)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedJSON), string(actualJSON))
			expectedExt, err := bson.MarshalExtJSON(encodedLog(eager), true, true)
			require.NoError(t, err)
			actualExt, err := bson.MarshalExtJSON(log.ParsedLog, true, true)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedExt), string(actualExt))
			require.Nil(t, log.Object, "serialization must not retain decoded payloads")
			require.Nil(t, log.Query)
			for _, key := range []string{"o", "o2"} {
				require.Equal(t, bson.Raw(raw).Lookup(key).Value, bson.Raw(encoded).Lookup(key).Value)
			}
			log.RawSize, log.SourceId = 123, 42
			encoded, err = bson.Marshal(log)
			require.NoError(t, err)
			require.Zero(t, bson.Raw(encoded).Lookup("rawsize").Type)
			require.Zero(t, bson.Raw(encoded).Lookup("sourceid").Type)
		})
	}
}

func TestParseRawHashParity(t *testing.T) {
	ids := []interface{}{primitive.NewObjectID(), "string", int32(4), int64(5), nil,
		primitive.Binary{Subtype: 4, Data: []byte{1, 2}}, bson.D{{"nested", int32(3)}}}
	for _, op := range []string{"i", "u", "d"} {
		for _, id := range ids {
			raw := lazyFixture(t, op, bson.D{{"_id", id}}, bson.D{{"_id", id}})
			log, err := ParseRaw(raw)
			require.NoError(t, err)
			var eager PartialLog
			require.NoError(t, bson.Unmarshal(raw, &eager.ParsedLog))
			require.Equal(t, GetIdOrNSFromOplog(&eager), GetIdOrNSFromOplog(log))
			hasher := &PrimaryKeyHasher{}
			require.Equal(t, hasher.DistributeOplogByMod(&eager, 17), hasher.DistributeOplogByMod(log, 17))
			require.Nil(t, log.Object)
		}
	}
	for _, query := range []bson.D{nil, {{"_id", nil}}, {{"other", int32(1)}}} {
		raw := lazyFixture(t, "u", bson.D{{"_id", "fallback"}}, query)
		log, err := ParseRaw(raw)
		require.NoError(t, err)
		var eager PartialLog
		require.NoError(t, bson.Unmarshal(raw, &eager.ParsedLog))
		require.Equal(t, GetIdOrNSFromOplog(&eager), GetIdOrNSFromOplog(log))
	}
}

func TestParseRawFallbackAndErrors(t *testing.T) {
	for _, op := range []string{"c", "n"} {
		raw := lazyFixture(t, op, bson.D{{"applyOps", bson.A{bson.D{{"op", "i"}, {"o", bson.D{{"_id", 1}}}}}}}, nil)
		log, err := ParseRaw(raw)
		require.NoError(t, err)
		var eager ParsedLog
		require.NoError(t, bson.Unmarshal(raw, &eager))
		require.Equal(t, eager, log.ParsedLog)
	}
	raw, err := bson.Marshal(bson.D{{"op", "i"}, {"ns", "db.system.indexes"}, {"o", bson.D{{"ns", "db.coll"}}}})
	require.NoError(t, err)
	log, err := ParseRaw(raw)
	require.NoError(t, err)
	require.NotNil(t, log.Object)
	for _, invalid := range [][]byte{nil, {5, 0, 0, 0, 1}, raw[:len(raw)-1]} {
		_, err := ParseRaw(invalid)
		require.Error(t, err)
	}
	corrupt := lazyFixture(t, "i", bson.D{{"x", "value"}}, nil)
	nested := bson.Raw(corrupt).Lookup("o").Document()
	nested[len(nested)-1] = 1
	_, err = ParseRaw(corrupt)
	require.Error(t, err)
	for _, field := range []string{"o", "o2", "ts", "txnNumber"} {
		raw, err := bson.Marshal(bson.D{{"op", "i"}, {field, "invalid"}})
		require.NoError(t, err)
		_, err = ParseRaw(raw)
		require.Error(t, err, field)
	}
	for _, field := range []string{"o", "o2"} {
		raw, err := bson.Marshal(bson.D{{"op", "i"}, {field, primitive.Undefined{}}})
		require.NoError(t, err)
		_, err = ParseRaw(raw)
		require.Error(t, err)
		var eager ParsedLog
		require.Error(t, bson.Unmarshal(raw, &eager))
	}
	for _, object := range []interface{}{nil, bson.D{}} {
		raw := lazyFixture(t, "i", object, nil)
		log, err := ParseRaw(raw)
		require.NoError(t, err)
		var eager ParsedLog
		require.NoError(t, bson.Unmarshal(raw, &eager))
		encoded, err := bson.Marshal(log)
		require.NoError(t, err)
		var decoded ParsedLog
		require.NoError(t, bson.Unmarshal(encoded, &decoded))
		require.Equal(t, eager, decoded)
	}
}

func TestParseRawUpdateAndMutation(t *testing.T) {
	cases := []struct {
		name     string
		object   bson.D
		expected bson.D
		modifier bool
	}{
		{"replacement", bson.D{{"_id", int32(1)}, {"x", "value"}}, bson.D{{"_id", int32(1)}, {"x", "value"}}, false},
		{"v1", bson.D{{"$v", int32(1)}, {"$set", bson.D{{"x", "value"}}}}, bson.D{{"$set", bson.D{{"x", "value"}}}}, true},
		{"v2", bson.D{{"$v", int32(2)}, {"diff", bson.D{{"u", bson.D{{"x", "value"}}}}}}, bson.D{{"$set", bson.D{{"x", "value"}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := lazyFixture(t, "u", tc.object, bson.D{{"_id", int32(1)}})
			before := bytes.Clone(raw)
			log, err := ParseRaw(raw)
			require.NoError(t, err)
			require.Equal(t, tc.modifier, log.ObjectHasPrefix("$"))
			for i := 0; i < 2; i++ { // retries must observe the same original oplog
				update, err := log.UpdateValue()
				require.NoError(t, err)
				encoded, err := bson.Marshal(update)
				require.NoError(t, err)
				var decoded bson.D
				require.NoError(t, bson.Unmarshal(encoded, &decoded))
				require.Equal(t, tc.expected, decoded)
			}
			require.Equal(t, before, raw)
			require.Nil(t, log.Object)
		})
	}
	log, err := ParseRaw(lazyFixture(t, "i", bson.D{{"_id", int32(1)}}, nil))
	require.NoError(t, err)
	require.NoError(t, log.MaterializeObject())
	log.Object = append(log.Object, bson.E{Key: "added", Value: "new"})
	encoded, err := bson.Marshal(log)
	require.NoError(t, err)
	require.Equal(t, "new", bson.Raw(encoded).Lookup("o", "added").StringValue())
	log.Object = nil
	encoded, err = bson.Marshal(log)
	require.NoError(t, err)
	require.Equal(t, bsontype.Null, bson.Raw(encoded).Lookup("o").Type)
}

func TestParseRawLookupDoesNotDescendIntoArrays(t *testing.T) {
	document := bson.D{{"array", bson.A{bson.D{{"key", "value"}}}}}
	log, err := ParseRaw(lazyFixture(t, "i", document, nil))
	require.NoError(t, err)
	for _, doc := range []interface{}{document, log.ObjectValue()} {
		_, found := LookupDocument(doc, "array", "0", "key")
		require.False(t, found)
	}
}

func TestParseRawGatherAndIndexValues(t *testing.T) {
	object := bson.D{{"_id", int32(7)}, {"nested", bson.D{{"key", "value"}}}, {"nested.key", "literal"}}
	for _, document := range []bson.D{object, {{"$set", object}}} {
		log, err := ParseRaw(lazyFixture(t, "i", document, nil))
		require.NoError(t, err)
		require.Equal(t, "literal", log.IndexValue("nested.key"))
		require.Nil(t, log.IndexValue("missing.key"))
		require.Equal(t, bson.M{"key": "value"}, log.IndexValue("nested"))
		require.Nil(t, log.Object)
		gathered, err := GatherApplyOps([]*PartialLog{log})
		require.NoError(t, err)
		require.Equal(t, log.objectRaw, bson.Raw(gathered.Raw).Lookup("o", "applyOps").Array().Index(0).Value().Document().Lookup("o").Document())
	}
}

func BenchmarkParseRaw(b *testing.B) {
	// Many nested fields model the allocation-heavy payload, not just one large string.
	object := bson.D{{"_id", int64(42)}}
	for i := 0; i < 4096; i++ {
		object = append(object, bson.E{Key: fmt.Sprintf("field_%d", i), Value: bson.D{
			{"text", strings.Repeat("x", 128)}, {"values", bson.A{int32(i), int64(i), "value"}},
		}})
	}
	raw := lazyFixture(b, "i", object, nil)
	for _, lazy := range []bool{false, true} {
		name := "eager"
		if lazy {
			name = "raw"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if lazy {
					log, err := ParseRaw(raw)
					if err != nil {
						b.Fatal(err)
					}
					_ = GetIdOrNSFromOplog(log)
				} else {
					log := &PartialLog{}
					if err := bson.Unmarshal(raw, &log.ParsedLog); err != nil {
						b.Fatal(err)
					}
					_ = GetIdOrNSFromOplog(log)
				}
			}
		})
	}
}

func TestParseRawDepthLimit(t *testing.T) {
	for _, kind := range []string{"document", "array", "scope"} {
		t.Run(kind, func(t *testing.T) {
			doc := bson.Raw{5, 0, 0, 0, 0}
			for depth := 0; depth <= maxRawDocumentDepth+1; depth++ {
				err := validateRawDocuments(doc, 0)
				if depth <= maxRawDocumentDepth {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "maximum depth")
					_, err = ParseRaw(doc)
					require.ErrorContains(t, err, "maximum depth")
				}
				var value interface{} = doc
				if kind == "array" {
					value = bson.RawValue{Type: bsontype.Array, Value: doc}
				} else if kind == "scope" {
					value = primitive.CodeWithScope{Code: "return x", Scope: doc}
				}
				raw, err := bson.Marshal(bson.D{{"0", value}})
				require.NoError(t, err)
				doc = bson.Raw(raw)
			}
		})
	}
}

func TestLazyLegacyIndexNamespace(t *testing.T) {
	for _, ns := range []string{"db.system.indexes", "db.mysystem.indexes", "db.foo.system.indexes"} {
		raw, err := bson.Marshal(bson.D{{"op", "i"}, {"ns", ns}, {"o", bson.D{{"ns", "db.coll"}}}})
		require.NoError(t, err)
		log, err := ParseRaw(raw)
		require.NoError(t, err)
		require.Equal(t, ns == "db.system.indexes", log.Object != nil)
	}
}

func TestLazyCommandName(t *testing.T) {
	for _, object := range []bson.D{{}, {{"create", "coll"}}, {{"unknown", 1}, {"create", "coll"}}} {
		raw, err := bson.Marshal(object)
		require.NoError(t, err)
		expected, found := ExtraCommandName(object)
		actual, rawFound := ExtraCommandName(bson.Raw(raw))
		require.Equal(t, expected, actual)
		require.Equal(t, found, rawFound)
	}
}
