package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

// parseShardKey is the pure-logic core of GetColShardType. These cases pin
// the per-column behaviour so a future "simplification" that collapses
// shard type into a single collection-wide value (the pre-fix bug) breaks
// here rather than silently mis-classifying docs in OrphanFilter.Filter.
func TestParseShardKey(t *testing.T) {
	cases := []struct {
		name       string
		keyDoc     bson.D
		wantKeys   []string
		wantTypes  []string
		wantErrSub string // non-empty -> expect error containing this substring
	}{
		{
			name:      "single ranged",
			keyDoc:    bson.D{{"a", int32(1)}},
			wantKeys:  []string{"a"},
			wantTypes: []string{RangedShard},
		},
		{
			name:      "single hashed",
			keyDoc:    bson.D{{"a", "hashed"}},
			wantKeys:  []string{"a"},
			wantTypes: []string{HashedShard},
		},
		{
			name:      "compound ranged",
			keyDoc:    bson.D{{"a", int32(1)}, {"b", int32(1)}},
			wantKeys:  []string{"a", "b"},
			wantTypes: []string{RangedShard, RangedShard},
		},
		{
			// MongoDB 4.4+ compound hashed: ranged-first, hashed-second.
			// Pre-fix GetColShardType would overwrite shardType every
			// iteration so the whole collection looked HashedShard.
			name:      "compound hashed, ranged first",
			keyDoc:    bson.D{{"a", int32(1)}, {"b", "hashed"}},
			wantKeys:  []string{"a", "b"},
			wantTypes: []string{RangedShard, HashedShard},
		},
		{
			// Inverse direction. Pre-fix this would resolve to RangedShard
			// (last field wins) so Filter would skip hashing entirely.
			name:      "compound hashed, hashed first",
			keyDoc:    bson.D{{"a", "hashed"}, {"b", int32(1)}},
			wantKeys:  []string{"a", "b"},
			wantTypes: []string{HashedShard, RangedShard},
		},
		{
			// Numeric direction may come back as int / int32 / int64 /
			// float64 depending on the BSON decoder; all should be ranged.
			name:      "mixed numeric encodings are all ranged",
			keyDoc:    bson.D{{"a", int(1)}, {"b", int64(-1)}, {"c", float64(1)}},
			wantKeys:  []string{"a", "b", "c"},
			wantTypes: []string{RangedShard, RangedShard, RangedShard},
		},
		{
			name:       "unsupported value type errors out",
			keyDoc:     bson.D{{"a", true}}, // bool is not a legal shard-key direction
			wantErrSub: "field[a]",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keys, types, err := parseShardKey(c.keyDoc)
			if c.wantErrSub != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), c.wantErrSub)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.wantKeys, keys)
			assert.Equal(t, c.wantTypes, types)
			assert.Equal(t, len(keys), len(types), "per-column slices must be 1:1")
		})
	}
}

// Round-trip through bson.Marshal/Unmarshal — verifies parseShardKey works
// on the exact value shapes the driver hands us when decoding a real
// config.collections document, not just hand-built bson.D literals.
func TestParseShardKey_BsonRoundTrip(t *testing.T) {
	original := bson.D{{"a", int32(1)}, {"b", "hashed"}}
	raw, err := bson.Marshal(bson.D{{"key", original}})
	assert.NoError(t, err)

	var decoded bson.D
	assert.NoError(t, bson.Unmarshal(raw, &decoded))

	keyDoc, ok := decoded[0].Value.(bson.D)
	assert.True(t, ok, "decoded key field must be bson.D")

	keys, types, err := parseShardKey(keyDoc)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, keys)
	assert.Equal(t, []string{RangedShard, HashedShard}, types)
}
