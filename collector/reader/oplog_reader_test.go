package sourceReader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestCloneOplogRawDetachesCursorBuffer(t *testing.T) {
	original := bson.Raw{1, 2, 3, 4}
	cloned := cloneOplogRaw(original)

	original[0] = 9

	assert.Equal(t, bson.Raw{1, 2, 3, 4}, cloned, "should keep the original oplog bytes")
}

func TestOplogReaderPreservesSourceFiltersWhenTimestampChanges(t *testing.T) {
	require.NoError(t, utils.InitialLogger("", "", "error", true, 2))
	reader := NewOplogReader("", "test")
	expected := bson.M{"fromMigrate": bson.M{"$ne": true},
		"ns": bson.M{"$not": primitive.Regex{Pattern: "^config\\.", Options: "i"}}}
	assert.Equal(t, expected, reader.query)
	for _, ts := range []primitive.Timestamp{{T: 100, I: 1}, {T: 200, I: 2}} {
		reader.UpdateQueryTimestamp(utils.TimeStampToInt64(ts))
		expected[QueryTs] = bson.M{QueryOpGT: ts}
		assert.Equal(t, expected, reader.query, "advancing the timestamp must retain source filters")
		reader.SetQueryTimestampOnEmpty(utils.TimeStampToInt64(primitive.Timestamp{T: 1}))
		assert.Equal(t, utils.TimeStampToInt64(ts), reader.getQueryTimestamp(),
			"initialization must not rewind an existing cursor boundary")
		_, err := bson.Marshal(reader.query)
		require.NoError(t, err)
	}
}

func TestOplogReaderRespectsConfigDatabasePassThrough(t *testing.T) {
	for _, test := range []struct {
		name       string
		specialDBs []string
		passConfig bool
	}{
		{"default", nil, false},
		{"other special database", []string{"admin"}, false},
		{"config explicitly enabled", []string{"config"}, true},
		{"config among multiple databases", []string{"admin", "config"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldSpecialDBs := conf.Options.FilterPassSpecialDb
			oldRules := filter.NsShouldBeIgnore
			filter.NsShouldBeIgnore = make(map[string]bool, len(oldRules))
			for namespace, prefix := range oldRules {
				filter.NsShouldBeIgnore[namespace] = prefix
			}
			t.Cleanup(func() {
				conf.Options.FilterPassSpecialDb = oldSpecialDBs
				filter.NsShouldBeIgnore = oldRules
			})
			conf.Options.FilterPassSpecialDb = test.specialDBs
			filter.InitNs(test.specialDBs) // Match startup's filter.pass.special.db handling.
			reader := NewOplogReader("", "test")
			assert.Equal(t, bson.M{"$ne": true}, reader.query["fromMigrate"],
				"enabling a system database must not enable source migration events")
			if test.passConfig {
				assert.NotContains(t, reader.query, "ns",
					"reader must not exclude config oplogs explicitly enabled by filter.pass.special.db")
			} else {
				assert.Equal(t, bson.M{"$not": primitive.Regex{Pattern: "^config\\.", Options: "i"}},
					reader.query["ns"], "config should still be excluded by default")
			}
		})
	}
}
