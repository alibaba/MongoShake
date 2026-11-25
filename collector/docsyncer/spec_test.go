package docsyncer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestMatchesTarget(t *testing.T) {
	ts := destCollectionSpec{name: "c", kind: kindTimeSeries}
	assert.True(t, ts.matchesTarget("timeseries", nil))
	assert.False(t, ts.matchesTarget("collection", nil))

	capped := destCollectionSpec{name: "c", kind: kindCapped}
	assert.True(t, capped.matchesTarget("collection", bson.M{"capped": true}))
	assert.False(t, capped.matchesTarget("collection", bson.M{"capped": false}))
	assert.False(t, capped.matchesTarget("collection", bson.M{}))

	ordinary := destCollectionSpec{name: "c"}
	assert.False(t, ordinary.matchesTarget("collection", bson.M{}))
}

func TestMismatchMessage(t *testing.T) {
	ns := utils.NS{Database: "db", Collection: "c"}
	ts := destCollectionSpec{name: "c", kind: kindTimeSeries}
	msg := ts.mismatchMessage(ns, "collection")
	assert.Contains(t, msg, "time-series")
	assert.Contains(t, msg, "full_sync.collection_exist_drop")

	capped := destCollectionSpec{name: "c", kind: kindCapped}
	assert.NotEmpty(t, capped.mismatchMessage(ns, "collection"))
}
