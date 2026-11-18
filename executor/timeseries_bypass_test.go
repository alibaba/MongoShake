package executor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

// TestTimeSeriesBucketInsertIgnoresBypassDocumentValidation covers
// incr_sync.executor.bypass_document_validation combined with a time-series target.
//
// Whether the server tolerates the option on a bucket namespace depends on the patch
// release. Measured by inserting one real, unmodified bucket into
// system.buckets.<ts collection>:
//
//	7.0.37   option omitted / false / true   all accepted
//	7.0.43   option omitted / false accepted, true rejected with
//	         code 2 "bypassDocumentValidation is not permitted on timeseries collections"
//	8.0.4    option omitted / false / true   all accepted
//	8.2.12   option omitted / false / true   all accepted
//
// A replay must not depend on which patch release the target happens to be, so the
// writers omit the option for system.buckets.* whatever the configuration says. The
// assertion is therefore "the real bucket lands and its measurements are readable",
// which holds on every version once the option is omitted, rather than "the server
// rejects it", which holds only on some.
//
// The bucket is captured from a real time-series collection and keeps its original _id:
// a bucket _id embeds the opening timestamp, and the server rejects a substituted one
// with code 121 for disagreeing with control.min.
func TestTimeSeriesBucketInsertIgnoresBypassDocumentValidation(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	defer conn.Close()
	ctx := context.Background()

	const (
		srcDb        = "ts_bypass_src"
		coll         = "gps"
		bucketColl   = "system.buckets." + coll
		measurements = 5
	)

	// Every database this test creates is dropped on the way out, including on a failed
	// assertion: a time-series database left half-built holds a dangling view definition,
	// which makes the server reject listCollections for that whole database and so breaks
	// unrelated tests that run afterwards.
	created := []string{srcDb}
	defer func() {
		for _, db := range created {
			_ = conn.Client.Database(db).Drop(ctx)
		}
	}()

	createTsCollection := func(db string) {
		_ = conn.Client.Database(db).Drop(ctx)
		if err := conn.Client.Database(db).RunCommand(ctx, bson.D{
			{Key: "create", Value: coll},
			{Key: "timeseries", Value: bson.D{
				{Key: "timeField", Value: "ts"},
				{Key: "metaField", Value: "device"},
			}},
		}).Err(); err != nil {
			t.Skipf("server cannot create time-series collections (needs 5.0+): %v", err)
		}
		created = append(created, db)
	}

	createTsCollection(srcDb)

	base := time.Now().Truncate(time.Second)
	for i := 0; i < measurements; i++ {
		_, err = conn.Client.Database(srcDb).Collection(coll).InsertOne(ctx, bson.D{
			{Key: "ts", Value: base.Add(time.Duration(i) * time.Second)},
			{Key: "device", Value: "d1"},
			{Key: "v", Value: i},
		})
		assert.Equal(t, nil, err, "insert a measurement into the source time-series collection")
	}

	var bucket bson.D
	err = conn.Client.Database(srcDb).Collection(bucketColl).FindOne(ctx, bson.D{}).Decode(&bucket)
	assert.Equal(t, nil, err, "capture a real bucket from the source")
	assert.True(t, len(bucket) > 0, "the captured bucket must not be empty")

	saved := conf.Options.IncrSyncBypassDocumentValidation
	conf.Options.IncrSyncBypassDocumentValidation = true
	defer func() { conf.Options.IncrSyncBypassDocumentValidation = saved }()

	// CommandWriter is deliberately absent. Selecting it requires metadata {g: <gid>},
	// which only buildMetadata produces when filter.oplog.gids is configured, and all six
	// of its command paths append that field verbatim to the server command - modern
	// MongoDB rejects it with 40415 "BSON field 'insert.g' is an unknown field" before
	// anything about bypassDocumentValidation is reached. That is a separate pre-existing
	// defect. Its five bypass sites call the same predicate the unit test below covers.
	writers := []struct {
		name     string
		metadata bson.E
		bulk     bool
	}{
		{"SingleWriter", bson.E{}, false},
		{"BulkWriter", bson.E{}, true},
	}

	for i, w := range writers {
		tgtDb := fmt.Sprintf("ts_bypass_tgt_%d", i)
		// A freshly created target collection has an empty system.buckets.gps, so the
		// captured bucket's _id cannot collide and the insert stays a plain insert.
		createTsCollection(tgtDb)

		writer := NewDbWriter(conn, w.metadata, w.bulk, 0)
		oplogs := []*OplogRecord{{
			original: &PartialLogWithCallback{
				partialLog: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Operation: "i",
						Namespace: tgtDb + "." + bucketColl,
						Object:    bucket,
					},
				},
			},
		}}

		err = writer.doInsert(tgtDb, bucketColl, w.metadata, oplogs, false)
		assert.Equal(t, nil, err,
			"%s must replay a real time-series bucket with bypass_document_validation=true", w.name)

		got, countErr := conn.Client.Database(tgtDb).Collection(coll).CountDocuments(ctx, bson.D{})
		assert.Equal(t, nil, countErr, "%s: count measurements through the logical view", w.name)
		assert.Equal(t, int64(measurements), got,
			"%s: the replayed bucket must be readable through the logical view", w.name)
	}
}

// TestBypassDocumentValidationFor is the version-independent half of the coverage above.
// The replay test can only go red on a patch release that rejects the option, so this
// pins the invariant directly: all thirteen bypass sites in the three writers funnel
// through this predicate, including the five in the command writer that the replay test
// cannot reach.
func TestBypassDocumentValidationFor(t *testing.T) {
	saved := conf.Options.IncrSyncBypassDocumentValidation
	defer func() { conf.Options.IncrSyncBypassDocumentValidation = saved }()

	conf.Options.IncrSyncBypassDocumentValidation = false
	assert.False(t, bypassDocumentValidationFor("coll"),
		"the option must stay off when it is not configured")
	assert.False(t, bypassDocumentValidationFor("system.buckets.coll"))

	conf.Options.IncrSyncBypassDocumentValidation = true
	assert.True(t, bypassDocumentValidationFor("coll"),
		"an ordinary collection must still honour the configuration")
	assert.True(t, bypassDocumentValidationFor("system.views"),
		"only bucket namespaces are affected")
	assert.False(t, bypassDocumentValidationFor("system.buckets.coll"),
		"the option must be omitted for time-series buckets: 7.0.43 rejects it with code 2 "+
			"\"bypassDocumentValidation is not permitted on timeseries collections\"")
}
