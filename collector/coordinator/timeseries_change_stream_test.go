package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

// TestTimeSeriesUnsupportedByChangeStream pins the gate behind the startup warning: a
// change-stream run whose scope contains a time-series collection has to say so, because
// the run otherwise looks healthy - the checkpoint advances on the resume token while
// write_success stays at 0 - and nothing in the log explains why.
func TestTimeSeriesUnsupportedByChangeStream(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	ctx := context.Background()

	const db = "ts_cs_warn_test"
	_ = conn.Client.Database(db).Drop(ctx)
	// Drop on the way out too: a time-series database left half-built holds a dangling
	// view definition, which makes the server reject listCollections for that whole
	// database and so breaks unrelated tests that run afterwards.
	defer func() { _ = conn.Client.Database(db).Drop(ctx) }()

	err = conn.Client.Database(db).RunCommand(ctx, bson.D{
		{Key: "create", Value: "gps"},
		{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "ts"}}},
	}).Err()
	if err != nil {
		t.Skipf("server cannot create time-series collections (needs 5.0+): %v", err)
	}
	conn.Close()

	savedMethod := conf.Options.IncrSyncMongoFetchMethod
	savedWhite := conf.Options.FilterNamespaceWhite
	savedBlack := conf.Options.FilterNamespaceBlack
	defer func() {
		conf.Options.IncrSyncMongoFetchMethod = savedMethod
		conf.Options.FilterNamespaceWhite = savedWhite
		conf.Options.FilterNamespaceBlack = savedBlack
	}()
	conf.Options.FilterNamespaceWhite = []string{db + ".gps"}
	conf.Options.FilterNamespaceBlack = []string{}

	sources := []*utils.MongoSource{{URL: testMongoAddress}}

	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
	assert.Contains(t, timeSeriesUnsupportedByChangeStream(sources), db+".gps",
		"change_stream cannot sync a time-series collection, so the run must be told")

	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	assert.Equal(t, 0, len(timeSeriesUnsupportedByChangeStream(sources)),
		"oplog mode replays bucket writes normally, so there is nothing to warn about")

	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
	conf.Options.FilterNamespaceWhite = []string{db + ".somewhere-else"}
	assert.Equal(t, 0, len(timeSeriesUnsupportedByChangeStream(sources)),
		"a time-series collection outside the sync scope is not this run's problem")
}

// TestWarnChangeStreamCannotSyncTimeSeriesIsLogged checks the warning actually reaches the
// log, naming the collections and stating the remedy. That is the whole deliverable: the
// gate returning a list is worth nothing if nothing tells the operator, and the omission
// is otherwise invisible because the run keeps advancing its checkpoint.
func TestWarnChangeStreamCannotSyncTimeSeriesIsLogged(t *testing.T) {
	conn, err := utils.NewMongoCommunityConn(testMongoAddress, "primary", true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	if err != nil {
		t.Skipf("no mongodb at %s: %v", testMongoAddress, err)
	}
	ctx := context.Background()

	const db = "ts_cs_warn_log_test"
	_ = conn.Client.Database(db).Drop(ctx)
	defer func() { _ = conn.Client.Database(db).Drop(ctx) }()

	err = conn.Client.Database(db).RunCommand(ctx, bson.D{
		{Key: "create", Value: "gps"},
		{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "ts"}}},
	}).Err()
	if err != nil {
		t.Skipf("server cannot create time-series collections (needs 5.0+): %v", err)
	}
	conn.Close()

	savedMethod := conf.Options.IncrSyncMongoFetchMethod
	savedWhite := conf.Options.FilterNamespaceWhite
	savedBlack := conf.Options.FilterNamespaceBlack
	defer func() {
		conf.Options.IncrSyncMongoFetchMethod = savedMethod
		conf.Options.FilterNamespaceWhite = savedWhite
		conf.Options.FilterNamespaceBlack = savedBlack
		// verbose 2 is console only, so putting the logger back leaves no file behind
		assert.Equal(t, nil, utils.InitialLogger("", "", "info", true, 2))
	}()
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
	conf.Options.FilterNamespaceWhite = []string{db + ".gps"}
	conf.Options.FilterNamespaceBlack = []string{}

	logDir := t.TempDir()
	// logFlush true means the file sink is unbuffered, so the log is complete by the time
	// it is read back
	assert.Equal(t, nil, utils.InitialLogger(logDir, "warn.log", "info", true, 1))

	warnChangeStreamCannotSyncTimeSeries([]*utils.MongoSource{{URL: testMongoAddress}})

	body, err := os.ReadFile(filepath.Join(logDir, "warn.log"))
	assert.Equal(t, nil, err, "read back the log the warning was written to")
	logged := string(body)
	assert.Contains(t, logged, "cannot sync time-series collections")
	assert.Contains(t, logged, db+".gps", "the affected collection must be named")
	assert.Contains(t, logged, "incr_sync.mongo_fetch_method = oplog", "the remedy must be stated")
	assert.Contains(t, logged,
		"https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/",
		"the upstream limitation must be cited")
}
