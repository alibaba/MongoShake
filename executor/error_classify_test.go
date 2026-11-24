package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestIgnoreErrorBranches(t *testing.T) {
	assert.True(t, IgnoreError(nil, "i", false), "nil is always ignorable")
	assert.False(t, IgnoreError(errors.New("plain"), "d", false), "non-server error is not ignorable")

	// delete: NamespaceNotFound ignorable, others not
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 26}, "d", false))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 280}, "d", false))

	// update: PathNotViable/KeyNotFound only during full sync
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 28}, "u", true))
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 211}, "u", true))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 28}, "u", false))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 11000}, "u", true))

	// upsert-insert: DuplicateKey only during full sync
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 11000}, "ui", true))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 11000}, "ui", false))

	// command: NamespaceNotFound / NamespaceExists
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 26}, "c", false))
	assert.True(t, IgnoreError(&mongo.WriteError{Code: 48}, "c", false))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 11000}, "c", false))

	// insert has no ignore branch; unknown op falls to default
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 11000}, "i", true))
	assert.False(t, IgnoreError(&mongo.WriteError{Code: 26}, "x", false))
}

func dupWriteErr(index int, msg string) mongo.BulkWriteError {
	return mongo.BulkWriteError{WriteError: mongo.WriteError{Index: index, Code: 11000, Message: msg}}
}

func TestAllDupKeyErrorsOnIDIndex(t *testing.T) {
	idMsg := "E11000 duplicate key error collection: db.c index: _id_ dup key: { _id: 1 }"
	secMsg := "E11000 duplicate key error collection: db.c index: uniq_x dup key: { x: 1 }"

	assert.False(t, allDupKeyErrorsOnIDIndex(mongo.BulkWriteException{}), "no write errors")
	assert.True(t, allDupKeyErrorsOnIDIndex(mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(0, idMsg), dupWriteErr(1, idMsg)}}))
	assert.False(t, allDupKeyErrorsOnIDIndex(mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(0, idMsg), {WriteError: mongo.WriteError{Index: 1, Code: 26, Message: "ns"}}}}),
		"a non-dup error keeps the configured strategy")
	assert.False(t, allDupKeyErrorsOnIDIndex(mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(0, secMsg)}}),
		"secondary unique-index conflict is not the benign overlap dup")
	// DocumentDB phrasing must classify the same way
	assert.True(t, allDupKeyErrorsOnIDIndex(mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(0, "Duplicate key violation on the requested collection: Index '_id_'")}}))
}

func rec() *OplogRecord {
	return &OplogRecord{original: &PartialLogWithCallback{partialLog: &oplog.PartialLog{}}}
}

func TestFailedInsertSubset(t *testing.T) {
	batch := []*OplogRecord{rec(), rec(), rec()}

	// out-of-range index falls back to the whole batch (pre-#1047 behavior)
	got := failedInsertSubset(batch, mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(9, "x")}})
	assert.Len(t, got, 3)

	// no write errors -> whole batch
	assert.Len(t, failedInsertSubset(batch, mongo.BulkWriteException{}), 3)

	// server reports out of order; subset must follow model order
	got = failedInsertSubset(batch, mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{dupWriteErr(2, "x"), dupWriteErr(0, "x")}})
	require.Len(t, got, 2)
	assert.Same(t, batch[0], got[0])
	assert.Same(t, batch[2], got[1])
}

func TestParseDupKeyIndexNameAndFields(t *testing.T) {
	assert.Equal(t, "", parseDupKeyIndexName(nil))
	assert.Equal(t, "_id_", parseDupKeyIndexName(errors.New(
		"E11000 duplicate key error collection: db.c index: _id_ dup key: { _id: 1 }")))
	assert.Equal(t, "uniq_x", parseDupKeyIndexName(errors.New(
		"E11000 duplicate key error collection: db.c index: uniq_x dup key: { x: 1 }")))
	assert.Equal(t, "_id_", parseDupKeyIndexName(errors.New(
		"Duplicate key violation on the requested collection: Index '_id_'")))
	assert.Equal(t, "", parseDupKeyIndexName(errors.New("no index info here")))

	assert.Nil(t, parseDupKeyFields(nil))
	assert.Nil(t, parseDupKeyFields(errors.New("no dup key content")))
	fields := parseDupKeyFields(errors.New(
		"E11000 duplicate key error collection: db.c index: uniq_x dup key: { x: 1, y: 2 }"))
	assert.Equal(t, []string{"x", "y"}, fields)
}
