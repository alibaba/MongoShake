package docsyncer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestDocumentReaderResumeQueryPreservesPieceRange(t *testing.T) {
	lastReadID := bson.RawValue{Type: bson.TypeInt64, Value: []byte{100, 0, 0, 0, 0, 0, 0, 0}}
	reader := NewDocumentReader(0, "", utils.NS{}, "sk", "k000000", "k000999", "")
	reader.lastReadID = lastReadID

	resumeQuery := reader.resumeQuery()
	assert.Equal(t, bson.M{
		"sk":  bson.M{"$gt": "k000000", "$lte": "k000999"},
		"_id": bson.M{"$gt": lastReadID},
	}, resumeQuery)
	assert.Equal(t, bson.M{"sk": bson.M{"$gt": "k000000", "$lte": "k000999"}}, reader.query)

	_, err := bson.Marshal(resumeQuery)
	assert.NoError(t, err, "a raw _id value must be valid in a MongoDB query")
}

func TestDocumentReaderResumeQueryUsesIDForAllReaderModes(t *testing.T) {
	lastReadID := bson.RawValue{Type: bson.TypeInt32, Value: []byte{100, 0, 0, 0}}
	tests := []struct {
		name   string
		key    string
		start  interface{}
		end    interface{}
		query  bson.M
		resume bson.M
	}{
		{
			name:   "single reader",
			query:  bson.M{},
			resume: bson.M{"_id": bson.M{"$gt": lastReadID}},
		},
		{
			name:  "id range reader",
			key:   "_id",
			start: int32(10),
			end:   int32(200),
			query: bson.M{"_id": bson.M{"$gt": int32(10), "$lte": int32(200)}},
			resume: bson.M{"_id": bson.M{"$gt": lastReadID, "$lte": int32(200)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := NewDocumentReader(0, "", utils.NS{}, test.key, test.start, test.end, "")
			reader.lastReadID = lastReadID
			assert.Equal(t, test.resume, reader.resumeQuery())
			assert.Equal(t, test.query, reader.query)
		})
	}
}

func TestDocumentReaderStoresIndependentRawID(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: int64(100)}, {Key: "sk", Value: "k000100"}})
	assert.NoError(t, err)
	expected := append([]byte(nil), bson.Raw(doc).Lookup("_id").Value...)

	reader := NewDocumentReader(0, "", utils.NS{}, "", nil, nil, "")
	reader.setLastReadID(bson.Raw(doc))
	for index := range doc {
		doc[index] = 0
	}

	assert.Equal(t, bson.TypeInt64, reader.lastReadID.Type)
	assert.Equal(t, expected, reader.lastReadID.Value)
}

func TestIsTransientReadErrorRecognizesCursorInvalidation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"query plan killed by code", mongo.CommandError{Code: 175, Name: "QueryPlanKilled"}, true},
		{"cursor killed by code", mongo.CommandError{Code: 237, Name: "CursorKilled"}, true},
		{"cursor not found by code", mongo.CommandError{Code: 43, Name: "CursorNotFound"}, true},
		{"wrapped cursor invalidation", fmt.Errorf("source cursor failed: %w", mongo.CommandError{Code: 43, Name: "CursorNotFound"}), true},
		{"mongos cursor id message", fmt.Errorf("(CursorNotFound) cursor id 2488742369752141800 not found"), true},
		{"non transient command error", mongo.CommandError{Code: 13, Name: "Unauthorized"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, isTransientReadError(test.err))
		})
	}
}
