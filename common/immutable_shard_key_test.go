package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestIsImmutableShardKeyError covers the full version spectrum of the
// immutable-shard-key error shape:
//   - code 66  (ImmutableField) on MongoDB 4.0–4.4
//   - code 31025 (ShardKeyUpdateForbidden) on MongoDB 5.0+
//   - mongos "caused by" chain via message substring
//   - CommandError wrapping (non-BulkWriteException path)
func TestIsImmutableShardKeyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "bulk write code 66 (4.0 ImmutableField)",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{
					{WriteError: mongo.WriteError{Index: 0, Code: 66, Message: "After applying the update, the immutable field 'payCompanyInfo.companyId' was found to have been altered"}},
				},
			},
			want: true,
		},
		{
			name: "bulk write code 31025 (5.0+ ShardKeyUpdateForbidden)",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{
					{WriteError: mongo.WriteError{Index: 0, Code: 31025, Message: "Shard key update is not allowed without specifying the full shard key in the query"}},
				},
			},
			want: true,
		},
		{
			name: "bulk write duplicate key 11000 (not immutable)",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{
					{WriteError: mongo.WriteError{Index: 0, Code: 11000, Message: "E11000 duplicate key error"}},
				},
			},
			want: false,
		},
		{
			name: "command error code 66",
			err:  mongo.CommandError{Code: 66, Name: "ImmutableField", Message: "immutable field altered"},
			want: true,
		},
		{
			name: "command error code 31025",
			err:  mongo.CommandError{Code: 31025, Name: "Location31025", Message: "Shard key update is not allowed"},
			want: true,
		},
		{
			name: "message substring immutable field",
			err:  errors.New("Executor error during getMore :: caused by :: After applying the update, the immutable field 'a' was found to have been altered"),
			want: true,
		},
		{
			name: "message substring was found to have been altered",
			err:  errors.New("write error: the field 'companyId' was found to have been altered"),
			want: true,
		},
		{
			name: "message substring shard key update is not allowed",
			err:  errors.New("Shard key update is not allowed without specifying the full shard key in the query"),
			want: true,
		},
		{
			name: "message substring without specifying the full shard key",
			err:  errors.New("update failed: without specifying the full shard key"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("something else entirely"),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsImmutableShardKeyError(tc.err)
			assert.Equal(t, tc.want, got, "IsImmutableShardKeyError(%v) = %v, want %v", tc.err, got, tc.want)
		})
	}
}
