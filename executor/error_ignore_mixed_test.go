package executor

import (
	"errors"
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// Exercise execute, including its outer ignore policy, rather than only the writer.
func TestExecutorMixed61DoesNotComplete(t *testing.T) {
	oldOptions, oldLogger := conf.Options, l.Logger
	require.NoError(t, l.New("info", t.TempDir(), "outer61.log", true, 20, 7, 0))
	defer func() { _ = l.Logger.Close(); conf.Options, l.Logger = oldOptions, oldLogger }()
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, kind := range []string{"validation", "unauthorized", "duplicate", "concern", "concern61_validation", "pure61"} {
		mt.Run(kind, func(mt *mtest.T) {
			conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: "ignore"}
			response := mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 61, Message: "shard key missing"})
			switch kind {
			case "validation", "unauthorized", "duplicate":
				code := 121
				if kind == "unauthorized" {
					code = 13
				}
				if kind == "duplicate" {
					code = 11000
				}
				response = mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 61, Message: "shard key missing"}, mtest.WriteError{Index: 1, Code: code, Message: "real write failed"})
			case "concern":
				response = append(response, bson.E{Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 64}, {Key: "errmsg", Value: "durability not confirmed"}}})
			case "concern61_validation":
				response = mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 1, Code: 121, Message: "Document failed validation"})
				response = append(response, bson.E{Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 61}, {Key: "errmsg", Value: "injected concern 61"}}})
			}
			// ensureConnection pings first; then execute sends the actual insert.
			mt.AddMockResponses(mtest.CreateSuccessResponse(), response)
			records := make([]*OplogRecord, 2)
			for i := range records {
				records[i], _ = lazyRecord(mt, "i", bson.D{{Key: "_id", Value: int32(i)}})
			}
			completed := false
			records[1].wait = func() { completed = true }
			executor := &Executor{batchExecutor: &BatchGroupExecutor{}, conn: &utils.MongoCommunityConn{Client: mt.Client}, bulkInsert: true}
			err := executor.execute(&OplogsGroup{ns: "db.mixed", op: "i", oplogRecords: records})
			if kind == "pure61" {
				require.NoError(mt, err)
				require.True(mt, completed)
				return
			}
			require.Error(mt, err, "mixed failures must not complete the execution group")
			require.False(mt, completed, "failed group must not signal successful completion")
			require.Equal(mt, uint64(2), executor.metricError)
			var bulk mongo.BulkWriteException
			require.True(mt, errors.As(err, &bulk), "retain original write and durability errors")
			require.True(mt, bulk.HasErrorCode(61))
			if kind == "validation" || kind == "concern61_validation" {
				require.True(mt, bulk.HasErrorCode(121))
			}
		})
	}
}

func TestErrorIgnoreRequiresAllErrorsKnown(t *testing.T) {
	fcv := "Invalid value for featureCompatibilityVersion"
	for _, form := range []string{"bulk", "write"} {
		for _, tc := range []struct {
			name    string
			codes   []int
			concern int
			ns, op  string
			message string
			want    bool
		}{
			{name: "pure61", codes: []int{61, 61}, want: true},
			{name: "61_then121", codes: []int{61, 121}},
			{name: "121_then61", codes: []int{121, 61}},
			{name: "61_then13", codes: []int{61, 13}},
			{name: "61_then11000", codes: []int{61, 11000}},
			{name: "61_and_concern64", codes: []int{61}, concern: 64},
			{name: "concern61_only", concern: 61},
			{name: "concern61_and121", codes: []int{121}, concern: 61},
			{name: "empty"},
			{name: "fcv", codes: []int{2}, ns: "admin.system.version", op: "u", message: fcv, want: true},
			{name: "fcv_mixed121", codes: []int{2, 121}, ns: "admin.system.version", op: "u", message: fcv},
			{name: "fcv_with_concern", codes: []int{2}, concern: 64, ns: "admin.system.version", op: "u", message: fcv},
			{name: "fcv_wrong_namespace", codes: []int{2}, ns: "db.other", op: "u", message: fcv},
			{name: "fcv_wrong_operation", codes: []int{2}, ns: "admin.system.version", op: "i", message: fcv},
		} {
			t.Run(form+"/"+tc.name, func(t *testing.T) {
				var concern *mongo.WriteConcernError
				if tc.concern != 0 {
					concern = &mongo.WriteConcernError{Code: tc.concern, Message: "concern failure"}
				}
				write := mongo.WriteException{WriteConcernError: concern}
				bulk := mongo.BulkWriteException{WriteConcernError: concern}
				for i, code := range tc.codes {
					message := "write failed"
					if code == 2 {
						message = tc.message
					}
					we := mongo.WriteError{Index: i, Code: code, Message: message}
					write.WriteErrors = append(write.WriteErrors, we)
					bulk.WriteErrors = append(bulk.WriteErrors, mongo.BulkWriteError{WriteError: we})
				}
				var err error = bulk
				if form == "write" {
					err = write
				}
				require.Equal(t, tc.want, (&Executor{}).errorIgnore(err, tc.ns, tc.op))
			})
		}
	}
	for _, tc := range []struct {
		name   string
		err    error
		ns, op string
		want   bool
	}{
		{"nil", nil, "", "", false},
		{"command61", mongo.CommandError{Code: 61}, "", "", true},
		{"write61", mongo.WriteError{Code: 61}, "", "", true},
		{"command121", mongo.CommandError{Code: 121}, "", "", false},
		{"command_fcv", mongo.CommandError{Code: 2, Message: fcv}, "admin.system.version", "u", true},
		{"command_fcv_wrong_op", mongo.CommandError{Code: 2, Message: fcv}, "admin.system.version", "i", false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, (&Executor{}).errorIgnore(tc.err, tc.ns, tc.op)) })
	}
}
