package executor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestBulkInsertDupKeyLogRequiresSuccessfulHandling(t *testing.T) {
	originalOptions, originalLog, originalDupLog := conf.Options, l.Logger, utils.GlobalDupKeyLog
	logDir := t.TempDir()
	require.NoError(t, l.New("info", logDir, "dup.log", true, 20, 7, 0))
	utils.GlobalDupKeyLog = utils.NewDupKeyLog()
	defer func() {
		utils.GlobalDupKeyLog.Flush()
		_ = l.Logger.Close()
		conf.Options, l.Logger, utils.GlobalDupKeyLog = originalOptions, originalLog, originalDupLog
	}()
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, tc := range []struct {
		name        string
		dupUpdate   bool
		strategy    string
		updateFails bool
		wantHandled bool
		wantError   bool
	}{
		{name: "converted", dupUpdate: true, wantHandled: true},
		{name: "conversion_failed", dupUpdate: true, updateFails: true, wantError: true},
		{name: "ignored", strategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore, wantHandled: true},
		{name: "unhandled", strategy: utils.VarIncrSyncExecutorDupKeyStrategyError, wantError: true},
	} {
		mt.Run(tc.name, func(mt *mtest.T) {
			conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: tc.strategy}
			record, _ := lazyRecord(mt, "i", bson.D{{"_id", int32(1)}, {"value", "payload"}})
			mt.AddMockResponses(mtest.CreateWriteErrorsResponse(mtest.WriteError{
				Index: 0, Code: 11000, Message: "E11000 duplicate key index: _id_",
			}))
			if tc.dupUpdate {
				if tc.updateFails {
					mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "update unauthorized"}))
				} else {
					mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}))
				}
			}
			writer := &BulkWriter{conn: &utils.MongoCommunityConn{Client: mt.Client}}
			err := writer.doInsert("db", tc.name, bson.E{}, []*OplogRecord{record}, tc.dupUpdate)
			if tc.wantError {
				require.Error(mt, err)
			} else {
				require.NoError(mt, err)
			}
			utils.GlobalDupKeyLog.Flush()
			content, readErr := os.ReadFile(filepath.Join(logDir, "dup.log"))
			require.NoError(mt, readErr)
			handled := "dup key handled on ns[db." + tc.name + "]"
			if tc.wantHandled {
				require.Contains(mt, string(content), handled)
			} else {
				require.NotContains(mt, string(content), handled)
			}
		})
	}
}

func TestBulkInsertDupKeyLogCountsAndDebugDetail(t *testing.T) {
	for _, tc := range []struct {
		name         string
		level        string
		strategy     string
		convert      bool
		decline      bool
		mixed        bool
		writeConcern bool
	}{
		{name: "convert_info", level: "info", convert: true},
		{name: "ignore_info", level: "info", strategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore},
		{name: "skip_info", level: "info", strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip},
		{name: "convert_debug", level: "debug", convert: true},
		{name: "ignore_debug", level: "debug", strategy: utils.VarIncrSyncExecutorDupKeyStrategyIgnore},
		{name: "skip_debug", level: "debug", strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip},
		{name: "skip_declined", level: "info", strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip, decline: true},
		{name: "skip_mixed", level: "info", strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip, mixed: true},
		{name: "skip_write_concern", level: "info", strategy: utils.VarIncrSyncExecutorDupKeyStrategySkip, writeConcern: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalOptions, originalLog, originalDupLog := conf.Options, l.Logger, utils.GlobalDupKeyLog
			logDir := t.TempDir()
			require.NoError(t, l.New(tc.level, logDir, "dup.log", true, 20, 7, 0))
			utils.GlobalDupKeyLog = utils.NewDupKeyLog()
			defer func() {
				utils.GlobalDupKeyLog.Flush()
				_ = l.Logger.Close()
				conf.Options, l.Logger, utils.GlobalDupKeyLog = originalOptions, originalLog, originalDupLog
			}()
			conf.Options = conf.Configuration{IncrSyncExecutorDupKeyStrategy: tc.strategy}
			mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
			mt.Run("two_batches", func(mt *mtest.T) {
				records := make([]*OplogRecord, 128)
				for id := range records {
					records[id], _ = lazyRecord(mt, "i", bson.D{{Key: "_id", Value: int32(id)}, {Key: "value", Value: "new"}})
				}
				tail := "tail-duplicate-key-99-visible-only-in-complete-detail"
				index := "_id_"
				if tc.decline {
					index = "not_allowlisted_1"
				}
				secondCode := 11000
				if tc.mixed {
					secondCode = 121
				}
				writer := &BulkWriter{conn: &utils.MongoCommunityConn{Client: mt.Client}}
				wantError := tc.decline || tc.mixed || tc.writeConcern
				for batch := 0; batch < 2; batch++ {
					response := mtest.CreateWriteErrorsResponse(
						mtest.WriteError{Index: 7, Code: 11000, Message: "E11000 index: " + index + " dup key: " + strings.Repeat("x", 600)},
						mtest.WriteError{Index: 99, Code: secondCode, Message: "E11000 index: " + index + " dup key: " + tail})
					if tc.writeConcern {
						response = append(response, bson.E{Key: "writeConcernError", Value: bson.D{
							{Key: "code", Value: 64}, {Key: "errmsg", Value: "write concern failed"},
						}})
					}
					mt.AddMockResponses(response)
					if tc.convert {
						mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(128)}))
					}
					err := writer.doInsert("db", "probe", bson.E{}, records, tc.convert)
					if wantError {
						require.Error(mt, err)
					} else {
						require.NoError(mt, err)
					}
				}
				utils.GlobalDupKeyLog.Flush()
				content, err := os.ReadFile(filepath.Join(logDir, "dup.log"))
				require.NoError(mt, err)
				text := string(content)
				if wantError {
					require.NotContains(mt, text, "dup key handled on ns[")
					require.Contains(mt, text, "[WARNING]")
					require.Contains(mt, text, tail, "declined/mixed/concern errors must keep full warning detail")
				} else {
					require.Contains(mt, text, "docs[2] (convert-to-update/ignore/skip)")
					require.Contains(mt, text, "additional batches[1] docs[2]")
					require.NotContains(mt, text, "docs[128]")
					require.NotContains(mt, text, "[WARNING]")
					if tc.level == "info" {
						require.NotContains(mt, text, tail)
						require.Equal(mt, 2, strings.Count(text, "[INFO]"), "bulk handling must not retain per-batch Info lines")
					} else {
						require.Contains(mt, text, tail)
					}
				}
				var inserts, updates int
				for _, event := range mt.GetAllStartedEvents() {
					if event.CommandName == "insert" {
						inserts++
					}
					if event.CommandName == "update" {
						values, err := event.Command.Lookup("updates").Array().Values()
						require.NoError(mt, err)
						require.Len(mt, values, 128, "logging must not change bulk conversion's whole-batch replay")
						updates++
					}
				}
				require.Equal(mt, 2, inserts)
				if tc.convert {
					require.Equal(mt, 2, updates)
				} else {
					require.Zero(mt, updates)
				}
			})
		})
	}
}
