package docsyncer

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

func TestFullSyncDupKeyLogRequiresSuccessfulHandling(t *testing.T) {
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
		updateFails bool
		timeseries  bool
		missingID   bool
		fallback    bool
		insertCode  int
		wantHandled bool
	}{
		{name: "converted", wantHandled: true},
		{name: "conversion_failed", updateFails: true},
		{name: "timeseries", timeseries: true},
		{name: "missing_id", missingID: true},
		{name: "fallback_succeeded", fallback: true, wantHandled: true},
		{name: "non_duplicate", insertCode: 121},
	} {
		mt.Run(tc.name, func(mt *mtest.T) {
			conf.Options = conf.Configuration{
				FullSyncExecutorInsertOnDupUpdate:         true,
				FullSyncExecutorImmutableShardKeyFallback: tc.fallback,
			}
			object := bson.D{{"value", "payload"}}
			if !tc.missingID {
				object = append(object, bson.E{Key: "_id", Value: int32(1)})
			}
			data, err := bson.Marshal(object)
			require.NoError(mt, err)
			raw := bson.Raw(data)
			code := tc.insertCode
			if code == 0 {
				code = 11000
			}
			mt.AddMockResponses(mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: code, Message: "insert failed"}))
			if code == 11000 {
				stats := mtest.CreateSuccessResponse()
				if tc.timeseries {
					stats = mtest.CreateSuccessResponse(bson.E{Key: "timeseries", Value: bson.D{}})
				}
				mt.AddMockResponses(stats) // IsTimeSeriesCollection issues collStats.
				if !tc.timeseries && !tc.missingID {
					if tc.updateFails {
						mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "update unauthorized"}))
					} else if tc.fallback {
						mt.AddMockResponses(
							mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 66, Message: "immutable field altered"}),
							mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}),
							mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}),
						)
					} else {
						mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}))
					}
				}
			}
			exec := NewDocExecutor(0, &CollectionExecutor{ns: utils.NS{Database: "db", Collection: tc.name}},
				&utils.MongoCommunityConn{Client: mt.Client}, &DBSyncer{qos: &utils.Qos{}})
			err = exec.doSync([]*bson.Raw{&raw})
			if tc.wantHandled {
				require.NoError(mt, err)
			} else {
				require.Error(mt, err)
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

// Two unordered 128-document batches contain only two duplicate write errors
// each. The other 126 documents must not inflate the duplicate counter.
func TestFullSyncDupKeyLogCountsAndDebugDetail(t *testing.T) {
	for _, level := range []string{"info", "debug", "all"} {
		t.Run(level, func(t *testing.T) {
			originalOptions, originalLog, originalDupLog := conf.Options, l.Logger, utils.GlobalDupKeyLog
			logDir := t.TempDir()
			require.NoError(t, l.New(level, logDir, "dup.log", true, 20, 7, 0))
			utils.GlobalDupKeyLog = utils.NewDupKeyLog()
			defer func() {
				utils.GlobalDupKeyLog.Flush()
				_ = l.Logger.Close()
				conf.Options, l.Logger, utils.GlobalDupKeyLog = originalOptions, originalLog, originalDupLog
			}()
			conf.Options = conf.Configuration{FullSyncExecutorInsertOnDupUpdate: true}
			mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
			mt.Run("two_batches", func(mt *mtest.T) {
				docs := make([]*bson.Raw, 128)
				for id := range docs {
					data, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(id)}, {Key: "value", Value: "new"}})
					require.NoError(mt, err)
					raw := bson.Raw(data)
					docs[id] = &raw
				}
				tail := "tail-duplicate-key-99-visible-only-in-complete-detail"
				exec := NewDocExecutor(0, &CollectionExecutor{ns: utils.NS{Database: "db", Collection: "probe"}},
					&utils.MongoCommunityConn{Client: mt.Client}, &DBSyncer{qos: &utils.Qos{}})
				for batch := 0; batch < 2; batch++ {
					mt.AddMockResponses(
						mtest.CreateWriteErrorsResponse(
							mtest.WriteError{Index: 7, Code: 11000, Message: "E11000 " + strings.Repeat("x", 600)},
							mtest.WriteError{Index: 99, Code: 11000, Message: "E11000 " + tail}),
						mtest.CreateSuccessResponse(), mtest.CreateSuccessResponse(),
						mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(2)}))
					require.NoError(mt, exec.doSync(docs))
				}
				utils.GlobalDupKeyLog.Flush()
				content, err := os.ReadFile(filepath.Join(logDir, "dup.log"))
				require.NoError(mt, err)
				text := string(content)
				require.Contains(mt, text, "docs[2] (convert-to-update/ignore/skip)")
				require.Contains(mt, text, "additional batches[1] docs[2]")
				require.NotContains(mt, text, "docs[128]")
				require.NotContains(mt, text, "[WARNING]")
				if level == "info" {
					require.NotContains(mt, text, tail)
					require.Equal(mt, 2, strings.Count(text, "[INFO]"))
				} else {
					require.Equal(mt, 2, strings.Count(text, tail))
				}
				var inserts, updates int
				for _, event := range mt.GetAllStartedEvents() {
					switch event.CommandName {
					case "insert":
						values, err := event.Command.Lookup("documents").Array().Values()
						require.NoError(mt, err)
						require.Len(mt, values, 128)
						inserts++
					case "update":
						values, err := event.Command.Lookup("updates").Array().Values()
						require.NoError(mt, err)
						require.Len(mt, values, 2)
						for i, id := range []int32{7, 99} {
							update := values[i].Document()
							require.Equal(mt, id, update.Lookup("q", "_id").Int32())
							require.Equal(mt, "new", update.Lookup("u", "$set", "value").StringValue())
						}
						updates++
					}
				}
				require.Equal(mt, 2, inserts)
				require.Equal(mt, 2, updates)
			})
		})
	}
}
