package barrier_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Opt-in live test: only accepts loopback MongoDB servers and creates unique
// databases. The source must be a replica set; both servers need test commands.
func TestBarrierLive(t *testing.T) {
	sourceURI, targetURI := os.Getenv("MONGOSHAKE_BARRIER_SOURCE"), os.Getenv("MONGOSHAKE_BARRIER_TARGET")
	after, before := os.Getenv("MONGOSHAKE_TEST_COLLECTOR_BINARY"), os.Getenv("MONGOSHAKE_BARRIER_BASELINE")
	if sourceURI == "" || targetURI == "" || after == "" {
		t.Skip("set MONGOSHAKE_BARRIER_SOURCE, MONGOSHAKE_BARRIER_TARGET and MONGOSHAKE_TEST_COLLECTOR_BINARY")
	}
	for _, address := range []string{sourceURI, targetURI} {
		u, err := url.Parse(address)
		require.NoError(t, err)
		require.Equal(t, "mongodb", u.Scheme)
		require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, u.Hostname())
	}
	require.NotEqual(t, sourceURI, targetURI)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	source, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceURI).SetRetryWrites(false).SetServerSelectionTimeout(3*time.Second))
	require.NoError(t, err)
	target, err := mongo.Connect(ctx, options.Client().ApplyURI(targetURI).SetServerSelectionTimeout(3*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = source.Disconnect(context.Background())
		_ = target.Disconnect(context.Background())
	})
	require.NoError(t, source.Ping(ctx, nil))
	require.NoError(t, target.Ping(ctx, nil))
	var version struct{ Version string }
	require.NoError(t, source.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&version))
	output := os.Getenv("MONGOSHAKE_BARRIER_OUTPUT")
	if output == "" {
		output = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(output, 0700))
	type result struct {
		Name              string  `json:"name"`
		Workers           int     `json:"workers"`
		Prepared          int64   `json:"prepared_transactions"`
		Commits           int64   `json:"commit_records"`
		Documents         int64   `json:"target_documents"`
		Seconds           float64 `json:"collector_seconds_including_startup"`
		CheckpointReached bool    `json:"checkpoint_reached_end_fence"`
		PauseConfirmed    bool    `json:"pause_resume_confirmed"`
		WriteErrorSeen    bool    `json:"checkpoint_write_error_seen"`
		OutageProgress    bool    `json:"progress_during_checkpoint_outage"`
		OutageCheckpoint  int64   `json:"checkpoint_during_outage"`
		Passed            bool    `json:"passed"`
	}
	results := []result{}
	t.Cleanup(func() {
		report := map[string]interface{}{"platform": "native host", "mongodb_version": version.Version,
			"transactions_per_case": 60, "passed": !t.Failed(), "cases": results,
			"scope": "real collector, real prepared transactions, MongoDB checkpoint storage, DDL and failpoints; not a Docker/Linux runtime test"}
		data, marshalErr := json.MarshalIndent(report, "", "  ")
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(filepath.Join(output, "results.json"), append(data, '\n'), 0600))
	})
	for _, tc := range []struct {
		name, binary                                    string
		workers                                         int
		ddl, pause, checkpointFailure, checkpointOutage bool
	}{
		{"before_300ms", before, 1, false, false, false, false},
		{"after_one_worker", after, 1, false, false, false, false},
		{"after_two_workers", after, 2, false, false, false, false},
		{"ddl_slow_worker", after, 2, true, false, false, false},
		{"pause_resume", after, 2, false, true, false, false},
		{"checkpoint_retry", after, 2, false, false, true, false},
		{"checkpoint_outage", after, 2, false, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.binary == "" {
				t.Skip("set MONGOSHAKE_BARRIER_BASELINE for the former 300ms implementation")
			}
			entry := result{Name: tc.name, Workers: tc.workers}
			defer func() { entry.Passed = !t.Failed(); results = append(results, entry) }()
			name := fmt.Sprintf("barrier_%x_%s", time.Now().UnixNano(), tc.name)
			srcDB, dstDB, ckptDB := source.Database(name), target.Database(name), target.Database(name+"_ckpt")
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_ = srcDB.Drop(cleanup)
				_ = dstDB.Drop(cleanup)
				_ = ckptDB.Drop(cleanup)
			})
			require.NoError(t, srcDB.CreateCollection(ctx, "docs"))
			start := lastOplogTS(t, ctx, source)
			if tc.ddl {
				batch := make([]interface{}, 1000)
				for i := range batch {
					batch[i] = bson.D{{Key: "_id", Value: i}, {Key: "value", Value: i}}
				}
				_, err = srcDB.Collection("docs").InsertMany(ctx, batch)
				require.NoError(t, err)
				require.NoError(t, srcDB.Collection("docs").Drop(ctx))
				_, err = srcDB.Collection("docs").InsertOne(ctx, bson.D{{Key: "_id", Value: -1}, {Key: "value", Value: "after_drop"}})
				require.NoError(t, err)
			} else {
				for i := 0; i < 60; i++ {
					prepareAndCommit(t, ctx, source, srcDB, i)
				}
			}
			// A trailing DDL forces checkpoint confirmation after every business op.
			require.NoError(t, srcDB.CreateCollection(ctx, "end_fence"))
			end := lastOplogTS(t, ctx, source)
			rangeFilter := bson.D{{Key: "$gt", Value: start}, {Key: "$lte", Value: end}}
			entry.Prepared, err = source.Database("local").Collection("oplog.rs").CountDocuments(ctx,
				bson.D{{Key: "ts", Value: rangeFilter}, {Key: "o.prepare", Value: true}})
			require.NoError(t, err)
			entry.Commits, err = source.Database("local").Collection("oplog.rs").CountDocuments(ctx,
				bson.D{{Key: "ts", Value: rangeFilter}, {Key: "o.commitTransaction", Value: 1}})
			require.NoError(t, err)
			if !tc.ddl {
				require.Equal(t, int64(60), entry.Prepared)
				require.Equal(t, int64(60), entry.Commits)
			}
			_, err = ckptDB.Collection("checkpoint").InsertOne(ctx, bson.D{
				{Key: "name", Value: "barrier_src"}, {Key: "ckpt", Value: timestampValue(start)},
				{Key: "version", Value: 2}, {Key: "fetch_method", Value: "oplog"},
			})
			require.NoError(t, err)
			if tc.ddl || tc.pause || tc.checkpointFailure {
				data := bson.D{{Key: "failCommands", Value: bson.A{"insert"}}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 1000}}
				if tc.pause {
					data[2].Value = 1500
				}
				if tc.checkpointFailure {
					data = bson.D{{Key: "failCommands", Value: bson.A{"update"}}, {Key: "errorCode", Value: 2}}
				}
				var mode interface{} = bson.D{{Key: "times", Value: 1}}
				if tc.checkpointOutage {
					mode = "alwaysOn"
				}
				require.NoError(t, target.Database("admin").RunCommand(ctx, bson.D{
					{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: mode}, {Key: "data", Value: data},
				}).Err())
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					if err := target.Database("admin").RunCommand(cleanup, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err(); err != nil {
						t.Errorf("failpoint cleanup: %v", err)
					}
				})
			}
			caseDir := filepath.Join(output, tc.name)
			require.NoError(t, os.MkdirAll(caseDir, 0700))
			api, profile := freePort(t), freePort(t)
			configuration := fmt.Sprintf(`conf.version = 14
id = barrier_%s
master_quorum = false
sync_mode = incr
mongo_urls = %s
mongo_connect_mode = primary
tunnel = direct
tunnel.address = %s
filter.namespace.white = %s
filter.ddl_enable = true
checkpoint.storage.url = %s
checkpoint.storage.db = %s
checkpoint.storage.collection = checkpoint
checkpoint.interval = 20
incr_sync.mongo_fetch_method = oplog
incr_sync.barrier.ordering_enable = true
incr_sync.shard_key = id
incr_sync.worker = %d
incr_sync.executor = 2
incr_sync.reader.fetch_batch_size = 1024
incr_sync.reader.buffer_time = 1
incr_sync.executor.insert_on_dup_update = true
incr_sync.http_port = %d
full_sync.http_port = 0
prom.http_port = 0
system_profile_port = %d
log.dir = %s
log.file = collector.log
log.level = info
log.flush = true
`, tc.name, sourceURI, targetURI, name, targetURI, ckptDB.Name(), tc.workers, api, profile, caseDir)
			confPath := filepath.Join(caseDir, "collector.conf")
			require.NoError(t, os.WriteFile(confPath, []byte(configuration), 0600))
			logPath := filepath.Join(caseDir, "stdout.log")
			logFile, openErr := os.Create(logPath)
			require.NoError(t, openErr)
			cmd := exec.CommandContext(ctx, tc.binary, "-conf", confPath, "-verbose", "2")
			cmd.Dir, cmd.Stdout, cmd.Stderr = caseDir, logFile, logFile
			started := time.Now()
			require.NoError(t, cmd.Start())
			stopped := false
			stopCollector := func() {
				if !stopped {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					_ = logFile.Close()
					stopped = true
				}
			}
			t.Cleanup(stopCollector)
			t.Cleanup(func() {
				if !t.Failed() {
					return
				}
				stopCollector()
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				snapshot := bson.M{}
				for label, collection := range map[string]*mongo.Collection{
					"source_docs": srcDB.Collection("docs"), "target_docs": dstDB.Collection("docs"),
					"source_progress": srcDB.Collection("progress"), "target_progress": dstDB.Collection("progress"),
					"checkpoint": ckptDB.Collection("checkpoint"),
				} {
					cursor, err := collection.Find(cleanup, bson.D{})
					if err != nil {
						t.Errorf("snapshot %s: %v", label, err)
						continue
					}
					var docs []bson.M
					if err := cursor.All(cleanup, &docs); err != nil {
						t.Errorf("snapshot %s: %v", label, err)
					}
					snapshot[label] = docs
				}
				data, err := bson.MarshalExtJSON(snapshot, true, false)
				if err == nil {
					err = os.WriteFile(filepath.Join(caseDir, "failure-snapshot.ejson"), data, 0600)
				}
				if err != nil {
					t.Errorf("write failure snapshot: %v", err)
				}
			})
			if tc.pause {
				base := fmt.Sprintf("http://127.0.0.1:%d", api)
				require.Eventually(t, func() bool {
					response, getErr := http.Get(base + "/worker")
					if getErr != nil {
						return false
					}
					defer response.Body.Close()
					var workers []struct {
						Ack   string `json:"last_ack"`
						Unack string `json:"last_unack"`
					}
					if json.NewDecoder(response.Body).Decode(&workers) != nil {
						return false
					}
					for _, worker := range workers {
						if worker.Ack != worker.Unack {
							return true
						}
					}
					return false
				}, 10*time.Second, 10*time.Millisecond)
				setPause(t, base, true)
				require.Eventually(t, func() bool {
					data, _ := os.ReadFile(logPath)
					return bytes.Contains(data, []byte("barrier wait interrupted"))
				}, time.Second, 10*time.Millisecond)
				setPause(t, base, false)
			}
			durableEnd := end
			if tc.checkpointOutage {
				collectionExists := func(name string) bool {
					names, err := dstDB.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
					return err == nil && len(names) == 1
				}
				require.Eventually(t, func() bool { return collectionExists("end_fence") }, 15*time.Second, 20*time.Millisecond)
				// This input follows an already-applied DDL while every checkpoint
				// update still fails. Seeing it proves the batcher passed that
				// DDL's ACK barrier rather than merely applying the DDL itself.
				marker := bson.M{"_id": "during_outage", "value": true}
				_, err = srcDB.Collection("progress").InsertOne(ctx, marker)
				require.NoError(t, err)
				require.NoError(t, srcDB.CreateCollection(ctx, "outage_progress"))
				durableEnd = lastOplogTS(t, ctx, source)
				require.Eventually(t, func() bool { return collectionExists("outage_progress") }, 15*time.Second, 20*time.Millisecond, "checkpoint outage blocked later DML/DDL; log: %s", logPath)
				var actual bson.M
				require.NoError(t, dstDB.Collection("progress").FindOne(ctx, bson.D{{Key: "_id", Value: "during_outage"}}).Decode(&actual))
				require.Equal(t, marker, actual)
				var checkpoint struct {
					TS int64 `bson:"ckpt"`
				}
				require.NoError(t, ckptDB.Collection("checkpoint").FindOne(ctx, bson.D{{Key: "name", Value: "barrier_src"}}).Decode(&checkpoint))
				entry.OutageCheckpoint = checkpoint.TS
				require.Equal(t, timestampValue(start), checkpoint.TS, "ACK completion must not fabricate durable progress")
				entry.OutageProgress = true
				require.NoError(t, target.Database("admin").RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err())
				// Finite normal input resumes the existing checkpoint writer;
				// neither the checkpoint nor transaction pins are edited.
				require.NoError(t, srcDB.CreateCollection(ctx, "recovery_fence"))
				require.Eventually(t, func() bool { return collectionExists("recovery_fence") }, 15*time.Second, 20*time.Millisecond)
			}
			require.Eventually(t, func() bool {
				var checkpoint struct {
					TS int64 `bson:"ckpt"`
				}
				return ckptDB.Collection("checkpoint").FindOne(ctx, bson.D{{Key: "name", Value: "barrier_src"}}).Decode(&checkpoint) == nil && checkpoint.TS >= timestampValue(durableEnd)
			}, 45*time.Second, 20*time.Millisecond, "collector did not reach end fence; log: %s", logPath)
			entry.Seconds, entry.CheckpointReached = time.Since(started).Seconds(), true
			entry.Documents, err = dstDB.Collection("docs").CountDocuments(ctx, bson.D{})
			require.NoError(t, err)
			if tc.ddl {
				require.Equal(t, int64(1), entry.Documents, "late pre-drop inserts must not recreate old data")
				var actual struct {
					Value string `bson:"value"`
				}
				require.NoError(t, dstDB.Collection("docs").FindOne(ctx, bson.D{{Key: "_id", Value: -1}}).Decode(&actual))
				require.Equal(t, "after_drop", actual.Value)
			} else {
				require.Equal(t, int64(60), entry.Documents)
				for i := 0; i < 60; i++ {
					var expected, actual bson.M
					filter := bson.D{{Key: "_id", Value: i}}
					require.NoError(t, srcDB.Collection("docs").FindOne(ctx, filter).Decode(&expected))
					require.NoError(t, dstDB.Collection("docs").FindOne(ctx, filter).Decode(&actual))
					require.Equal(t, expected, actual, "full BSON content differs for document %d", i)
				}
			}
			stopCollector()
			logs, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			if tc.pause {
				entry.PauseConfirmed = bytes.Contains(logs, []byte("confirmed after resume"))
				require.True(t, entry.PauseConfirmed)
			}
			if tc.checkpointFailure {
				entry.WriteErrorSeen = bytes.Contains(logs, []byte("upsert error"))
				require.True(t, entry.WriteErrorSeen)
			}
			t.Logf("%s: workers=%d prepared=%d commits=%d docs=%d collector=%.3fs checkpoint=end_fence", tc.name, tc.workers, entry.Prepared, entry.Commits, entry.Documents, entry.Seconds)
		})
	}
	if before != "" && !t.Failed() {
		require.Less(t, results[1].Seconds, results[0].Seconds/3, "fixed small-transaction replay should remove the 300ms per-barrier cost")
	}
}

func timestampValue(ts primitive.Timestamp) int64 { return int64(ts.T)<<32 | int64(ts.I) }

func lastOplogTS(t *testing.T, ctx context.Context, client *mongo.Client) primitive.Timestamp {
	t.Helper()
	var last struct {
		TS primitive.Timestamp `bson:"ts"`
	}
	require.NoError(t, client.Database("local").Collection("oplog.rs").FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&last))
	return last.TS
}

func prepareAndCommit(t *testing.T, ctx context.Context, client *mongo.Client, db *mongo.Database, id int) {
	t.Helper()
	session, err := client.StartSession(options.Session().SetCausalConsistency(false))
	require.NoError(t, err)
	defer session.EndSession(ctx)
	sc := mongo.NewSessionContext(ctx, session)
	txnNumber := time.Now().UnixNano()
	committed := false
	defer func() {
		if !committed {
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_ = client.Database("admin").RunCommand(mongo.NewSessionContext(cleanup, session), bson.D{{Key: "abortTransaction", Value: 1}, {Key: "txnNumber", Value: txnNumber}, {Key: "autocommit", Value: false}}).Err()
		}
	}()
	// Keep driver-managed transaction state inactive and explicitly send the
	// server transaction fields so prepare/commitTimestamp can be exercised.
	require.NoError(t, db.RunCommand(sc, bson.D{
		{Key: "insert", Value: "docs"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: id}, {Key: "value", Value: id}}}},
		{Key: "txnNumber", Value: txnNumber}, {Key: "startTransaction", Value: true}, {Key: "autocommit", Value: false},
	}).Err())
	var prepared struct {
		TS primitive.Timestamp `bson:"prepareTimestamp"`
	}
	require.NoError(t, client.Database("admin").RunCommand(sc, bson.D{{Key: "prepareTransaction", Value: 1}, {Key: "txnNumber", Value: txnNumber}, {Key: "autocommit", Value: false}}).Decode(&prepared))
	require.NotZero(t, prepared.TS.T)
	require.NoError(t, client.Database("admin").RunCommand(sc, bson.D{
		{Key: "commitTransaction", Value: 1}, {Key: "commitTimestamp", Value: prepared.TS},
		{Key: "txnNumber", Value: txnNumber}, {Key: "autocommit", Value: false}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
	}).Err())
	committed = true
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func setPause(t *testing.T, base string, pause bool) {
	t.Helper()
	response, err := http.Post(base+"/sentinel/options", "application/json", strings.NewReader(fmt.Sprintf(`{"Pause":%t}`, pause)))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
}
