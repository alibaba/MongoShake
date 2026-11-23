//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// Regression for the #1052 review P1: with a future checkpoint the first run
// clamps the reader anchor to the newest oplog. If that adjustment lives only
// in the volatile reader query, a restart reloads the future checkpoint,
// clamps to a LATER newest and skips everything in between — including writes
// committed while the collector was stopped. The adjusted anchor must be
// persisted before the clamped stream flows.
//
// Environment: replica-set source (MSHAKE_CS_SRC_URL, default 27030) and target
// (MSHAKE_TGT_URL, default 27019), same as the lazy oplog e2e test.
func TestFutureCheckpointRestartKeepsOfflineDML(t *testing.T) {
	futureCheckpointRestart(t, true)
}

// Control: an ordinary checkpoint must keep recovering downtime writes.
func TestNormalCheckpointRestartKeepsOfflineDML(t *testing.T) {
	futureCheckpointRestart(t, false)
}

func futureCheckpointRestart(t *testing.T, futureCheckpoint bool) {
	ctx := context.Background()
	srcURL := lzEnvOr("MSHAKE_CS_SRC_URL", "mongodb://127.0.0.1:27030")
	tgtURL := lzEnvOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")
	src, tgt := lzDial(t, srcURL), lzDial(t, tgtURL)
	const dbName = "it_future_ckpt_restart"
	const ckptDBName = "it_future_ckpt"
	const ckptCollection = "checkpoint"
	t.Cleanup(func() {
		_ = src.Database(dbName).Drop(ctx)
		_ = tgt.Database(dbName).Drop(ctx)
		_ = src.Database(ckptDBName).Drop(ctx)
		_ = src.Disconnect(ctx)
		_ = tgt.Disconnect(ctx)
	})
	for _, db := range []string{dbName, ckptDBName} {
		if err := src.Database(db).Drop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := tgt.Database(dbName).Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := src.Database(dbName).CreateCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if err := tgt.Database(dbName).CreateCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}

	seed := (time.Now().Unix() + 3600) << 32
	if !futureCheckpoint {
		seed = packed(latestOplogTs(t, src))
	}
	name := overlapReplsetName(t, src)
	if _, err := src.Database(ckptDBName).Collection(ckptCollection).InsertOne(ctx, bson.M{
		"name": name, "ckpt": seed, "version": 2, "fetch_method": "oplog",
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded checkpoint=%d future=%v", seed, futureCheckpoint)

	start := func(leg string) *lzProc {
		t.Helper()
		logDir := filepath.Join(t.TempDir(), leg)
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			t.Fatal(err)
		}
		confPath := overlapWriteConf(t, srcURL, tgtURL, logDir, "incr", ckptCollection, map[string]string{
			"id": "it_future_ckpt_restart", "filter.namespace.white": dbName,
			"filter.ddl_enable": "false", "checkpoint.storage.db": ckptDBName,
			"checkpoint.start_position": time.Unix(seed>>32, 0).UTC().Format("2006-01-02T15:04:05Z"),
			"log.file":                  lazyLogFile, "log.level": "info", "log.flush": "true", "mongo_connect_mode": "primary",
			"full_sync.http_port": "19541", "incr_sync.http_port": "19540",
			"prom.http_port": "19542", "system_profile_port": "19543",
			"full_sync.reader.oplog_store_disk": "false",
		})
		proc := lzStartCollector(t, confPath, logDir)
		t.Cleanup(proc.kill)
		proc.waitReady(t)
		deadline := time.Now().Add(30 * time.Second)
		for !proc.logContains("generates new cursor") {
			proc.requireAlive(t)
			if time.Now().After(deadline) {
				proc.dumpLog(t)
				t.Fatal("reader did not open its cursor")
			}
			time.Sleep(100 * time.Millisecond)
		}
		return proc
	}
	insert := func(id string) {
		t.Helper()
		if _, err := src.Database(dbName).Collection("c").InsertOne(ctx, bson.M{"_id": id, "v": 1}); err != nil {
			t.Fatal(err)
		}
	}

	first := start("first")
	insert("before_shutdown")
	waitDocIn(t, tgt, dbName, "c", "before_shutdown", 30*time.Second)
	var persisted bson.M
	if err := src.Database(ckptDBName).Collection(ckptCollection).FindOne(ctx, bson.M{"name": name}).Decode(&persisted); err != nil {
		t.Fatal(err)
	}
	if futureCheckpoint {
		// The adjusted anchor must be durable before the stream flows; the
		// future seed value must no longer be the persisted checkpoint.
		ckpt, ok := persisted["ckpt"].(int64)
		if !ok || ckpt >= seed {
			t.Fatalf("future checkpoint was not adjusted durably before flow: %v (seed=%d)", persisted["ckpt"], seed)
		}
	}
	t.Logf("first run delivered before_shutdown; persisted checkpoint=%v", persisted["ckpt"])
	first.kill()
	insert("during_shutdown")
	t.Logf("DML committed while collector stopped: during_shutdown ts=%d", packed(latestOplogTs(t, src)))

	restarted := start("restart")
	insert("after_restart")
	waitDocIn(t, tgt, dbName, "c", "after_restart", 30*time.Second)
	waitDocIn(t, tgt, dbName, "c", "during_shutdown", 30*time.Second)
	restarted.requireAlive(t)
	for _, id := range []string{"before_shutdown", "during_shutdown", "after_restart"} {
		n, err := tgt.Database(dbName).Collection("c").CountDocuments(ctx, bson.M{"_id": id})
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("document %s missing or duplicated after restart: count=%d, want 1", id, n)
		}
	}
}
