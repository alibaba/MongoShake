//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

// Sharded-source full+incr sync. The local topology is a real 2-shard cluster,
// so this exercises the docsyncer sharding split and the coordinator's
// chunk-map / source-is-sharding paths that a replica-set source never reaches.
func TestShardedFullAndIncrSync(t *testing.T) {
	mongosURL := envOr("MSHAKE_MONGOS_URL", "mongodb://127.0.0.1:27018")
	shard1URL := envOr("MSHAKE_SHARD1_URL", "mongodb://127.0.0.1:27021")
	shard2URL := envOr("MSHAKE_SHARD2_URL", "mongodb://127.0.0.1:27022")
	cfgURL := envOr("MSHAKE_CONFIGSVR_URL", "mongodb://127.0.0.1:27020")
	tgtURL := envOr("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019")

	mongos := dial(t, mongosURL)
	tgt := dial(t, tgtURL)
	ctx := context.Background()

	// sharded full sync refuses to run while the balancer is on (exit 250)
	require.NoError(t, mongos.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStop", Value: 1}}).Err())
	t.Cleanup(func() {
		_ = mongos.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStart", Value: 1}}).Err()
	})

	dbA, dbB := "shardfull_a", "shardfull_b"
	for _, db := range []string{dbA, dbB} {
		_ = mongos.Database(db).Drop(ctx)
		_ = tgt.Database(db).Drop(ctx)
	}
	_ = dial(t, shard1URL).Database("mongoshake").Collection("ckpt_shardfull_it").Drop(ctx)
	t.Cleanup(func() {
		for _, db := range []string{dbA, dbB} {
			_ = mongos.Database(db).Drop(ctx)
			_ = tgt.Database(db).Drop(ctx)
		}
	})

	// put the two databases on different shards so the full sync must split
	for db, shard := range map[string]string{dbA: "shard1", dbB: "shard2"} {
		require.NoError(t, mongos.Database(db).RunCommand(ctx, bson.D{{Key: "create", Value: "c1"}}).Err())
		res := mongos.Database("admin").RunCommand(ctx, bson.D{{Key: "movePrimary", Value: db}, {Key: "to", Value: shard}})
		require.NoError(t, res.Err(), "movePrimary %s -> %s", db, shard)
	}

	const n = 40
	for _, db := range []string{dbA, dbB} {
		for i := 0; i < n; i++ {
			_, err := mongos.Database(db).Collection("c1").InsertOne(ctx, bson.M{"_id": i, "v": i * 2})
			require.NoError(t, err)
		}
	}

	logDir := t.TempDir()
	confPath := shardedFullConf(t, shard1URL+";"+shard2URL, tgtURL, cfgURL, logDir)
	proc := startCollector(t, confPath, logDir)
	t.Cleanup(func() { proc.kill(); proc.dumpLog(t) })

	// full sync must land every document on both databases
	deadline := time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		ok := true
		for _, db := range []string{dbA, dbB} {
			got, err := tgt.Database(db).Collection("c1").CountDocuments(ctx, bson.M{})
			if err != nil || got != n {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(pollPeriod)
	}
	for _, db := range []string{dbA, dbB} {
		got, err := tgt.Database(db).Collection("c1").CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Equal(t, int64(n), got, "full sync must replicate %s", db)
	}

	// incr tail: a write after full sync must arrive too
	_, err := mongos.Database(dbA).Collection("c1").InsertOne(ctx, bson.M{"_id": n + 1, "v": "incr"})
	require.NoError(t, err)
	deadline = time.Now().Add(syncTO)
	for time.Now().Before(deadline) {
		if c, _ := tgt.Database(dbA).Collection("c1").CountDocuments(ctx, bson.M{"_id": n + 1}); c == 1 {
			break
		}
		time.Sleep(pollPeriod)
	}
	c, err := tgt.Database(dbA).Collection("c1").CountDocuments(ctx, bson.M{"_id": n + 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), c, "incr write after full sync must replicate")
}

func shardedFullConf(t *testing.T, srcURL, tgtURL, cfgURL, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(repoRoot(t), "conf", "collector.conf"))
	require.NoError(t, err)
	conf := string(tpl)
	set := func(key, value string) {
		re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*$`)
		if !re.MatchString(conf) {
			conf += "\n" + key + " = " + value + "\n"
			return
		}
		conf = re.ReplaceAllString(conf, key+" = "+value)
	}
	set("id", "shardfull_it")
	set("sync_mode", "all")
	set("mongo_urls", srcURL)
	set("mongo_cs_url", cfgURL)
	set("incr_sync.worker", "2")
	set("incr_sync.tunnel.write_thread", "2")
	set("checkpoint.storage.url", strings.Split(srcURL, ";")[0])
	set("tunnel.address", tgtURL)
	set("mongo_connect_mode", "standalone")
	set("incr_sync.mongo_fetch_method", "oplog")
	set("checkpoint.storage.collection", "ckpt_shardfull_it")
	set("checkpoint.interval", "1000")
	set("checkpoint.start_position", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	set("full_sync.http_port", "19601")
	set("incr_sync.http_port", "19600")
	set("prom.http_port", "19602")
	set("system_profile_port", "19620")
	set("log.dir", logDir)
	set("log.file", "collector-it.log")
	set("tunnel.kafka.producer.max_message_bytes", "18874368")
	path := filepath.Join(logDir, "collector-it.conf")
	require.NoError(t, os.WriteFile(path, []byte(conf), 0o644))
	return path
}
