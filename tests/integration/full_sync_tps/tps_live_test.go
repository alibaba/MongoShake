package full_sync_tps_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestFullSyncTPSLive uses dedicated local source/target servers, never the
// general unit-test instance (whose tests may drop unrelated databases).
func TestFullSyncTPSLive(t *testing.T) {
	srcURI, dstURI, binary := os.Getenv("MONGOSHAKE_FULL_TPS_SOURCE"), os.Getenv("MONGOSHAKE_FULL_TPS_TARGET"), os.Getenv("MONGOSHAKE_TEST_COLLECTOR_BINARY")
	if srcURI == "" || dstURI == "" || binary == "" {
		t.Skip("requires isolated FULL_TPS_SOURCE/TARGET and TEST_COLLECTOR_BINARY")
	}
	for _, uri := range []string{srcURI, dstURI} {
		u, e := url.Parse(uri)
		require.NoError(t, e)
		require.Equal(t, "mongodb", u.Scheme)
		require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, u.Hostname())
	}
	require.NotEqual(t, srcURI, dstURI)
	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelConnect()
	src, e := mongo.Connect(connectCtx, options.Client().ApplyURI(srcURI))
	require.NoError(t, e)
	disconnect := func(client *mongo.Client) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, client.Disconnect(ctx))
	}
	defer disconnect(src)
	dst, e := mongo.Connect(connectCtx, options.Client().ApplyURI(dstURI))
	require.NoError(t, e)
	defer disconnect(dst)
	out := os.Getenv("MONGOSHAKE_FULL_TPS_OUTPUT")
	if out == "" {
		out = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(out, 0700))
	for round := 0; round < 3; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			db := fmt.Sprintf("tps_live_%d", time.Now().UnixNano())
			dir := filepath.Join(out, db)
			require.NoError(t, os.MkdirAll(dir, 0700))
			// Retain failed datasets as evidence.
			defer func() {
				if !t.Failed() {
					cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancelCleanup()
					sourceErr := src.Database(db).Drop(cleanupCtx)
					targetErr := dst.Database(db).Drop(cleanupCtx)
					require.NoError(t, sourceErr)
					require.NoError(t, targetErr)
				}
			}()
			docs := make([]interface{}, 10000)
			for i := range docs {
				docs[i] = bson.D{{Key: "_id", Value: int32(i)}, {Key: "nested", Value: bson.D{{Key: "n", Value: int64(i)}, {Key: "values", Value: bson.A{int32(i), "payload", nil}}}}}
			}
			_, e = src.Database(db).Collection("docs").InsertMany(ctx, docs)
			require.NoError(t, e)
			l, e := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, e)
			port := l.Addr().(*net.TCPAddr).Port
			require.NoError(t, l.Close())
			config := fmt.Sprintf(`conf.version = 14
id = tps_live
sync_mode = full
mongo_urls = %s
mongo_connect_mode = standalone
tunnel = direct
tunnel.address = %s
filter.namespace.white = %s.docs
full_sync.tps = 128
full_sync.reader.collection_parallel = 1
full_sync.reader.write_document_parallel = 2
full_sync.reader.document_batch_size = 128
full_sync.reader.fetch_batch_size = 512
full_sync.create_index = none
full_sync.http_port = %d
incr_sync.http_port = 0
prom.http_port = 0
system_profile_port = 0
log.dir = %s
log.file = collector.log
log.level = info
`, srcURI, dstURI, db, port, dir)
			path := filepath.Join(dir, "collector.conf")
			require.NoError(t, os.WriteFile(path, []byte(config), 0600))
			log, e := os.Create(filepath.Join(dir, "process.log"))
			require.NoError(t, e)
			defer log.Close()
			cmd := exec.CommandContext(ctx, binary, "-conf", path, "-verbose", "2")
			cmd.Stdout = log
			cmd.Stderr = log
			require.NoError(t, cmd.Start())
			finished := false
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			defer func() {
				if !finished {
					_ = cmd.Process.Kill()
					<-done
				}
			}()
			client := &http.Client{Timeout: time.Second}
			base := fmt.Sprintf("http://127.0.0.1:%d", port)
			read := func() (int64, error) {
				resp, e := client.Get(base + "/sentinel")
				if e != nil {
					return 0, e
				}
				defer resp.Body.Close()
				var v struct{ TPS int64 }
				e = json.NewDecoder(resp.Body).Decode(&v)
				return v.TPS, e
			}
			require.Eventually(t, func() bool { _, e := read(); return e == nil }, 10*time.Second, 20*time.Millisecond)
			rate, e := read()
			require.NoError(t, e)
			require.Equal(t, int64(128), rate, "config must be visible without POST")
			count := func() int64 {
				n, e := dst.Database(db).Collection("docs").CountDocuments(ctx, bson.D{})
				require.NoError(t, e)
				return n
			}
			// Include the count RPCs in the observed interval. The interval may
			// cross a tick boundary, and either of the two writers may already
			// hold a batch allowance whose write is not visible in the first count.
			maxDocuments := func(elapsed time.Duration, rate int64) int64 {
				ticks := int64(elapsed/time.Second) + 1
				batchesPerTick := (rate + 127) / 128
				return (ticks*batchesPerTick + 2) * 128
			}
			limitedStart := time.Now()
			before := count()
			time.Sleep(3 * time.Second)
			after := count()
			limitedElapsed := time.Since(limitedStart)
			require.LessOrEqual(t, after-before, maxDocuments(limitedElapsed, 128))
			require.Greater(t, after-before, int64(0))
			require.Less(t, after, int64(10000))
			set := func(rate int64) {
				resp, e := client.Post(base+"/sentinel/options", "application/json", bytes.NewBufferString(fmt.Sprintf(`{"TPS":%d}`, rate)))
				require.NoError(t, e)
				defer resp.Body.Close()
				body, e := io.ReadAll(resp.Body)
				require.NoError(t, e)
				require.Contains(t, string(body), "success")
				got, e := read()
				require.NoError(t, e)
				require.Equal(t, rate, got)
			}
			set(512)
			increasedWindowStart := time.Now()
			increasedStart := count()
			time.Sleep(3 * time.Second)
			increased := count() - increasedStart
			increasedElapsed := time.Since(increasedWindowStart)
			require.GreaterOrEqual(t, increased, int64(768), "HTTP rate increase must affect writes")
			require.LessOrEqual(t, increased, maxDocuments(increasedElapsed, 512))
			set(0)
			select {
			case e := <-done:
				finished = true
				require.NoError(t, e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			a, e := src.Database(db).Collection("docs").Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
			require.NoError(t, e)
			defer a.Close(ctx)
			b, e := dst.Database(db).Collection("docs").Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
			require.NoError(t, e)
			defer b.Close(ctx)
			n := 0
			for a.Next(ctx) {
				require.True(t, b.Next(ctx))
				require.Equal(t, []byte(a.Current), []byte(b.Current), "complete BSON document %d", n)
				n++
			}
			require.NoError(t, a.Err())
			require.False(t, b.Next(ctx))
			require.NoError(t, b.Err())
			require.Equal(t, 10000, n)
			t.Logf("round=%d first_count=%d limited_count=%d limited_elapsed=%s increased=%d increased_elapsed=%s full_raw_BSON_match=%d",
				round, before, after, limitedElapsed, increased, increasedElapsed, n)
		})
	}
}
