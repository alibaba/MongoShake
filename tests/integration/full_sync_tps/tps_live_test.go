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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	src, e := mongo.Connect(ctx, options.Client().ApplyURI(srcURI))
	require.NoError(t, e)
	defer src.Disconnect(context.Background())
	dst, e := mongo.Connect(ctx, options.Client().ApplyURI(dstURI))
	require.NoError(t, e)
	defer dst.Disconnect(context.Background())
	out := os.Getenv("MONGOSHAKE_FULL_TPS_OUTPUT")
	if out == "" {
		out = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(out, 0700))
	for round := 0; round < 3; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			db := fmt.Sprintf("tps_live_%d", time.Now().UnixNano())
			dir := filepath.Join(out, db)
			require.NoError(t, os.MkdirAll(dir, 0700))
			// Retain failed datasets as evidence.
			defer func() {
				if !t.Failed() {
					require.NoError(t, src.Database(db).Drop(ctx))
					require.NoError(t, dst.Database(db).Drop(ctx))
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
			before := count()
			time.Sleep(3 * time.Second)
			after := count()
			// Include one timer-boundary batch; unthrottled 10k import cannot pass.
			require.LessOrEqual(t, after-before, int64(512))
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
			increasedStart := count()
			time.Sleep(3 * time.Second)
			increased := count() - increasedStart
			require.GreaterOrEqual(t, increased, int64(768), "HTTP rate increase must affect writes")
			require.LessOrEqual(t, increased, int64(2048))
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
			t.Logf("round=%d first_count=%d limited_count=%d full_raw_BSON_match=%d", round, before, after, n)
		})
	}
}
