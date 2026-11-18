//go:build integration

package sharding

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests need a real config server, so they run only under the
// integration tag. MSHAKE_CONFIGSVR_URL overrides the default, which points at
// the local docker cluster described in AGENTS.md (mshake-cfg, port 27020).
func csUrl() string {
	if v := os.Getenv("MSHAKE_CONFIGSVR_URL"); v != "" {
		return v
	}
	return "mongodb://127.0.0.1:27020"
}

// TestGetChunkMapByUrl_Live is the end-to-end guard for #1017.
//
// The local cluster runs MongoDB 7.0, whose config.chunks documents carry uuid
// and no ns. Before the fix, every chunk resolved to the empty namespace and
// GetChunkMapByUrl aborted with "mongo: no documents in result", which
// surfaced to users as:
//
//	[CRIT] fetch chunk map failed[mongo: no documents in result]
//	[CRIT] run replication failed: mongo: no documents in result
//
// Point MSHAKE_CONFIGSVR_URL at a pre-5.0 config server to exercise the ns
// path instead; every assertion below holds for both shapes.
func TestGetChunkMapByUrl_Live(t *testing.T) {
	url := csUrl()

	chunkMap, err := GetChunkMapByUrl(url)
	require.NoError(t, err, "GetChunkMapByUrl(%s) must succeed on a 5.0+ config server", url)
	require.NotNil(t, chunkMap)
	require.NotEmpty(t, chunkMap, "config.shards should yield at least one replset")

	totalChunks := 0
	var nsPerReplset []map[string]bool
	for replset, dbChunkMap := range chunkMap {
		t.Logf("replset=%v namespaces=%v", replset, len(dbChunkMap))
		require.NotEmpty(t, dbChunkMap,
			"replset %v resolved no namespace; chunk uuid -> ns lookup is broken", replset)

		seen := make(map[string]bool, len(dbChunkMap))
		for ns, sc := range dbChunkMap {
			seen[ns] = true
			require.NotEmpty(t, sc.Keys, "namespace %v has no shard key", ns)
			assert.Equal(t, len(sc.Keys), len(sc.ShardTypes),
				"namespace %v: Keys and ShardTypes must be 1:1", ns)
			for _, st := range sc.ShardTypes {
				assert.Contains(t, []string{HashedShard, RangedShard}, st,
					"namespace %v has an unknown shard type", ns)
			}
			for _, c := range sc.Chunks {
				assert.Len(t, c.Mins, len(sc.Keys), "namespace %v: chunk min width", ns)
				assert.Len(t, c.Maxs, len(sc.Keys), "namespace %v: chunk max width", ns)
			}
			totalChunks += len(sc.Chunks)
		}
		nsPerReplset = append(nsPerReplset, seen)
	}

	assert.Greater(t, totalChunks, 0, "no chunk was resolved from config.chunks")

	// OrphanFilter relies on every replset knowing every sharded namespace, so
	// it can tell "sharded collection this shard holds nothing of" apart from
	// "not sharded at all". The namespace sets must therefore be identical.
	for i := 1; i < len(nsPerReplset); i++ {
		assert.Equal(t, nsPerReplset[0], nsPerReplset[i],
			"all replsets must resolve the same namespace set")
	}
}
