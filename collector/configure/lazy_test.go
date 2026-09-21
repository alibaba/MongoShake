package conf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gugemichael/nimo4go"
	"github.com/stretchr/testify/require"
)

func TestLazyOplogConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		enabled    bool
	}{
		{"omitted", "id = test\n", true},
		{"enabled", "incr_sync.lazy_oplog_parse = true\n", true},
		{"disabled", "incr_sync.lazy_oplog_parse = false\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "collector.conf")
			require.NoError(t, os.WriteFile(path, []byte(tc.text), 0600))
			file, err := os.Open(path)
			require.NoError(t, err)
			defer file.Close()
			cfg := DefaultConfiguration()
			require.NoError(t, nimo.NewConfigLoader(file).Load(&cfg))
			require.Equal(t, tc.enabled, cfg.IncrSyncLazyOplogParse)
		})
	}
}
