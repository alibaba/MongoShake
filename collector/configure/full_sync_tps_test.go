package conf

import (
	nimo "github.com/gugemichael/nimo4go"
	"os"
	"path/filepath"
	"testing"
)

func TestFullSyncTPSConfig(t *testing.T) {
	for _, tc := range []struct {
		input   string
		want    int64
		invalid bool
	}{
		{"", 0, false}, {"full_sync.tps = 0", 0, false},
		{"full_sync.tps = 1000", 1000, false}, {"full_sync.tps = invalid", 0, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			opts := DefaultConfiguration()
			path := filepath.Join(t.TempDir(), "collector.conf")
			if err := os.WriteFile(path, []byte("id = tps-test\n"+tc.input+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			err = nimo.NewConfigLoader(file).Load(&opts)
			if (err != nil) != tc.invalid {
				t.Fatalf("load error: %v", err)
			}
			if !tc.invalid && opts.FullSyncTPS != tc.want {
				t.Fatalf("got %d want %d", opts.FullSyncTPS, tc.want)
			}
		})
	}
}
