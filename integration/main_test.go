//go:build integration

package integration

import (
	"os"
	"testing"
)

// When the test binary is built with -cover, `go test` overrides GOCOVERDIR with
// a per-build temporary directory, so a GOCOVERDIR exported by the caller never
// reaches the collector children spawned by the launch helpers. Carry the
// intended directory under a different name and install it here, before any
// test starts a collector.
func TestMain(m *testing.M) {
	if dir := os.Getenv("MSHAKE_COLLECTOR_COVERDIR"); dir != "" {
		_ = os.Setenv("GOCOVERDIR", dir)
	}
	os.Exit(m.Run())
}
