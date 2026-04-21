package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func TestInitialLoggerWithRotationUsesDefaultLogFile(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := InitialLoggerWithRotation(logDir, "", "info", true, 0, 20, 7); err != nil {
		t.Fatalf("InitialLoggerWithRotation() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Logger.Close()
	})

	l.Logger.Infof("default file from common")
	_ = l.Logger.Sync()

	content, err := os.ReadFile(filepath.Join(logDir, defaultLogFile))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}
	if !strings.Contains(string(content), "[INFO] default file from common") {
		t.Fatalf("expected info log in default file, got %q", string(content))
	}
}
