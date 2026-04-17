package log

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestNewWritesExpectedTextFormat(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "mongoshake.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Sync()
	})

	Logger.Infof("hello %d", 1)
	_ = Logger.Sync()

	content, err := os.ReadFile(filepath.Join(logDir, "mongoshake.log"))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}

	text := string(content)
	if !strings.Contains(text, "[INFO] hello 1") {
		t.Fatalf("expected info log in output, got %q", text)
	}
	if strings.Contains(text, "caller") || strings.Contains(text, "msg=") {
		t.Fatalf("unexpected zap console metadata in output: %q", text)
	}
}

func TestPanicfLogsAndPanics(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "panic.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Sync()
	})

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic from Panicf")
		}
		if recovered != "boom 7" {
			t.Fatalf("unexpected panic value: %#v", recovered)
		}

		content, err := os.ReadFile(filepath.Join(logDir, "panic.log"))
		if err != nil {
			t.Fatalf("ReadFile() returned error: %v", err)
		}
		if !strings.Contains(string(content), "[CRITICAL] boom 7") {
			t.Fatalf("expected critical log in output, got %q", string(content))
		}
	}()

	Logger.Panicf("boom %d", 7)
}

func TestPanicfFlushesBufferedWriterBeforePanicking(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "panic-buffered.log", false, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Sync()
	})

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic from Panicf")
		}
		if recovered != "boom buffered" {
			t.Fatalf("unexpected panic value: %#v", recovered)
		}

		content, err := os.ReadFile(filepath.Join(logDir, "panic-buffered.log"))
		if err != nil {
			t.Fatalf("ReadFile() returned error: %v", err)
		}
		if !strings.Contains(string(content), "[CRITICAL] boom buffered") {
			t.Fatalf("expected buffered critical log in output, got %q", string(content))
		}
	}()

	Logger.Panicf("boom %s", "buffered")
}

func TestNewUsesDefaultLogFileForFileSink(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "", true, 20, 7, 0); err != nil {
		t.Fatalf("expected default log file to be used: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Sync()
	})

	Logger.Infof("default file")
	_ = Logger.Sync()

	content, err := os.ReadFile(filepath.Join(logDir, defaultLogFile))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}
	if !strings.Contains(string(content), "[INFO] default file") {
		t.Fatalf("expected info log in default log file, got %q", string(content))
	}
}

func TestDefaultLoggerIsSafeBeforeInitialization(t *testing.T) {
	t.Helper()

	previous := Logger
	Logger = &ZapLogger{logger: zap.NewNop().Sugar()}
	t.Cleanup(func() {
		Logger = previous
	})

	Logger.Debugf("debug %d", 1)
	Logger.Infof("info %d", 2)
	Logger.Warnf("warn %d", 3)
	Logger.Errorf("error %d", 4)
	Logger.Criticalf("critical %d", 5)
}

func TestNewVerboseStdoutOnlyDoesNotRequireLogFile(t *testing.T) {
	t.Helper()

	if err := New("info", "", "", true, 20, 7, 2); err != nil {
		t.Fatalf("expected stdout-only logger initialization to succeed: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Sync()
	})
}
