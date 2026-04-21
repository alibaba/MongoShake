package log

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

const fatalfTestLogFile = "fatal-buffered.log"

func captureStdout(t *testing.T, run func()) string {
	t.Helper()

	previousStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() returned error: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = previousStdout
	}()

	run()

	_ = writer.Close()
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() returned error: %v", err)
	}
	_ = reader.Close()
	return string(output)
}

func TestNewWritesExpectedTextFormat(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "mongoshake.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
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

func TestNewVerboseFileAndStdoutWriteToBothSinks(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	stdout := captureStdout(t, func() {
		if err := New("info", logDir, "dual.log", true, 20, 7, 1); err != nil {
			t.Fatalf("New() returned error: %v", err)
		}
		Logger.Infof("dual output")
		_ = Logger.Close()
	})

	content, err := os.ReadFile(filepath.Join(logDir, "dual.log"))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}

	if !strings.Contains(string(content), "[INFO] dual output") {
		t.Fatalf("expected file sink output, got %q", string(content))
	}
	if !strings.Contains(stdout, "[INFO] dual output") {
		t.Fatalf("expected stdout sink output, got %q", stdout)
	}
}

func TestPanicfLogsAndPanics(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "panic.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
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

func TestPanicLogsAndPanicsWithStringValue(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "panic.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
	})

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic from Panic")
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

	Logger.Panic("boom", 7)
}

func TestPanicfFlushesBufferedWriterBeforePanicking(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "panic-buffered.log", false, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
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

func TestSyncFlushesBufferedWriteSyncer(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "buffered.log", false, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
	})

	Logger.Infof("buffered sync")

	if err := Logger.Sync(); err != nil {
		t.Fatalf("Sync() returned error: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(logDir, "buffered.log"))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}
	if !strings.Contains(string(content), "[INFO] buffered sync") {
		t.Fatalf("expected info log in buffered log file, got %q", string(content))
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
		_ = Logger.Close()
	})
}

func TestNewRequiresLogFileForFileSink(t *testing.T) {
	t.Helper()

	err := New("info", t.TempDir(), "", true, 20, 7, 0)
	if err == nil {
		t.Fatal("expected file sink initialization without log.file to fail")
	}
	if !strings.Contains(err.Error(), "log.file is empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDebugfIsFilteredAtInfoLevel(t *testing.T) {
	t.Helper()

	logDir := t.TempDir()
	if err := New("info", logDir, "filter.log", true, 20, 7, 0); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = Logger.Close()
	})

	Logger.Debugf("hidden debug")
	Logger.Infof("visible info")
	_ = Logger.Sync()

	content, err := os.ReadFile(filepath.Join(logDir, "filter.log"))
	if err != nil {
		t.Fatalf("ReadFile() returned error: %v", err)
	}
	text := string(content)
	if strings.Contains(text, "hidden debug") {
		t.Fatalf("expected Debugf output to be filtered, got %q", text)
	}
	if !strings.Contains(text, "visible info") {
		t.Fatalf("expected Infof output to be present, got %q", text)
	}
}

func TestFatalfWritesLogAndExits(t *testing.T) {
	t.Helper()

	if os.Getenv("MONGOSHAKE_FATALF_HELPER") == "1" {
		logDir := os.Getenv("MONGOSHAKE_FATALF_LOG_DIR")
		if err := New("info", logDir, fatalfTestLogFile, false, 20, 7, 0); err != nil {
			t.Fatalf("New() returned error: %v", err)
		}
		Logger.Fatalf("fatal %s", "buffered")
		return
	}

	logDir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalfWritesLogAndExits$")
	cmd.Env = append(
		os.Environ(),
		"MONGOSHAKE_FATALF_HELPER=1",
		"MONGOSHAKE_FATALF_LOG_DIR="+logDir,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	err := cmd.Run()
	if err == nil {
		t.Fatal("expected Fatalf helper process to exit with non-zero status")
	}

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T", err)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}

	content, readErr := os.ReadFile(filepath.Join(logDir, fatalfTestLogFile))
	if readErr != nil {
		t.Fatalf("ReadFile() returned error: %v", readErr)
	}
	if !strings.Contains(string(content), "[CRITICAL] fatal buffered") {
		t.Fatalf("expected fatal log in output, got %q", string(content))
	}
}
