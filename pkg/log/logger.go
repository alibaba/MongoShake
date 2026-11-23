package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultMaxSizeMB                 = 20
	defaultMaxAge                    = 7
	timeLayout                       = "2006/01/02 15:04:05 MST"
	bufferedWriteSyncerSize          = 32 * 1024
	bufferedWriteSyncerFlushInterval = time.Second
)

const criticalLevel zapcore.Level = zapcore.FatalLevel + 1

var Logger = newNopLogger()

func newNopLogger() *ZapLogger {
	l := &ZapLogger{}
	l.logger.Store(zap.NewNop().Sugar())
	return l
}

var logLevelMap = map[string]zapcore.Level{
	"all":     zapcore.DebugLevel,
	"debug":   zapcore.DebugLevel,
	"info":    zapcore.InfoLevel,
	"warn":    zapcore.WarnLevel,
	"warning": zapcore.WarnLevel,
	"error":   zapcore.ErrorLevel,
}

// ZapLogger is installed once in the package-level Logger and never replaced.
// Re-initialisation swaps the inner fields atomically instead, so goroutines
// that are already logging cannot race with a later InitialLogger call.
type ZapLogger struct {
	logger  atomic.Pointer[zap.SugaredLogger]
	closers atomic.Pointer[closeState]
}

type closeState struct {
	stop []func() error
	once sync.Once
}

// sugared returns nil for a nil receiver or an uninitialised logger.
func (l *ZapLogger) sugared() *zap.SugaredLogger {
	if l == nil {
		return nil
	}
	return l.logger.Load()
}

func New(logLevel, logDir, logFile string,
	logFlush bool, maxSizeMB, maxAge, verbose int) error {
	level := parseLogLevel(logLevel)
	maxSizeMB = normalizeMaxSize(maxSizeMB)
	maxAge = normalizeMaxAge(maxAge)

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:          "time",
		LevelKey:         "level",
		NameKey:          zapcore.OmitKey,
		CallerKey:        zapcore.OmitKey,
		FunctionKey:      zapcore.OmitKey,
		MessageKey:       "msg",
		StacktraceKey:    zapcore.OmitKey,
		LineEnding:       zapcore.DefaultLineEnding,
		ConsoleSeparator: " ",
		EncodeTime:       encodeTime,
		EncodeLevel:      encodeLevel,
	}
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	var (
		cores               []zapcore.Core
		stopBufferedWriters []func() error
	)

	if verbose == 0 || verbose == 1 {
		if logFile == "" {
			return fmt.Errorf("log.file is empty")
		}

		fileWriter := &lumberjack.Logger{
			Filename: filepath.Join(logDir, logFile),
			MaxSize:  maxSizeMB,
			MaxAge:   maxAge,
			Compress: false,
		}
		fileSyncer, stop := buildWriteSyncer(zapcore.AddSync(fileWriter), logFlush)
		if stop != nil {
			stopBufferedWriters = append(stopBufferedWriters, stop)
		}
		cores = append(cores, zapcore.NewCore(encoder, fileSyncer, level))
	}

	if verbose == 1 || verbose == 2 {
		consoleSyncer, stop := buildWriteSyncer(zapcore.AddSync(os.Stdout), logFlush)
		if stop != nil {
			stopBufferedWriters = append(stopBufferedWriters, stop)
		}
		cores = append(cores, zapcore.NewCore(encoder, consoleSyncer, level))
	}

	if len(cores) == 0 {
		return fmt.Errorf("verbose[%d] is invalid", verbose)
	}

	baseLogger := zap.New(zapcore.NewTee(cores...)).Sugar()
	Logger.logger.Store(baseLogger)
	Logger.closers.Store(&closeState{stop: stopBufferedWriters})
	return nil
}

func buildWriteSyncer(
	target zapcore.WriteSyncer,
	logFlush bool,
) (zapcore.WriteSyncer, func() error) {
	if logFlush {
		return target, nil
	}

	buffered := &zapcore.BufferedWriteSyncer{
		WS:            target,
		Size:          bufferedWriteSyncerSize,
		FlushInterval: bufferedWriteSyncerFlushInterval,
	}
	return buffered, buffered.Stop
}

func normalizeMaxSize(maxSizeMB int) int {
	if maxSizeMB <= 0 {
		return defaultMaxSizeMB
	}
	return maxSizeMB
}

func normalizeMaxAge(maxAge int) int {
	if maxAge <= 0 {
		return defaultMaxAge
	}
	return maxAge
}

func parseLogLevel(level string) zapcore.Level {
	if parsed, ok := logLevelMap[strings.ToLower(level)]; ok {
		return parsed
	}
	return zapcore.DebugLevel
}

func encodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + t.Format(timeLayout) + "]")
}

func encodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch level {
	case zapcore.DebugLevel:
		enc.AppendString("[DEBUG]")
	case zapcore.InfoLevel:
		enc.AppendString("[INFO]")
	case zapcore.WarnLevel:
		enc.AppendString("[WARNING]")
	case zapcore.ErrorLevel:
		enc.AppendString("[ERROR]")
	case criticalLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		enc.AppendString("[CRITICAL]")
	default:
		enc.AppendString("[" + strings.ToUpper(level.String()) + "]")
	}
}

func formatArgs(args ...any) string {
	if len(args) == 0 {
		return ""
	}
	return fmt.Sprintf(strings.Repeat(" %v", len(args))[1:], args...)
}

func (l *ZapLogger) log(level zapcore.Level, message string) {
	if lg := l.sugared(); lg != nil {
		lg.Desugar().Log(level, message)
	}
}

func (l *ZapLogger) Printf(format string, args ...any) {
	l.Infof(format, args...)
}

func (l *ZapLogger) Print(args ...any) {
	l.Infof("%s", formatArgs(args...))
}

func (l *ZapLogger) Println(args ...any) {
	l.Infof("%s", formatArgs(args...))
}

func (l *ZapLogger) Debugf(format string, args ...any) {
	if lg := l.sugared(); lg != nil {
		lg.Debugf(format, args...)
	}
}

func (l *ZapLogger) Infof(format string, args ...any) {
	if lg := l.sugared(); lg != nil {
		lg.Infof(format, args...)
	}
}

func (l *ZapLogger) Warnf(format string, args ...any) {
	if lg := l.sugared(); lg != nil {
		lg.Warnf(format, args...)
	}
}

func (l *ZapLogger) Errorf(format string, args ...any) {
	if lg := l.sugared(); lg != nil {
		lg.Errorf(format, args...)
	}
}

func (l *ZapLogger) Criticalf(format string, args ...any) {
	l.log(criticalLevel, fmt.Sprintf(format, args...))
}

func (l *ZapLogger) Fatalf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	l.log(criticalLevel, message)
	_ = l.Sync()
	os.Exit(1)
}

func (l *ZapLogger) Panicf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	l.log(criticalLevel, message)
	_ = l.Sync()
	panic(message)
}

func (l *ZapLogger) Debug(format string, args ...any) {
	l.Debugf(format, args...)
}

func (l *ZapLogger) Info(format string, args ...any) {
	l.Infof(format, args...)
}

func (l *ZapLogger) Warn(format string, args ...any) {
	l.Warnf(format, args...)
}

func (l *ZapLogger) Error(format string, args ...any) {
	l.Errorf(format, args...)
}

func (l *ZapLogger) Critical(format string, args ...any) {
	l.Criticalf(format, args...)
}

func (l *ZapLogger) Fatal(args ...any) {
	l.Fatalf("%s", formatArgs(args...))
}

func (l *ZapLogger) Fatalln(args ...any) {
	l.Fatalf("%s", formatArgs(args...))
}

func (l *ZapLogger) Panic(args ...any) {
	message := formatArgs(args...)
	l.log(criticalLevel, message)
	_ = l.Sync()
	panic(message)
}

func (l *ZapLogger) Crashf(format string, args ...any) {
	l.Panicf(format, args...)
}

func (l *ZapLogger) Crash(args ...any) {
	l.Panic(args...)
}

func (l *ZapLogger) Sync() error {
	if lg := l.sugared(); lg != nil {
		return lg.Sync()
	}
	return nil
}

func (l *ZapLogger) Close() error {
	if l == nil {
		return nil
	}

	syncErr := l.Sync()
	if cs := l.closers.Load(); cs != nil {
		cs.once.Do(func() {
			for _, stop := range cs.stop {
				if stopErr := stop(); syncErr == nil && stopErr != nil {
					syncErr = stopErr
				}
			}
		})
	}
	return syncErr
}
