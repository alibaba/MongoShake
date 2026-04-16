package log

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogDir    = "logs"
	defaultMaxSizeMB = 20
	defaultMaxAge    = 7
	timeLayout       = "2006/01/02 15:04:05 MST"
	bufferSize       = 32 * 1024
)

var Logger *ZapLogger

var logLevelMap = map[string]zapcore.Level{
	"all":     zapcore.DebugLevel,
	"debug":   zapcore.DebugLevel,
	"info":    zapcore.InfoLevel,
	"warn":    zapcore.WarnLevel,
	"warning": zapcore.WarnLevel,
	"error":   zapcore.ErrorLevel,
}

type bufferedWriteSyncer struct {
	mu     sync.Mutex
	buffer *bufio.Writer
	target zapcore.WriteSyncer
}

func newBufferedWriteSyncer(target zapcore.WriteSyncer) zapcore.WriteSyncer {
	return &bufferedWriteSyncer{
		buffer: bufio.NewWriterSize(target, bufferSize),
		target: target,
	}
}

func (b *bufferedWriteSyncer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *bufferedWriteSyncer) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.buffer.Flush(); err != nil {
		return err
	}
	return b.target.Sync()
}

type ZapLogger struct {
	logger *zap.SugaredLogger
}

func New(logLevel, logDir, logFile string,
	logFlush bool, maxSizeMB, maxAge, verbose int) error {
	level := parseLogLevel(logLevel)
	logDir = normalizeLogDir(logDir)
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

	var cores []zapcore.Core
	if verbose == 0 || verbose == 1 {
		if logFile == "" {
			return fmt.Errorf("log.file[%v] shouldn't be empty", logFile)
		}

		if err := os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
			return fmt.Errorf("create log.dir[%v] failed[%v]", logDir, err)
		}

		fileWriter := &lumberjack.Logger{
			Filename: filepath.Join(logDir, logFile),
			MaxSize:  maxSizeMB,
			MaxAge:   maxAge,
			Compress: false,
		}
		fileSyncer := zapcore.AddSync(fileWriter)
		if !logFlush {
			fileSyncer = newBufferedWriteSyncer(fileSyncer)
		}
		cores = append(cores, zapcore.NewCore(encoder, fileSyncer, level))
	}

	if verbose == 1 || verbose == 2 {
		consoleSyncer := zapcore.AddSync(os.Stdout)
		if !logFlush {
			consoleSyncer = newBufferedWriteSyncer(consoleSyncer)
		}
		cores = append(cores, zapcore.NewCore(encoder, consoleSyncer, level))
	}

	if len(cores) == 0 {
		return fmt.Errorf("verbose[%d] is invalid", verbose)
	}

	baseLogger := zap.New(zapcore.NewTee(cores...)).Sugar()
	Logger = &ZapLogger{logger: baseLogger}
	return nil
}

func normalizeLogDir(logDir string) string {
	if logDir == "" {
		return defaultLogDir
	}
	return logDir
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
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
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
	if checked := l.logger.Desugar().Check(level, message); checked != nil {
		checked.Write()
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
	l.logger.Debugf(format, args...)
}

func (l *ZapLogger) Infof(format string, args ...any) {
	l.logger.Infof(format, args...)
}

func (l *ZapLogger) Warnf(format string, args ...any) {
	l.logger.Warnf(format, args...)
}

func (l *ZapLogger) Errorf(format string, args ...any) {
	l.logger.Errorf(format, args...)
}

func (l *ZapLogger) Criticalf(format string, args ...any) {
	l.log(zapcore.DPanicLevel, fmt.Sprintf(format, args...))
}

func (l *ZapLogger) Fatalf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	l.log(zapcore.FatalLevel, message)
	_ = l.Sync()
	os.Exit(1)
}

func (l *ZapLogger) Panicf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	l.log(zapcore.PanicLevel, message)
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
	l.log(zapcore.PanicLevel, message)
	_ = l.Sync()
	panic(args)
}

func (l *ZapLogger) Crashf(format string, args ...any) {
	l.Panicf(format, args...)
}

func (l *ZapLogger) Crash(args ...any) {
	l.Panic(args...)
}

func (l *ZapLogger) Sync() error {
	if l == nil || l.logger == nil {
		return nil
	}
	return l.logger.Sync()
}

func (l *ZapLogger) Close() error {
	return l.Sync()
}
