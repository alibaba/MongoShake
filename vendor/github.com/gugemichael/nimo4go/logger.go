package nimo

import (
	"fmt"
	"log"
	"os"
	"path"
)

// LogHelper global logger instance
type LogHelper struct {
	LogFileNames [2]string      // file names include .log and .log.wf
	Logger       [2]*log.Logger // logger handle
	Level        LogLevel
}

// LogLevel logger record level
type LogLevel uint8

const (
	// LogDebug lowest debug
	LogDebug = 2
	// LogTrace trace
	LogTrace = 4
	// LogInfo trace
	LogInfo = 8
	// LogError only error
	LogError = 16
	// LogFatal fatal error
	LogFatal = 32
)

// new log files perm mode
const FLAGS = log.Ldate | log.Ltime | log.Lshortfile | log.Lmicroseconds

// NewLogHelper new logger
func NewLogHelper(v ...interface{}) (*LogHelper, error) {
	mask := os.O_APPEND | os.O_RDWR | os.O_CREATE
	mylog := LogHelper{}

	// use stdout & stderr
	if len(v) == 0 {
		mylog.Logger[0] = log.New(os.Stdout, "TRACE - ", FLAGS)
		mylog.Logger[1] = log.New(os.Stderr, "ERROR - ", FLAGS)
		return &mylog, nil
	}
	fileName := v[0].(string)
	// app.log & app.log.error
	mylog.LogFileNames = [2]string{fileName, fileName + ".error"}
	// check dir wether exist and mkdir
	if _, err := os.Stat(path.Dir(mylog.LogFileNames[0])); err != nil {
		os.Mkdir(path.Dir(mylog.LogFileNames[0]), 0777)
	}

	if logFile, err := os.OpenFile(mylog.LogFileNames[0], mask, 0666); err == nil {
		mylog.Logger[0] = log.New(logFile, "TRACE - ", FLAGS)
		mylog.Logger[0].Flags()
	} else {
		return nil, err
	}

	if logFile, err := os.OpenFile(mylog.LogFileNames[1], mask, 0666); err == nil {
		mylog.Logger[1] = log.New(logFile, "ERROR - ", FLAGS)
	} else {
		return nil, err
	}
	return &mylog, nil
}

// LogLevel hidden low level log record
func (log *LogHelper) LogLevel(level LogLevel) {
	log.Level = level
}

// Trace log
func (log *LogHelper) Trace(v ...interface{}) {
	if log.check(LogTrace) {
		log.Logger[0].Output(2, fmt.Sprintln(v...))
	}
}

// Trace log
func (log *LogHelper) Info(v ...interface{}) {
	if log.check(LogInfo) {
		log.Logger[0].Output(2, fmt.Sprintln(v...))
	}
}

// Error log
func (log *LogHelper) Error(v ...interface{}) {
	if log.check(LogError) {

		// also dump log in normal file that we can
		// find the syntex context, and checkout the
		// around log there
		log.Logger[0].Output(2, fmt.Sprintln(v...))
		log.Logger[1].Output(2, fmt.Sprintln(v...))
	}
}

func (log *LogHelper) check(level LogLevel) bool {
	return level >= log.Level
}
