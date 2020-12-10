package nimo

import (
	"time"
	"runtime"
	"strconv"
	"bytes"
)

func GoRoutine(function func()) {
	go func() {
		function()
	}()
}

func GoRoutineInLoop(function func()) {
	go func() {
		for {
			function()
		}
	}()
}

func GoRoutineInTimer(duration time.Duration, function func()) {
	go func() {
		for range time.NewTicker(duration).C {
			function()
		}
	}()
}

func GoVarLoop(n uint64, function func()) {
	for n != 0 {
		function()
		n--
	}
}

func GetRoutineId() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}
