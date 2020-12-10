// +build linux darwin windows

package nimo

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

func Profiling(port int) {
	if port == -1 {
		return
	}

	GoRoutine(func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	})
}

func RegisterSignalForPrintStack(sig os.Signal, callback func([]byte)) {
	ch := make(chan os.Signal)
	signal.Notify(ch, sig)

	go func() {
		for range ch {
			buffer := make([]byte, 1024*1024*4)
			runtime.Stack(buffer, true)
			callback(buffer)
		}
	}()
}

func RegisterSignalForProfiling(sig ...os.Signal) {
	ch := make(chan os.Signal)
	started := false
	signal.Notify(ch, sig...)

	go func() {
		var memoryProfile, cpuProfile, traceProfile *os.File
		for range ch {
			if started {
				pprof.StopCPUProfile()
				trace.Stop()
				pprof.WriteHeapProfile(memoryProfile)
				memoryProfile.Close()
				cpuProfile.Close()
				traceProfile.Close()
				started = false
			} else {
				// truncate them if already exist
				cpuProfile, _ = os.Create("cpu.pprof")
				memoryProfile, _ = os.Create("memory.pprof")
				traceProfile, _ = os.Create("runtime.trace")
				pprof.StartCPUProfile(cpuProfile)
				trace.Start(traceProfile)
				started = true
			}
		}
	}()
}
