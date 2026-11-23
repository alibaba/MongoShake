package log

import (
	"sync"
	"testing"
)

// Re-initialising the logger must not race with goroutines that are already
// logging. The tunnel file backend starts a SyncToDisk goroutine that never
// exits, so a later test calling InitialLogger used to race with it.
func TestLoggerReinitWhileReading(t *testing.T) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// Debugf is suppressed at info level but still reads the
				// underlying logger, which is what races.
				Logger.Debugf("concurrent reader")
				Logger.Sync()
			}
		}
	}()

	for i := 0; i < 20; i++ {
		if err := New("info", "", "", true, 0, 0, 2); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("New: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
