package log

import (
	"sync"
	"testing"
)

// Re-initialising the logger must not race with goroutines that are already
// logging or closing it. The tunnel file backend starts a SyncToDisk goroutine
// that never exits, so a later test calling InitialLogger used to race with it.
//
// logFlush=false is the interesting case: it installs BufferedWriteSyncers, so
// Close must stop the closers belonging to the state it observed, not a torn
// mix of old and new.
func TestLoggerReinitWhileReading(t *testing.T) {
	previous := Logger.state.Load()
	t.Cleanup(func() { Logger.state.Store(previous) })

	for _, flush := range []bool{true, false} {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
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
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = Logger.Close()
				}
			}
		}()

		for i := 0; i < 20; i++ {
			if err := New("info", "", "", flush, 0, 0, 2); err != nil {
				close(stop)
				wg.Wait()
				t.Fatalf("New(flush=%v): %v", flush, err)
			}
		}
		close(stop)
		wg.Wait()
	}
}

// Generation must advance on every successful New so tests can detect
// re-initialisation even though the Logger pointer never changes.
func TestGenerationAdvancesOnReinit(t *testing.T) {
	previous := Logger.state.Load()
	t.Cleanup(func() { Logger.state.Store(previous) })

	before := Generation()
	for i := 0; i < 3; i++ {
		if err := New("info", "", "", true, 0, 0, 2); err != nil {
			t.Fatalf("New: %v", err)
		}
	}
	if got := Generation(); got != before+3 {
		t.Fatalf("Generation = %d, want %d", got, before+3)
	}
}
