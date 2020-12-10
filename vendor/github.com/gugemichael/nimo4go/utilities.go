package nimo

import (
	"sync"
	"sync/atomic"
	"time"
)

type RateController interface {
	// take one from controller. if no more in the
	// pool (or tokens). make it block.
	//
	// return true. if need to be blocked
	Control(threshold int64) bool
}

// SimpleRateController. simply increase a inner counter
// and block if the counter grew up to it's threshold and
// yield current routine for a while
type SimpleRateController struct {
	token int64
	tick  int64

	lock *sync.Mutex
}

func NewSimpleRateController() *SimpleRateController {
	return &SimpleRateController{tick: time.Now().Unix(), lock: new(sync.Mutex)}
}

func (controller *SimpleRateController) Control(threshold, n int64) bool {
	now := time.Now().Unix()
	// current second is forward. we are behind
	if now > controller.tick {
		controller.lock.Lock()
		// check it again
		if now > controller.tick {
			controller.tick = now
			controller.token = 0
		}
		controller.lock.Unlock()
		return false
	}
	AssertTrue(now == controller.tick, "rate controller tick now correct !")

	snapshot := atomic.AddInt64(&controller.token, n)
	return snapshot >= threshold
}
