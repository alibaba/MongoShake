package utils

import (
	"time"

	LOG "github.com/vinllen/log4go"
	"math"
)

type Qos struct {
	Limit  int64 // qps, <= 0 means disable limit
	Ticket int64 // one tick size, default is 1

	bucket    chan struct{} // bucket channel
	addr      *int64        // periodically check whether the address value is equal to Limit, and update if not.
	close     bool          // channel is closed?
	prevLimit int64         // previous address limit
}

func StartQoS(limit, ticket int64, addr *int64) *Qos {
	if ticket <= 0 {
		// illegal
		return nil
	}

	q := new(Qos)
	q.Limit = limit
	q.Ticket = ticket
	q.addr = addr
	q.bucket = make(chan struct{}, limit)

	go q.timer()
	return q
}

func (q *Qos) FetchBucket() {
	for q.Limit > 0 { // the old bucket channel maybe release, so we need to retry once timeout
		select {
		case <-q.bucket:
			return
		case <-time.After(time.Second * 1):
			break
		}
	}
}

func (q *Qos) resizeLimit() {
	// we must empty previous channel first to avoid memory leak
FOR:
	for {
		select {
		case <-q.bucket:
		default:
			// break if bucket if empty
			break FOR
		}
	}

	LOG.Info("clear old channel, set new bucket size[%v]", q.Limit)
	q.bucket = make(chan struct{}, q.Limit)
}

func (q *Qos) timer() {
	var i int64
	for range time.NewTicker(1 * time.Second).C {
		if q.close {
			return
		}

		if *q.addr != q.prevLimit {
			LOG.Info("try to resize bucket channel from %v to %v, bucket size[%v], ticket[%v]",
				q.prevLimit, *q.addr, q.Limit, q.Ticket)
			q.prevLimit = *q.addr
			// 0 is ok
			q.Limit = int64(math.Ceil(float64(*q.addr) / float64(q.Ticket)))
			q.resizeLimit()
		}

	INJECT:
		for i = 0; i < q.Limit; i++ {
			select {
			case q.bucket <- struct{}{}:
			default:
				// break if bucket if full
				break INJECT
			}
		}
	}
}

func (q *Qos) Close() {
	q.close = true
}
