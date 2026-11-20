package utils

import (
	"time"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
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
	q := newQoS(limit, ticket, addr)
	if q != nil {
		go q.timer()
	}
	return q
}

// newQoS initializes the limiter before any writer can use it. limit is in
// documents/operations per second; each bucket token admits one ticket-sized batch.
// Start empty so the first batch also waits for a rate-limited tick.
func newQoS(limit, ticket int64, addr *int64) *Qos {
	if ticket <= 0 {
		// illegal
		return nil
	}

	q := new(Qos)
	q.Limit = qosBatchLimit(limit, ticket)
	q.prevLimit = limit
	q.Ticket = ticket
	q.addr = addr
	q.bucket = make(chan struct{}, q.Limit)

	return q
}

func qosBatchLimit(limit, ticket int64) int64 {
	if limit <= 0 {
		return 0
	}
	// Integer ceiling avoids both floating-point rounding and addition overflow.
	return 1 + (limit-1)/ticket
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

	l.Logger.Infof("clear old channel, set new bucket size[%v]", q.Limit)
	q.bucket = make(chan struct{}, q.Limit)
}

func (q *Qos) timer() {
	for range time.NewTicker(1 * time.Second).C {
		if q.close {
			return
		}
		q.refill()
	}
}

// refill is also used by tests to exercise rate changes without wall-clock sleeps.
func (q *Qos) refill() {
	if *q.addr != q.prevLimit {
		l.Logger.Infof("try to resize bucket channel from %v to %v, bucket size[%v], ticket[%v]",
			q.prevLimit, *q.addr, q.Limit, q.Ticket)
		q.prevLimit = *q.addr
		// 0 is ok
		q.Limit = qosBatchLimit(*q.addr, q.Ticket)
		q.resizeLimit()
	}

INJECT:
	for i := int64(0); i < q.Limit; i++ {
		select {
		case q.bucket <- struct{}{}:
		default:
			// break if bucket if full
			break INJECT
		}
	}
}

func (q *Qos) Close() {
	q.close = true
}
