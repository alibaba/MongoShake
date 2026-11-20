package utils

import (
	"testing"
	"time"
)

func TestQoSConfiguredStartupAndDynamicRate(t *testing.T) {
	for _, tc := range []struct {
		name               string
		tps, batch, tokens int64
	}{
		{"disabled", 0, 128, 0}, {"below batch", 10, 128, 1},
		{"exact", 256, 128, 2}, {"rounded", 1000, 128, 8}, {"incremental", 10, 1, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rate := tc.tps
			q := newQoS(rate, tc.batch, &rate)
			if q.Limit != tc.tokens || len(q.bucket) != 0 {
				t.Fatalf("startup limit=%d tokens=%d", q.Limit, len(q.bucket))
			}
			if rate == 0 {
				q.FetchBucket()
			}
			q.refill()
			if int64(len(q.bucket)) != tc.tokens {
				t.Fatalf("refill tokens=%d", len(q.bucket))
			}
			for i := int64(0); i < tc.tokens; i++ {
				q.FetchBucket()
			}
			// The same address updated by Sentinel controls subsequent ticks.
			rate = 2 * tc.batch
			q.refill()
			if q.Limit != 2 || len(q.bucket) != 2 {
				t.Fatal("dynamic increase not applied")
			}
			rate = tc.batch
			q.refill()
			if q.Limit != 1 || len(q.bucket) != 1 {
				t.Fatal("dynamic decrease retained old allowance")
			}
			rate = 0
			q.refill()
			if q.Limit != 0 || len(q.bucket) != 0 {
				t.Fatal("dynamic disable not applied")
			}
			q.FetchBucket()
			rate = tc.batch
			q.refill()
			if q.Limit != 1 || len(q.bucket) != 1 {
				t.Fatal("dynamic re-enable not applied")
			}
		})
	}
}

func TestQoSFirstBatchWaitsForAllowance(t *testing.T) {
	rate := int64(1000)
	q := newQoS(rate, 128, &rate)
	done := make(chan struct{})
	go func() { q.FetchBucket(); close(done) }()
	select {
	case <-done:
		t.Fatal("first batch bypassed configured limit")
	case <-time.After(50 * time.Millisecond):
	}
	q.refill()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch did not resume after refill")
	}
}
