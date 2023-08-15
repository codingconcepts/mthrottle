package mthrottle

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDo(t *testing.T) {
	cases := []struct {
		name    string
		workers int
		rps     int
		res     time.Duration
		total   int
		exp     int64
		err     error
	}{
		{name: "no throttle without requests", workers: 1, rps: 0, res: time.Millisecond, total: 0, exp: 0},
		{name: "1/ms throttle without requests", workers: 1, rps: 1, res: time.Millisecond, total: 0, exp: 0},
		{name: "no throttle with 1 request", workers: 1, rps: 0, res: time.Millisecond, total: 1, exp: 1},
		{name: "1/ms throttle with 1 request", workers: 1, rps: 1, res: time.Millisecond, total: 1, exp: 1},
		{name: "10/ms throttle with 1 request", workers: 1, rps: 10, res: time.Millisecond, total: 1, exp: 1},
		{name: "10/ms throttle with 10 requests", workers: 1, rps: 10, res: time.Millisecond, total: 10, exp: 10},
		{name: "10/ms throttle with 10 requests with error", workers: 1, rps: 10, res: time.Millisecond, total: 10, err: fmt.Errorf("oh no!")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := New(c.workers, c.rps, c.res)

			var sum int64
			actErr := r.Do(context.Background(), c.total, func() error {
				atomic.AddInt64(&sum, 1)
				return c.err
			})

			assert.Equal(t, c.err, actErr)
			if c.err != nil {
				return
			}
			assert.Equal(t, c.exp, sum)
		})
	}
}

func TestDoFor(t *testing.T) {
	cases := []struct {
		name    string
		workers int
		rps     int
		res     time.Duration
		d       time.Duration
		expMin  int64
		err     error
	}{
		{name: "no throttle without requests", workers: 1, rps: 0, res: time.Millisecond, d: 0, expMin: 0},
		{name: "1 throttle for 1ms", workers: 1, rps: 1, res: time.Millisecond, d: time.Millisecond, expMin: 1},
		{name: "1 throttle for 2ms", workers: 1, rps: 10, res: time.Millisecond, d: time.Millisecond * 2, expMin: 20},
		{name: "10 throttle with 1ms", workers: 1, rps: 10, res: time.Millisecond, d: time.Millisecond, expMin: 10},
		{name: "10 throttle with 1ms with error", workers: 1, rps: 10, res: time.Millisecond, d: time.Millisecond, expMin: 1, err: fmt.Errorf("oh no!")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := New(c.workers, c.rps, c.res)

			var sum int64
			actErr := r.DoFor(context.Background(), c.d, func() error {
				atomic.AddInt64(&sum, 1)
				return c.err
			})

			assert.GreaterOrEqual(t, sum, c.expMin)
			assert.Equal(t, c.err, actErr)
		})
	}
}

func TestQOS(t *testing.T) {
	cases := []struct {
		name string
		rate int
		res  time.Duration
		exp  time.Duration
	}{
		{name: "1/h", rate: 1, res: time.Hour, exp: time.Hour},
		{name: "1/m", rate: 1, res: time.Minute, exp: time.Minute},
		{name: "1/s", rate: 1, res: time.Second, exp: time.Second},
		{name: "1/ms", rate: 1, res: time.Millisecond, exp: time.Millisecond},
		{name: "1/µs", rate: 1, res: time.Microsecond, exp: time.Microsecond},
		{name: "60/h", rate: 60, res: time.Hour, exp: time.Minute},
		{name: "60/m", rate: 60, res: time.Minute, exp: time.Second},
		{name: "1000/s", rate: 1000, res: time.Second, exp: time.Millisecond},
		{name: "1000/ms", rate: 1000, res: time.Millisecond, exp: time.Microsecond},
		{name: "1000/µs", rate: 1000, res: time.Microsecond, exp: time.Nanosecond},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			act := qos(c.rate, c.res)
			assert.Equal(t, c.exp, act)
		})
	}
}

func TestCancelDo(t *testing.T) {
	r := New(1, 10, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancel()

	r.Do(ctx, 10, func() error {
		time.Sleep(time.Second * 10)
		return nil
	})
}

func TestCancelDoFor(t *testing.T) {
	r := New(1, 10, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancel()

	r.DoFor(ctx, time.Second*10, func() error {
		time.Sleep(time.Second * 10)
		return nil
	})
}

func Example() {
	r := New(1, 10, time.Second)

	var sum int64
	r.Do(context.Background(), 10, func() error {
		atomic.AddInt64(&sum, 1)
		return nil
	})
	log.Println("sum", sum)
}
