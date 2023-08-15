package mthrottle

import (
	"context"
	"sync"
	"time"
)

// MThrottle holds the runtime configuration for an mthrottle instance.
type MThrottle struct {
	rate    int
	workers int
	res     time.Duration
	c       <-chan time.Time
}

// New returns a pointer to a new instance of MThrottle.
func New(workers, rate int, res time.Duration) *MThrottle {
	r := MThrottle{
		rate:    rate,
		res:     res,
		workers: workers,
	}

	if rate > 0 {
		r.c = time.NewTicker(qos(rate, res)).C
	}

	return &r
}

// Do performs function f, total times.
func (r *MThrottle) Do(ctx context.Context, total int, f func() error) error {
	var wg sync.WaitGroup
	wg.Add(total)

	// Prepare cross-thread channels.
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		finished <- struct{}{}
	}()

	tasks := make(chan struct{}, r.workers)
	errors := make(chan error)

	// Start workers.
	for w := 0; w < r.workers; w++ {
		w := w
		go r.worker(w, tasks, &wg, f, errors)
	}

	// Schedule work.
	for i := 0; i < total; i++ {
		if r.rate > 0 {
			<-r.c
		}

		tasks <- struct{}{}
	}
	close(tasks)

	for {
		select {
		case <-finished:
			return nil
		case <-ctx.Done():
			return nil
		case err := <-errors:
			return err
		}
	}
}

// DoFor performs function f, for d duration.
func (r *MThrottle) DoFor(ctx context.Context, d time.Duration, f func() error) error {
	var wg sync.WaitGroup

	if d == 0 {
		return nil
	}

	tasks := make(chan struct{}, r.workers)
	end := time.After(d)
	errors := make(chan error)

	// Start workers.
	for w := 0; w < r.workers; w++ {
		w := w
		go r.worker(w, tasks, &wg, f, errors)
	}

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-end:
			wg.Wait()
			return nil
		case err := <-errors:
			return err
		default:
			if r.rate > 0 {
				<-r.c
			}

			wg.Add(1)
			tasks <- struct{}{}
		}
	}
}

func qos(rate int, res time.Duration) time.Duration {
	micros := res.Nanoseconds()
	return time.Duration(micros/int64(rate)) * time.Nanosecond
}

func (r *MThrottle) worker(id int, tasks <-chan struct{}, wg *sync.WaitGroup, f func() error, errors chan<- error) {
	for range tasks {
		go func() {
			defer wg.Done()
			if err := f(); err != nil {
				errors <- err
			}
		}()
	}
}
