package bigquery

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"

	"storj.io/eventkit"
)

// Parallel sends messages parallel from multiple goroutines.
type Parallel struct {
	queue    chan []*eventkit.Event
	target   func() (eventkit.Destination, error)
	workers  int
	teardown chan struct{}
}

// NewParallel creates a destination. It requires a way to create the worker destinations and the number of goroutines.
func NewParallel(target func() (eventkit.Destination, error), workers int) *Parallel {
	return &Parallel{
		queue:    make(chan []*eventkit.Event, workers),
		teardown: make(chan struct{}),
		target:   target,
		workers:  workers,
	}
}

// Submit implements eventkit.Destination.
func (p *Parallel) Submit(events ...*eventkit.Event) {
	select {
	case p.queue <- events:
	case <-p.teardown:
	}

}

// Run implements eventkit.Destination.
func (p *Parallel) Run(ctx context.Context) {
	w := errgroup.Group{}
	for i := 0; i < p.workers; i++ {
		dest, err := p.target()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "WARNING: eventkit destination couldn't be created: %v", err)
			continue
		}
		w.Go(func() error {
			dest.Run(ctx)
			return nil
		})
		w.Go(func() error {
			for {
				select {
				case events := <-p.queue:
					dest.Submit(events...)
				case <-ctx.Done():
					return nil
				}
			}
		})
	}
	_ = w.Wait()
	close(p.teardown)
	close(p.queue)

}

var _ eventkit.Destination = &Parallel{}
