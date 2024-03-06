package destination

import (
	"context"
	"sync"
	"time"

	"github.com/spacemonkeygo/monkit/v3"
	"golang.org/x/sync/errgroup"

	"storj.io/eventkit"
	"storj.io/eventkit/utils"
)

var mon = monkit.Package()

// BatchQueue collects events and send them in batches.
type BatchQueue struct {
	batchThreshold int
	flushInterval  time.Duration
	submitQueue    chan *eventkit.Event
	target         eventkit.Destination
	mu             sync.Mutex
	events         []*eventkit.Event
}

var _ eventkit.Destination = &BatchQueue{}

// NewBatchQueue creates a new batchQueue. It sends out the received events in batch. Either after the flushInterval is
// expired or when there are more than batchSize element in the queue.
func NewBatchQueue(target eventkit.Destination, queueSize int, batchSize int, flushInterval time.Duration) *BatchQueue {
	c := &BatchQueue{
		submitQueue:    make(chan *eventkit.Event, queueSize),
		batchThreshold: batchSize,
		events:         make([]*eventkit.Event, 0),
		flushInterval:  flushInterval,
		target:         target,
	}
	return c
}

// Run implements Destination.
func (c *BatchQueue) Run(ctx context.Context) {
	ticker := utils.NewJitteredTicker(c.flushInterval)
	var background errgroup.Group
	defer func() { _ = background.Wait() }()
	background.Go(func() error {
		c.target.Run(ctx)
		return nil
	})
	background.Go(func() error {
		ticker.Run(ctx)
		return nil
	})

	sendAndReset := func() {
		c.mu.Lock()
		eventsToSend := c.events
		c.events = make([]*eventkit.Event, 0)
		c.mu.Unlock()

		c.target.Submit(eventsToSend...)
	}

	for {

		select {
		case em := <-c.submitQueue:
			if c.addEvent(em) {
				sendAndReset()
			}
		case <-ticker.C:
			if len(c.events) > 0 {
				sendAndReset()
			}
		case <-ctx.Done():
			left := len(c.submitQueue)
			for i := 0; i < left; i++ {
				if c.addEvent(<-c.submitQueue) {
					sendAndReset()
				}
			}
			if len(c.events) > 0 {
				c.target.Submit(c.events...)
			}
			return
		}
	}
}

func (c *BatchQueue) addEvent(ev *eventkit.Event) (full bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
	return len(c.events) >= c.batchThreshold
}

// Submit implements Destination.
func (c *BatchQueue) Submit(events ...*eventkit.Event) {
	defer mon.Task()(nil)(nil)
	for _, e := range events {
		select {
		case c.submitQueue <- e:
		default:
			mon.Counter("dropped_events").Inc(1)
		}
	}
}
