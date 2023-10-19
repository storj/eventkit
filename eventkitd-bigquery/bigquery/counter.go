package bigquery

import (
	"fmt"
	"sync"
	"time"
)

type Counter struct {
	mu         sync.Mutex
	counter    int
	lastReport time.Time
	message    string
}

func NewCounter(message string) Counter {
	return Counter{
		lastReport: time.Now(),
		message:    message,
	}
}

func (c *Counter) Increment(s int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter += s
	if time.Since(c.lastReport).Milliseconds() > int64(5000) {
		fmt.Printf(c.message, c.counter)
		c.lastReport = time.Now()
	}

}
