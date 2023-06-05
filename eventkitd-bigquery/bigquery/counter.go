package bigquery

import (
	"fmt"
	"sync"
	"time"
)

type counter struct {
	mu         sync.Mutex
	counter    int
	lastReport time.Time
	message    string
}

func NewCounter(message string) counter {
	return counter{
		lastReport: time.Now(),
		message:    message,
	}
}

func (c *counter) Increment(s int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter += s
	if time.Since(c.lastReport).Milliseconds() > int64(5000) {
		fmt.Printf(c.message, c.counter)
		c.lastReport = time.Now()
	}

}
