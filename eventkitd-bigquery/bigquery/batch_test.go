// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package bigquery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/eventkit"
)

func TestBatchQueue(t *testing.T) {
	m := &mockDestination{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := NewBatchQueue(m, 1000, 10, 1*time.Hour)
	go func() {
		queue.Run(ctx)
	}()
	for i := 0; i < 25; i++ {
		queue.Submit(&eventkit.Event{
			Name: "foobar",
		})
	}
	require.Eventually(t, func() bool {
		return m.Len() == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, m.events[0], 10)
	require.Len(t, m.events[1], 10)
}

type mockDestination struct {
	mu     sync.Mutex
	events [][]*eventkit.Event
}

func (m *mockDestination) Submit(event ...*eventkit.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *mockDestination) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *mockDestination) Run(ctx context.Context) {

}

var _ eventkit.Destination = &mockDestination{}
