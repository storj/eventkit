package destination

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/eventkit"
)

func TestParallel(t *testing.T) {
	m := &mockDestination{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := NewParallel(func() (eventkit.Destination, error) {
		return m, nil
	}, 10)
	go func() {
		queue.Run(ctx)
	}()
	for i := 0; i < 10000; i++ {
		queue.Submit(&eventkit.Event{
			Name: "foobar",
		})
	}
	require.Eventually(t, func() bool {
		return m.Len() == 10000
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, m.events[0], 1)
	require.Len(t, m.events[1], 1)

}
