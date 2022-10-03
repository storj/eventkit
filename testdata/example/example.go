package main

import (
	"context"
	"time"

	"github.com/jtolio/eventkit"
	"golang.org/x/sync/errgroup"
)

var pkg = eventkit.Package()

type Something struct {
}

func NewSomething() *Something { return &Something{} }

func (s *Something) Interesting(ctx context.Context) (err error) {
	pkg.Event("interesting",
		eventkit.Int64("size", 3),
		eventkit.String("url", "http://google.com"),
	)
	return nil
}

func main() {
	client := eventkit.NewUDPClient("testapp", "v2", "testinst", "localhost:9002")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := errgroup.Group{}
	w.Go(func() error {
		client.Run(ctx)
		return nil
	})

	eventkit.DefaultRegistry.AddDestination(client)

	w.Go(func() error {
		s := NewSomething()
		for i := 0; i < 10; i++ {
			err := s.Interesting(context.Background())
			if err != nil {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
		cancel()
		return nil
	})
	err := w.Wait()
	if err != nil {
		panic(err)
	}
}
