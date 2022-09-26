package main

import (
	"context"
	"time"

	"github.com/jtolio/eventkit"
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
	eventkit.DefaultRegistry.AddDestination(eventkit.NewUDPClient("testapp", "testinst", "localhost:9002"))

	s := NewSomething()
	for i := 0; i < 10; i++ {
		err := s.Interesting(context.Background())
		if err != nil {
			panic(err)
		}
	}
	time.Sleep(30 * time.Second)
}
