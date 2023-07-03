package main

import (
	"context"
	"log"
	"strings"

	"github.com/jtolio/eventkit"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var ek = eventkit.Package()

func main() {
	c := cobra.Command{
		Use:   "eventkit-sender TAG=VALUE [TAG=VALUE ....]",
		Short: "Simple CLI to send out test eventkit events.",
		Args:  cobra.MinimumNArgs(1),
	}
	name := c.Flags().StringP("name", "n", "test", "Name of the event sending out")
	dest := c.Flags().StringP("destination", "d", "localhost:9000", "UDP host and port to send out package")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return send(*dest, *name, args)
	}
	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func send(dest string, name string, args []string) error {
	client := eventkit.NewUDPClient("eventkit-sender", "0.0.1", "i1", dest)
	eventkit.DefaultRegistry.AddDestination(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := errgroup.Group{}
	w.Go(func() error {
		client.Run(ctx)
		return nil
	})
	var tags []eventkit.Tag
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 2 {
			tags = append(tags, eventkit.String(parts[0], parts[1]))
		}
	}
	ek.Event(name, tags...)
	cancel()
	return w.Wait()
}
