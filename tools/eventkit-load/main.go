// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"storj.io/eventkit"
	"storj.io/eventkit/bigquery"
)

var mon = monkit.Package()

func main() {
	c := cobra.Command{
		Use:   "eventkit-load",
		Short: "Eventkit load test",
	}
	dest := c.Flags().StringP("destination", "d", "udp:localhost:9002", "complex eventkit destination")
	name := c.Flags().StringP("name", "", "test", "Name of the test (eventkitd tag)")
	workers := c.Flags().IntP("workers", "w", 10, "Number of worker goroutines to use")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return run(*dest, *name, *workers)
	}

	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func run(dest string, testName string, workers int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)

	destination, err := bigquery.CreateDestination(ctx, dest)
	if err != nil {
		return err
	}

	eventkit.DefaultRegistry.AddDestination(destination)

	start := time.Now()

	w := errgroup.Group{}
	w.Go(func() error {
		defer cancel()
		<-interrupted
		var dropped, sent float64
		monkit.Default.Stats(func(key monkit.SeriesKey, field string, val float64) {
			if key.String() == "event,scope=/main" && field == "value" {
				sent = val
			}
			if key.String() == "dropped_events,scope=storj.io/eventkit/eventkitd-bigquery/bigquery" && field == "value" {
				dropped = val
			}
		})

		durationSec := time.Since(start).Seconds()
		fmt.Println("done", durationSec, "seconds")
		fmt.Println("dropped", int(dropped))
		fmt.Println("sent", int(sent))
		fmt.Println("sent/s", int((sent-dropped)/durationSec))
		return nil
	})
	for i := 0; i < workers; i++ {
		ix := int64(i)
		w.Go(func() error {
			scope := eventkit.Package().Subscope(fmt.Sprintf("sub%d", ix))
			counter := int64(0)
			for {
				mon.Counter("event").Inc(1)
				scope.Event("load", eventkit.Int64("counter", counter), eventkit.String("test", testName), eventkit.Int64("goroutine", ix))
				counter++
				if ctx.Err() != nil {
					return nil
				}
			}
		})
	}
	w.Go(func() error {
		destination.Run(ctx)
		return nil
	})
	return w.Wait()
}
