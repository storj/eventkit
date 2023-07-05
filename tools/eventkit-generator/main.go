package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/jtolio/eventkit"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func main() {
	rand.Seed(time.Now().Unix())
	c := cobra.Command{
		Use:   "eventkit-generator",
		Short: "Simple CLI to generate test eventkit packages for test purposes.",
	}
	dest := c.Flags().StringP("destination", "d", "localhost:9002", "UDP host and port to send out package")
	freq := c.Flags().DurationP("frequency", "f", 100*time.Millisecond, "Frequency of sending 3 events (3 different scope)")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return run(*dest, *freq)
	}
	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func run(dest string, freq time.Duration) error {

	var ek = eventkit.Package()
	var ek1 = eventkit.Package().Subscope("sub1")
	var ek2 = eventkit.Package().Subscope("sub2")

	client := eventkit.NewUDPClient("eventkit-generator", "0.0.1", "i1", dest)
	eventkit.DefaultRegistry.AddDestination(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := errgroup.Group{}
	w.Go(func() error {
		client.Run(ctx)
		return nil
	})
	for {
		testID := strconv.Itoa(rand.Intn(3))
		keyID := strconv.Itoa(rand.Intn(2))
		ek.Event("test"+testID, eventkit.String("v"+testID+"/k"+keyID, testID+"/"+keyID+"/value"+strconv.Itoa(rand.Intn(8))))
		ek1.Event("test"+testID, eventkit.String("v"+testID+"/k"+keyID, testID+"/"+keyID+"/value"+strconv.Itoa(rand.Intn(8))))
		ek2.Event("test"+testID, eventkit.String("v"+testID+"/k"+keyID, testID+"/"+keyID+"/value"+strconv.Itoa(rand.Intn(8))))
		time.Sleep(freq)
	}
}
