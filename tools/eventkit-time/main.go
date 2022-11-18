package main

import (
	"context"
	"github.com/jtolio/eventkit"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

var ek = eventkit.Package()

func main() {
	c := cobra.Command{
		Use:   "eventkit-time [--tag=value] command args",
		Short: "CLI tool for performance tests, like `time` but results are sent to an eventkit target",
		Args:  cobra.MinimumNArgs(1),
	}
	name := c.Flags().StringP("name", "n", "test", "Name of the event sending out")
	dest := c.Flags().StringP("destination", "d", "localhost:9000", "UDP host and port to send out package")
	tags := c.Flags().StringSliceP("tag", "t", []string{}, "Custom tags to add to the events")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return send(*dest, *name, args, *tags)
	}
	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func send(dest string, name string, args []string, customTags []string) error {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = funcName()
	}
	client := eventkit.NewUDPClient("eventkit-time", "0.0.1", hostname, dest)
	eventkit.DefaultRegistry.AddDestination(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := errgroup.Group{}
	w.Go(func() error {
		client.Run(ctx)
		return nil
	})

	var cmd = exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	start := time.Now()
	err := cmd.Run()
	if err != nil {
		return err
	}
	t := time.Since(start)

	var tags []eventkit.Tag
	tags = append(tags, eventkit.String("cmd", args[0]))
	tags = append(tags, eventkit.Int64("duration-ms", t.Milliseconds()))
	for _, c := range customTags {
		parts := strings.Split(c, "=")
		tags = append(tags, eventkit.String(parts[0], parts[1]))
	}
	ek.Event(name, tags...)
	cancel()
	return w.Wait()
}

func funcName() string {
	return "unknown"
}
