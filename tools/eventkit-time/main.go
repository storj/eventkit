package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zeebo/errs/v2"
	"golang.org/x/sync/errgroup"

	"storj.io/eventkit"
	"storj.io/eventkit/bigquery"
)

func main() {
	c := cobra.Command{
		Use:   "eventkit-time [--tag=value] command args",
		Short: "CLI tool for performance tests, like `time` but results are sent to an eventkit target",
	}
	_ = c.Flags().StringP("name", "n", "test", "Name of the event sending out")
	_ = c.Flags().StringP("destination", "d", "localhost:9000", "UDP host and port to send out package, or bq:project/dataset to directly send data to BQ")
	_ = c.Flags().StringSliceP("tag", "t", []string{}, "Custom tags to add to the events")
	_ = c.Flags().StringP("scope", "s", "eventkit-time", "Scope to use for events")
	version := c.Flags().BoolP("version", "v", false, "Scope to use for events")
	viper.SetConfigName("eventkit-time")
	viper.SetEnvPrefix("EVENTKIT")
	viper.AutomaticEnv()
	err := viper.BindPFlags(c.Flags())
	if err != nil {
		panic(err)
	}
	c.RunE = func(cmd *cobra.Command, args []string) error {
		if *version {
			if bi, ok := debug.ReadBuildInfo(); ok {
				for _, s := range bi.Settings {
					if strings.HasPrefix(s.Key, "vcs.") {
						fmt.Printf("%+v\n", s.Key+"="+s.Value)
					}
				}
			}
			return nil
		}
		if len(args) == 0 {
			return errs.Errorf("Command is missing")
		}
		if err := viper.ReadInConfig(); err != nil {
			var configFileNotFoundError viper.ConfigFileNotFoundError
			if !errors.As(err, &configFileNotFoundError) {
				return err
			}
		}

		return execute(
			viper.GetString("destination"),
			viper.GetString("name"),
			args,
			viper.GetStringSlice("tag"),
			viper.GetString("scope"),
		)
	}
	err = c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func execute(dest string, name string, args []string, customTags []string, scope string) error {
	ek := eventkit.DefaultRegistry.Scope(scope)

	destCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	destination, err := bigquery.CreateDestination(destCtx, dest)
	if err != nil {
		return errors.WithStack(err)
	}
	eventkit.DefaultRegistry.AddDestination(destination)

	w := errgroup.Group{}
	w.Go(func() error {
		destination.Run(destCtx)
		return nil
	})

	var cmd = exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	start := time.Now()
	err = cmd.Run()
	if err != nil {
		return err
	}

	t := time.Since(start)

	var tags []eventkit.Tag
	tags = append(tags, eventkit.String("cmd", args[0]))
	tags = append(tags, eventkit.Int64("duration-ms", t.Milliseconds()))

	usage, hasUsage, err := GetChildrenUsage()
	if err != nil {
		return errs.Wrap(err)
	}
	if hasUsage {
		tags = append(tags, eventkit.Int64("user-time-ms", usage.UserTime.Milliseconds()))
		tags = append(tags, eventkit.Int64("system-time-ms", usage.SystemTime.Milliseconds()))
		tags = append(tags, eventkit.Int64("max-rss", usage.MaxRSS))
	}

	for _, c := range customTags {
		parts := strings.SplitN(c, "=", 2)
		tags = append(tags, eventkit.String(parts[0], parts[1]))
	}
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if strings.HasPrefix(parts[0], "EVENTKIT_TAG_") {
			tagName := strings.TrimPrefix(parts[0], "EVENTKIT_TAG_")
			tagName = strings.ToLower(tagName)
			tags = append(tags, eventkit.String(tagName, parts[1]))
		}
	}
	ek.Event(name, tags...)
	cancel()
	return w.Wait()
}
