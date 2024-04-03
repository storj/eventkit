package main

import (
	"context"
	"log"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"

	"storj.io/eventkit"
	"storj.io/eventkit/bigquery"
)

var ek = eventkit.Package()

func main() {
	c := cobra.Command{
		Use:   "eventkit-save-bq TAG=VALUE [TAG=VALUE ....]",
		Short: "Simple CLI to save eventkit records directly to BigQuery",
		Args:  cobra.MinimumNArgs(1),
	}
	name := c.Flags().StringP("name", "n", "test", "Name of the event sending out")
	project := c.Flags().StringP("project", "p", "", "GCP project to use")
	dataset := c.Flags().StringP("dataset", "d", "eventkitd", "GCP dataset to use")
	credentialsPath := c.Flags().StringP("credentialsPath", "c", "", "GCP credentials path, defaults to GOOGLE_APPLICATION_CREDENTIALS if not provided")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return send(*project, *dataset, *credentialsPath, *name, args)
	}
	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func send(project, dataset, credentialsPath, name string, args []string) error {
	var options []option.ClientOption
	if credentialsPath != "" {
		options = append(options, option.WithCredentialsFile(credentialsPath))
	}

	d, err := bigquery.NewBigQueryDestination(context.Background(), "evenkit-save", project, dataset, options...)
	if err != nil {
		return errors.WithStack(err)
	}
	eventkit.DefaultRegistry.AddDestination(d)
	var tags []eventkit.Tag
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 2 {
			tags = append(tags, eventkit.String(parts[0], parts[1]))
		}
	}
	ek.Event(name, tags...)
	return nil
}
