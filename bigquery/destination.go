// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package bigquery

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/api/option"

	"storj.io/eventkit"
	"storj.io/eventkit/pb"
)

// BigQueryDestination can be used to save each evenkit package directly to server.
type BigQueryDestination struct {
	client         *BigQueryClient
	SourceInstance string
	appName        string
}

var _ eventkit.Destination = &BigQueryDestination{}

func NewBigQueryDestination(ctx context.Context, appName, project, dataset string, options ...option.ClientOption) (*BigQueryDestination, error) {
	c, err := NewBigQueryClient(ctx, project, dataset, options...)
	if err != nil {
		return nil, err
	}

	res := &BigQueryDestination{
		client:  c,
		appName: appName,
	}
	host, err := os.Hostname()
	if err == nil {
		res.SourceInstance = host
	}
	return res, nil
}

// Submit implements Destination.
func (b *BigQueryDestination) Submit(events ...*eventkit.Event) {
	var err error
	defer mon.Task()(nil)(&err)
	records := map[string][]*Record{}
	for _, event := range events {
		var tags []*pb.Tag
		for _, t := range event.Tags {
			tags = append(tags, &pb.Tag{
				Key:   t.Key,
				Value: t.Value,
			})
		}
		tableName := TableName(event.Scope, event.Name)
		if _, found := records[tableName]; !found {
			records[tableName] = make([]*Record, 0)
		}
		records[tableName] = append(records[tableName], &Record{
			Application: Application{
				Name:    b.appName,
				Version: "0.0.1",
			},
			Source: Source{
				Instance: b.SourceInstance,
				Address:  "0.0.0.0",
			},
			ReceivedAt: time.Now(),
			Timestamp:  event.Timestamp,
			Tags:       tags,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	err = b.client.SaveRecord(ctx, records)
	if err != nil {
		fmt.Println("WARN: Couldn't save eventkit record to BQ: ", err)
	}
}

func (b *BigQueryDestination) Run(ctx context.Context) {
	// The BigQueryDestination doesn't need a separate goroutine
}

// Close cleans up resources
func (b *BigQueryDestination) Close() error {
	if b.client != nil {
		return b.client.Close()
	}
	return nil
}
