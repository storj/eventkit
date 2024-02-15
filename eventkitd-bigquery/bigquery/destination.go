package bigquery

import (
	"context"
	"fmt"
	"os"
	"time"

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

func NewBigQueryDestination(ctx context.Context, appName string, project string, dataset string) (*BigQueryDestination, error) {
	c, err := NewBigQueryClient(ctx, project, dataset)
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
	records := map[string][]*Record{}
	for _, event := range events {
		var tags []*pb.Tag
		for _, t := range event.Tags {
			tags = append(tags, &pb.Tag{
				Key:   t.Key,
				Value: t.Value,
			})
		}
		if _, found := records[event.Name]; !found {
			records[event.Name] = make([]*Record, 0)
		}
		records[event.Name] = append(records[event.Name], &Record{
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

	err := b.client.SaveRecord(context.Background(), records)
	if err != nil {
		fmt.Println("WARN: Couldn't save eventkit record to BQ: ", err)
	}
}

func (b *BigQueryDestination) Run(ctx context.Context) {
}
