package bigquery

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jtolio/eventkit"
	"github.com/jtolio/eventkit/pb"
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

func (b *BigQueryDestination) Submit(event *eventkit.Event) {
	var tags []*pb.Tag
	for _, t := range event.Tags {
		tags = append(tags, &pb.Tag{
			Key:   t.Key,
			Value: t.Value,
		})
	}
	records := map[string][]*Record{
		event.Name: {
			{
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
			},
		},
	}
	err := b.client.SaveRecord(context.Background(), records)
	if err != nil {
		fmt.Println("WARN: Couldn't save eventkit record to BQ: ", err)
	}
}
