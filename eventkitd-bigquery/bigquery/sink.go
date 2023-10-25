package bigquery

import (
	"context"
	"time"

	"github.com/jtolio/eventkit/eventkitd/listener"
	"github.com/jtolio/eventkit/pb"
)

// BigQuerySink provides an abstraction for processing events in a transport agnostic way.
type BigQuerySink struct {
	client *BigQueryClient
}

func NewBigQuerySink(ctx context.Context, project string, dataset string) (*BigQuerySink, error) {
	c, err := NewBigQueryClient(ctx, project, dataset)
	if err != nil {
		return nil, err
	}
	sink := &BigQuerySink{
		client: c,
	}
	return sink, nil
}

// Receive is called when the server receive an event to process.
func (b *BigQuerySink) Receive(ctx context.Context, unparsed *listener.Packet, packet pb.Packet) error {
	records := make(map[string][]*Record)
	correctedStart := unparsed.ReceivedAt.Add(time.Duration(-packet.SendOffsetNs) * time.Nanosecond)

	for _, event := range packet.Events {
		eventTime := correctedStart.Add(time.Duration(event.TimestampOffsetNs) * time.Nanosecond)
		correction := correctedStart.Sub(packet.StartTimestamp.AsTime())

		k := tableName(event)

		records[k] = append(records[k], &Record{
			Application: Application{
				Name:    packet.Application,
				Version: packet.ApplicationVersion,
			},
			Source: Source{
				Instance: packet.Instance,
				Address:  unparsed.Source.IP.String(),
			},
			ReceivedAt: unparsed.ReceivedAt,
			Timestamp:  eventTime,
			Correction: correction,
			Tags:       event.Tags,
		})
	}

	err := b.client.SaveRecord(ctx, records)
	mon.Counter("sent_to_bq").Inc(int64(len(packet.Events)))
	return err
}
