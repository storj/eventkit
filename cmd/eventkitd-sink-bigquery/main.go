package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jtolio/eventkit/pb"
	"github.com/jtolio/eventkit/transport"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Application struct {
	Name    string
	Version string
}

type Source struct {
	Instance string
	Address  *net.UDPAddr
}

type Record struct {
	Application Application

	Source Source

	ReceivedAt time.Time
	Timestamp  time.Time
	Correction time.Duration

	Tags []*pb.Tag
}

func (r *Record) Save() (map[string]bigquery.Value, string, error) {
	fields := make(map[string]bigquery.Value)
	fields["application_name"] = r.Application.Name
	fields["application_version"] = r.Application.Version

	fields["source_instance"] = r.Source.Instance
	fields["source_ip"] = r.Source.Address.IP.String()

	fields["received_at"] = r.ReceivedAt
	fields["timestamp"] = r.Timestamp
	fields["correction"] = r.Correction

	for _, tag := range r.Tags {
		field := "tag_" + tag.Key

		switch v := tag.Value.(type) {
		case *pb.Tag_Bool:
			fields[field] = v.Bool
		case *pb.Tag_Bytes:
			fields[field] = v.Bytes
		case *pb.Tag_Double:
			fields[field] = v.Double
		case *pb.Tag_Duration:
			fields[field] = v.Duration
		case *pb.Tag_Int64:
			fields[field] = v.Int64
		case *pb.Tag_String_:
			fields[field] = v.String_
		}
	}

	return fields, "", nil
}

// BigQuerySink provides an abstraction for processing events in a transport agnostic way.
type BigQuerySink struct {
	dataset *bigquery.Dataset
}

// Receive is called when the server receive an event to process.
func (b *BigQuerySink) Receive(ctx context.Context, packet *Packet) error {
	records := make(map[string][]*Record)
	correctedStart := packet.ReceivedAt.Add(-packet.Packet.SendOffset.AsDuration())

	for _, event := range packet.Packet.Events {
		eventTime := correctedStart.Add(event.TimestampOffset.AsDuration())
		correction := correctedStart.Sub(packet.Packet.StartTimestamp.AsTime())

		k := path.Join(event.Scope...) + "." + event.Name

		records[k] = append(records[k], &Record{
			Application: Application{
				Name:    packet.Packet.Application,
				Version: packet.Packet.ApplicationVersion,
			},
			Source: Source{
				Instance: packet.Packet.Instance,
				Address:  packet.Source,
			},
			ReceivedAt: packet.ReceivedAt,
			Timestamp:  eventTime,
			Correction: correction,
			Tags:       event.Tags,
		})
	}

	for table, events := range records {
		err := b.dataset.Table(table).Inserter().Put(ctx, events)
		if err != nil {
			return err
		}
	}

	return nil
}

type Config struct {
	Address *string
	Workers *int

	Google struct {
		ProjectID *string

		BigQuery struct {
			Dataset *string
		}
	}
}

type Packet struct {
	Packet     *pb.Packet
	Source     *net.UDPAddr
	ReceivedAt time.Time
}

func main() {
	log, _ := zap.NewProduction()

	cfg := Config{}
	cfg.Address = flag.String("bind-address", ":9002", "udp address to listen on")
	cfg.Workers = flag.Int("workers", runtime.NumCPU(), "number of workers")
	cfg.Google.ProjectID = flag.String("google-project-id", os.Getenv("GOOGLE_PROJECT_ID"), "configure which google project is being used (env: GOOGLE_PROJECT_ID)")
	cfg.Google.BigQuery.Dataset = flag.String("google-bigquery-dataset", os.Getenv("GOOGLE_BIGQUERY_DATASET"), "configure which dataset is being used (env: GOOGLE_BIGQUERY_DATASET)")
	flag.Parse()

	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	client, err := bigquery.NewClient(ctx, *cfg.Google.ProjectID)
	if err != nil {
		log.Error("bigquery client connection failed", zap.Error(err))
		os.Exit(1)
		return
	}

	log.Info("starting server", zap.Stringp("address", cfg.Address))
	listener, err := transport.ListenUDP(*cfg.Address)
	if err != nil {
		log.Error("failed to bind to udp address")
		os.Exit(1)
		return
	}

	sink := &BigQuerySink{dataset: client.Dataset(*cfg.Google.BigQuery.Dataset)}

	group, ctx := errgroup.WithContext(ctx)
	workQueue := make(chan *Packet, *cfg.Workers*2)

	for i := 0; i < *cfg.Workers; i++ {
		group.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case packet := <-workQueue:
					err = sink.Receive(ctx, packet)
					if err != nil {
						log.Error("failed to process packet", zap.Error(err))
					}
				}
			}
		})
	}

	noWait := &errgroup.Group{}
	noWait.Go(func() error {
		for {
			packet, source, err := listener.Next()
			if err != nil {
				log.Error("failed to read udp packet", zap.Error(err))
				continue
			}

			workQueue <- &Packet{
				Packet:     packet,
				Source:     source,
				ReceivedAt: time.Now(),
			}
		}
	})

	<-ctx.Done()
	log.Info("shutting down")

	err = group.Wait()
	if err != nil {
		log.Error("encountered error during shutdown", zap.Error(err))
		os.Exit(1)
		return
	}
}
