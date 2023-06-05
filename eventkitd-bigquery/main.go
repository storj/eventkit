package main

import (
	"context"
	"flag"
	"fmt"
	bq "github.com/jtolio/eventkit/eventkitd-bigquery/bigquery"
	"github.com/jtolio/eventkit/eventkitd/listener"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jtolio/eventkit/pb"
)

type Config struct {
	Address       *string
	PCAPInterface *string
	Workers       *int
	Verbose       *bool
	Filter        *string

	Google struct {
		ProjectID *string

		BigQuery struct {
			Dataset *string
		}
	}
}

func main() {
	cfg := Config{}
	cfg.Address = flag.String("addr", ":9002", "udp address to listen on")
	cfg.PCAPInterface = flag.String("pcap-iface", "", "if set, use pcap for udp packets on this interface. must be on linux")
	cfg.Workers = flag.Int("workers", runtime.NumCPU(), "number of workers")
	cfg.Google.ProjectID = flag.String("google-project-id", os.Getenv("GOOGLE_PROJECT_ID"), "configure which google project is being used (env: GOOGLE_PROJECT_ID)")
	cfg.Google.BigQuery.Dataset = flag.String("google-bigquery-dataset", os.Getenv("GOOGLE_BIGQUERY_DATASET"), "configure which dataset is being used (env: GOOGLE_BIGQUERY_DATASET)")
	cfg.Verbose = flag.Bool("verbose", false, "Print all received message to the console")
	cfg.Filter = flag.String("filter", "", "Application name to filter message. Default: forward all incoming messages.")

	flag.Parse()

	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	cb := bq.NewCounter("%d messages are sent to BigQuery\n")
	sink, err := bq.NewBigQuerySink(ctx, *cfg.Google.ProjectID, *cfg.Google.BigQuery.Dataset, cb.Increment)
	if err != nil {
		panic(err)
	}

	c := bq.NewCounter("%d events are received so far\n")
	listener.ProcessPackages(*cfg.Workers, *cfg.PCAPInterface, *cfg.Address, func(ctx context.Context, unparsed *listener.Packet, packet *pb.Packet) error {
		if *cfg.Filter != "" && *cfg.Filter != packet.Application {
			return nil
		}
		if *cfg.Verbose {
			for _, event := range packet.Events {
				fmt.Printf("%s %s %s %s %s %s\n",
					unparsed.ReceivedAt.Format(time.RFC3339),
					packet.Application,
					packet.Instance,
					event.Scope,
					strings.Join(event.Scope, "."),
					event.TagsString())
			}
		}
		c.Increment(len(packet.Events))
		return sink.Receive(ctx, unparsed, *packet)
	})
}
