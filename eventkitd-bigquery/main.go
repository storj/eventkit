package main

import (
	"context"
	"flag"
	"github.com/jtolio/eventkit/eventkitd/private/listener"
	"google.golang.org/api/googleapi"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jtolio/eventkit/pb"
	"go.uber.org/zap"
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
		field := tagFieldName(tag.Key)

		switch v := tag.Value.(type) {
		case *pb.Tag_Bool:
			fields[field] = v.Bool
		case *pb.Tag_Bytes:
			fields[field] = v.Bytes
		case *pb.Tag_Double:
			fields[field] = v.Double
		case *pb.Tag_DurationNs:
			fields[field] = v.DurationNs
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
	dataset          *bigquery.Dataset
	tables           map[string]bigquery.TableMetadata
	schemeChangeLock sync.Locker
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
				Address:  unparsed.Source,
			},
			ReceivedAt: unparsed.ReceivedAt,
			Timestamp:  eventTime,
			Correction: correction,
			Tags:       event.Tags,
		})
	}

	var err error
	for table, events := range records {
		tableMetadata, found := b.tables[table]
		if !found {
			tableMetadata, err = b.createOrLoadTableScheme(ctx, table)
		}
		if err != nil {
			return err
		}

		if len(events) > 0 {
			if isTagMissing(tableMetadata.Schema, events[0].Tags) {
				err = b.UpdateFields(ctx, tableMetadata, events[0].Tags)
				if err != nil {
					return err
				}
			}
		}

		err := b.dataset.Table(table).Inserter().Put(ctx, events)
		if err != nil {
			return err
		}
	}

	return nil
}

var nonSafeTableNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9]+`)
var multiUnderscore = regexp.MustCompile(`_{2,}`)

func tableName(event *pb.Event) string {
	var res []string
	for _, scope := range event.Scope {
		res = append(res, nonSafeTableNameCharacters.ReplaceAllString(scope, "_"))
	}
	res = append(res, nonSafeTableNameCharacters.ReplaceAllString(event.Name, "_"))

	name := strings.Join(res, "_")
	all := multiUnderscore.ReplaceAllString(name, "_")
	all = strings.Trim(all, "_")
	return all
}

func tagFieldName(key string) string {
	field := "tag_" + key
	field = strings.ReplaceAll(field, "/", "_")
	field = strings.ReplaceAll(field, "-", "_")
	return field
}

func isTagMissing(schema bigquery.Schema, tags []*pb.Tag) bool {
tagloop:
	for _, tag := range tags {
		for _, field := range schema {
			if field.Name == tagFieldName(tag.Key) {
				continue tagloop
			}
		}
		return true
	}
	return false
}

func (b *BigQuerySink) createOrLoadTableScheme(ctx context.Context, table string) (bigquery.TableMetadata, error) {
	b.schemeChangeLock.Lock()
	defer b.schemeChangeLock.Unlock()
	tableMetadata, found := b.tables[table]
	if found {
		return tableMetadata, nil
	}
	meta, err := b.dataset.Table(table).Metadata(ctx)
	switch e := err.(type) {
	case *googleapi.Error:
		if e.Code == 404 {
			err = b.dataset.Table(table).Create(ctx, &bigquery.TableMetadata{
				Name: table,
				Schema: bigquery.Schema{
					{
						Name: "application_name",
						Type: bigquery.StringFieldType,
					},
					{
						Name: "application_version",
						Type: bigquery.StringFieldType,
					},
					{
						Name: "source_instance",
						Type: bigquery.StringFieldType,
					},
					{
						Name: "source_ip",
						Type: bigquery.StringFieldType,
					},
					{
						Name: "received_at",
						Type: bigquery.TimestampFieldType,
					},
					{
						Name: "timestamp",
						Type: bigquery.TimestampFieldType,
					},
					{
						Name: "correction",
						Type: bigquery.IntegerFieldType,
					},
				},
			})
			if err != nil {
				return tableMetadata, err
			}
			meta, err = b.dataset.Table(table).Metadata(ctx)
			if err != nil {
				return tableMetadata, err
			}

		}
	}
	b.tables[table] = *meta
	return *meta, err
}

func (b *BigQuerySink) UpdateFields(ctx context.Context, metadata bigquery.TableMetadata, tags []*pb.Tag) error {
	b.schemeChangeLock.Lock()
	defer b.schemeChangeLock.Unlock()
	schema := metadata.Schema
tagloop:
	for _, tag := range tags {
		for _, field := range metadata.Schema {
			if field.Name == tagFieldName(tag.Key) {
				continue tagloop
			}
		}

		// missing field
		f := &bigquery.FieldSchema{
			Name: tagFieldName(tag.Key),
		}
		switch tag.Value.(type) {
		case *pb.Tag_Bool:
			f.Type = bigquery.BooleanFieldType
		case *pb.Tag_Bytes:
			f.Type = bigquery.BytesFieldType
		case *pb.Tag_Double:
			f.Type = bigquery.FloatFieldType
		case *pb.Tag_DurationNs:
			f.Type = bigquery.IntegerFieldType
		case *pb.Tag_Int64:
			f.Type = bigquery.IntegerFieldType
		case *pb.Tag_String_:
			f.Type = bigquery.StringFieldType
		}
		schema = append(schema, f)
	}
	md, err := b.dataset.Table(metadata.Name).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: schema,
	}, metadata.ETag)
	if err != nil {
		return err
	}
	b.tables[metadata.Name] = *md
	return nil
}

var _ bigquery.ValueSaver = &Record{}

type Config struct {
	Address       *string
	PCAPInterface *string
	Workers       *int

	Google struct {
		ProjectID *string

		BigQuery struct {
			Dataset *string
		}
	}
}

func main() {
	log, _ := zap.NewProduction()

	cfg := Config{}
	cfg.Address = flag.String("addr", ":9002", "udp address to listen on")
	cfg.PCAPInterface = flag.String("pcap-iface", "", "if set, use pcap for udp packets on this interface. must be on linux")
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

	sink := &BigQuerySink{
		dataset:          client.Dataset(*cfg.Google.BigQuery.Dataset),
		tables:           map[string]bigquery.TableMetadata{},
		schemeChangeLock: &sync.Mutex{},
	}

	listener.ProcessPackages(*cfg.Workers, *cfg.PCAPInterface, *cfg.Address, func(ctx context.Context, unparsed *listener.Packet, packet *pb.Packet) error {
		return sink.Receive(ctx, unparsed, *packet)
	})
}
