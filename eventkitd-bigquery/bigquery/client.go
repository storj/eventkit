package bigquery

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jtolio/eventkit/pb"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

var nonSafeTableNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9]+`)
var multiUnderscore = regexp.MustCompile(`_{2,}`)

type BigQueryClient struct {
	dataset          *bigquery.Dataset
	tables           map[string]bigquery.TableMetadata
	schemeChangeLock sync.Locker
}

func NewBigQueryClient(ctx context.Context, project string, datasetName string) (*BigQueryClient, error) {
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &BigQueryClient{
		schemeChangeLock: &sync.Mutex{},
		tables:           make(map[string]bigquery.TableMetadata),
		dataset:          client.Dataset(datasetName),
	}, nil
}

func (b *BigQueryClient) createOrLoadTableScheme(ctx context.Context, table string) (bigquery.TableMetadata, error) {
	b.schemeChangeLock.Lock()
	defer b.schemeChangeLock.Unlock()
	tableMetadata, found := b.tables[table]
	if found {
		return tableMetadata, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	meta, err := b.dataset.Table(table).Metadata(ctx)
	switch e := err.(type) {
	case *googleapi.Error:
		if e.Code == 404 {
			err = b.dataset.Table(table).Create(ctx, &bigquery.TableMetadata{
				Name:                   table,
				RequirePartitionFilter: true,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:  bigquery.DayPartitioningType,
					Field: "received_at",
				},
				Clustering: &bigquery.Clustering{
					Fields: []string{
						"application_name",
						"source_instance",
					},
				},
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
				return tableMetadata, errors.WithStack(err)
			}
			meta, err = b.dataset.Table(table).Metadata(ctx)
			if err != nil {
				return tableMetadata, errors.WithStack(err)
			}

		}
	}
	if err != nil {
		return bigquery.TableMetadata{}, errors.WithStack(err)
	}
	b.tables[table] = *meta
	return *meta, err
}

func (b *BigQueryClient) SaveRecord(ctx context.Context, records map[string][]*Record) error {
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
				err = b.bigqueryUpdate(ctx, tableMetadata, events[0].Tags)
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

func (b *BigQueryClient) bigqueryUpdate(ctx context.Context, metadata bigquery.TableMetadata, tags []*pb.Tag) error {
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
		case *pb.Tag_Timestamp:
			f.Type = bigquery.TimestampFieldType
		}
		schema = append(schema, f)
	}
	md, err := b.dataset.Table(metadata.Name).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: schema,
	}, metadata.ETag)
	if err != nil {
		return errors.WithStack(err)
	}
	b.tables[metadata.Name] = *md
	return nil
}

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
