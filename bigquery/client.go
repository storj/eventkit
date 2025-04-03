package bigquery

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/pkg/errors"
	"github.com/zeebo/errs/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"storj.io/eventkit/pb"
)

var nonSafeTableNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9]+`)
var multiUnderscore = regexp.MustCompile(`_{2,}`)

type BigQueryClient struct {
	projectID        string
	datasetID        string
	client           *bigquery.Client
	writerClient     *managedwriter.Client
	tables           map[string]*bigquery.TableMetadata
	streams          map[string]*managedwriter.ManagedStream // Cache of writer streams by table name
	schemeChangeLock sync.Locker
}

func NewBigQueryClient(ctx context.Context, project, datasetName string, options ...option.ClientOption) (*BigQueryClient, error) {
	client, err := bigquery.NewClient(ctx, project, options...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Create a managedwriter client
	writerClient, err := managedwriter.NewClient(ctx, project, options...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &BigQueryClient{
		projectID:        project,
		datasetID:        datasetName,
		client:           client,
		writerClient:     writerClient,
		schemeChangeLock: &sync.Mutex{},
		tables:           make(map[string]*bigquery.TableMetadata),
		streams:          make(map[string]*managedwriter.ManagedStream),
	}, nil
}

func (b *BigQueryClient) dataset() *bigquery.Dataset {
	return b.client.Dataset(b.datasetID)
}

// getOrCreateManagedStream returns a cached managedwriter stream for the table, or creates a new one
func (b *BigQueryClient) getOrCreateManagedStream(ctx context.Context, table string) (*managedwriter.ManagedStream, error) {
	b.schemeChangeLock.Lock()
	defer b.schemeChangeLock.Unlock()

	// Check if we already have a stream for this table
	if stream, ok := b.streams[table]; ok {
		return stream, nil
	}

	// Create the full table identifier
	tableID := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", b.projectID, b.datasetID, table)

	// Create a new ManagedStream for this table
	stream, err := b.writerClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tableID),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Store the stream for future use
	b.streams[table] = stream

	return stream, nil
}

func (b *BigQueryClient) createOrLoadTableScheme(ctx context.Context, table string) (*bigquery.TableMetadata, error) {
	b.schemeChangeLock.Lock()
	defer b.schemeChangeLock.Unlock()

	tableMetadata, found := b.tables[table]
	if found {
		return tableMetadata, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	meta, err := b.dataset().Table(table).Metadata(ctx)
	switch e := err.(type) {
	case *googleapi.Error:
		if e.Code == 404 {
			err = b.dataset().Table(table).Create(ctx, &bigquery.TableMetadata{
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
			meta, err = b.dataset().Table(table).Metadata(ctx)
			if err != nil {
				return tableMetadata, errors.WithStack(err)
			}
		}
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	b.tables[table] = meta
	return meta, err
}

// convertRecordsToJSON converts records to JSON format for the Storage Write API
func (b *BigQueryClient) convertRecordsToJSON(events []*Record) ([][]byte, error) {
	jsonRows := make([][]byte, 0, len(events))

	for _, event := range events {
		// Convert to JSON bytes
		jsonData, err := event.ToJSON()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		jsonRows = append(jsonRows, jsonData)
	}

	return jsonRows, nil
}

func (b *BigQueryClient) SaveRecord(ctx context.Context, records map[string][]*Record) error {
	for table, events := range records {
		tableMetadata, err := b.createOrLoadTableScheme(ctx, table)
		if err != nil {
			return err
		}

		// Check if we need to update the table schema for new tags
		if len(events) > 0 {
			if isTagMissing(tableMetadata.Schema, events[0].Tags) {
				err = b.bigqueryUpdate(ctx, tableMetadata, events[0].Tags)
				if err != nil {
					return err
				}
			}
		}

		// Try to use the managedwriter API first
		useManagedWriter := true

		// Process records in batches to avoid overwhelming the API
		const batchSize = 500
		for i := 0; i < len(events); i += batchSize {
			end := i + batchSize
			if end > len(events) {
				end = len(events)
			}

			batch := events[i:end]

			if useManagedWriter {
				// Try to use the managedwriter API
				err := b.saveBatchWithManagedWriter(ctx, table, batch)
				if err != nil {
					// If the managedwriter API fails, fall back to the standard API
					useManagedWriter = false
					// Log the error but continue with the fallback
					fmt.Printf("WARNING: managedwriter API failed: %v, falling back to standard API\n", err)
				} else {
					// Successfully used managedwriter, continue with the next batch
					continue
				}
			}

			// Fallback to standard BigQuery API
			err = b.dataset().Table(table).Inserter().Put(ctx, batch)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// saveBatchWithManagedWriter saves a batch of records using the managedwriter API
func (b *BigQueryClient) saveBatchWithManagedWriter(ctx context.Context, table string, batch []*Record) error {
	// Get or create a managed stream for this table
	stream, err := b.getOrCreateManagedStream(ctx, table)
	if err != nil {
		return err
	}

	// Convert records to JSON format
	jsonRows, err := b.convertRecordsToJSON(batch)
	if err != nil {
		return err
	}

	// Append rows to the stream
	result, err := stream.AppendRows(ctx, jsonRows)
	if err != nil {
		return errors.WithStack(err)
	}

	// Wait for the result to complete
	_, err = result.GetResult(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (b *BigQueryClient) bigqueryUpdate(ctx context.Context, metadata *bigquery.TableMetadata, tags []*pb.Tag) error {
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
		default:
			continue tagloop
		}
		schema = append(schema, f)
	}
	md, err := b.dataset().Table(metadata.Name).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: schema,
	}, metadata.ETag)
	if err != nil {
		return errors.WithStack(err)
	}
	b.tables[metadata.Name] = md

	// When we update the schema, we need to delete the stream cache entry
	// so that it will be recreated with the new schema on the next SaveRecord call
	if stream, ok := b.streams[metadata.Name]; ok {
		// Close the existing stream
		_ = stream.Close()
		delete(b.streams, metadata.Name)
	}

	return nil
}

// TableName defines the naming convention between event and BQ table name.
func TableName(scope []string, name string) string {
	var res []string
	for _, scope := range scope {
		res = append(res, nonSafeTableNameCharacters.ReplaceAllString(scope, "_"))
	}
	res = append(res, nonSafeTableNameCharacters.ReplaceAllString(name, "_"))

	all := multiUnderscore.ReplaceAllString(strings.Join(res, "_"), "_")
	all = strings.Trim(all, "_")
	return all
}

// Close closes all clients and connections
func (b *BigQueryClient) Close() error {
	var errsGroup errs.Group

	// Close all managed streams
	for table, stream := range b.streams {
		if err := stream.Close(); err != nil {
			errsGroup.Add(errors.WithStack(err))
		}
		delete(b.streams, table)
	}

	// Close the writer client
	if err := b.writerClient.Close(); err != nil {
		errsGroup.Add(errors.WithStack(err))
	}

	// Close the BigQuery client
	if err := b.client.Close(); err != nil {
		errsGroup.Add(errors.WithStack(err))
	}

	return errsGroup.Err()
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
