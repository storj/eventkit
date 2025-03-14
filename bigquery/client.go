package bigquery

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/pkg/errors"
	"github.com/zeebo/errs/v2"
	"google.golang.org/api/option"
)

var nonSafeTableNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9]+`)
var multiUnderscore = regexp.MustCompile(`_{2,}`)

type BigQueryClient struct {
	projectID    string
	datasetID    string
	client       *bigquery.Client
	writerClient *managedwriter.Client

	streamMu sync.Mutex                              // Lock for schema changes
	streams  map[string]*managedwriter.ManagedStream // Cache of writer streams by table name

	streamCtx    context.Context
	streamCancel context.CancelFunc

	schemaMu sync.Mutex
	tables   []*Schema
}

func NewBigQueryClient(ctx context.Context, project, datasetName string, options ...option.ClientOption) (*BigQueryClient, error) {
	client, err := bigquery.NewClient(ctx, project, options...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sctx, scancel := context.WithCancel(context.Background())

	// Create a managedwriter client
	writerClient, err := managedwriter.NewClient(sctx, project, options...)
	if err != nil {
		scancel()
		return nil, errors.WithStack(err)
	}

	return &BigQueryClient{
		projectID:    project,
		datasetID:    datasetName,
		client:       client,
		writerClient: writerClient,
		streamCtx:    sctx,
		streamCancel: scancel,
		streams:      make(map[string]*managedwriter.ManagedStream),
	}, nil
}

func (b *BigQueryClient) dataset() *bigquery.Dataset {
	return b.client.Dataset(b.datasetID)
}

// getOrCreateManagedStream returns a cached managedwriter stream for the table, or creates a new one
func (b *BigQueryClient) getOrCreateManagedStream(ctx context.Context, table string, schema *Schema) (*managedwriter.ManagedStream, error) {
	b.streamMu.Lock()
	defer b.streamMu.Unlock()

	// Check if we already have a stream for this table
	if stream, ok := b.streams[table]; ok {
		return stream, nil
	}

	// Create the full table identifier
	tableID := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", b.projectID, b.datasetID, table)

	stream, err := b.writerClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tableID),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.EnableWriteRetries(true),
		managedwriter.WithSchemaDescriptor(schema.PBDescriptor()),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Store the stream for future use
	b.streams[table] = stream

	return stream, nil
}

func (b *BigQueryClient) getOrCreateSchema(ctx context.Context, table string) (*Schema, error) {
	b.schemaMu.Lock()
	defer b.schemaMu.Unlock()
	for _, meta := range b.tables {
		if meta.name == table {
			return meta, nil
		}
	}

	meta, err := NewSchema(ctx, b.dataset(), table)
	if err != nil {
		return meta, err
	}
	b.tables = append(b.tables, meta)
	return meta, nil
}

func (b *BigQueryClient) SaveRecord(records map[string][]*Record) error {
	// this is a local context, to eventually timeout
	ctx, cancel := context.WithTimeout(b.streamCtx, 1*time.Minute)
	defer cancel()
	for table, events := range records {
		schema, err := b.getOrCreateSchema(ctx, table)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}

		// Check if we need to update the table schema for new tags
		changed, err := schema.UpdateIfRequired(ctx, events[0].Tags, b.dataset())
		if err != nil {
			return errs.Wrap(err)
		}
		if changed {
			// If the schema changed, we need to recreate the stream
			b.streamMu.Lock()
			if stream, ok := b.streams[table]; ok {
				stream.Close()
				delete(b.streams, table)
			}
			b.streamMu.Unlock()
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
				err := b.saveBatchWithManagedWriter(table, batch, schema)
				if err != nil {
					// If the managedwriter API fails, fall back to the standard API
					useManagedWriter = false
					// Log the error but continue with the fallback
					fmt.Printf("WARNING: managedwriter API failed: %++v, falling back to standard API\n", err)
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
func (b *BigQueryClient) saveBatchWithManagedWriter(table string, batch []*Record, schema *Schema) error {
	// Get or create a managed stream for this table
	stream, err := b.getOrCreateManagedStream(b.streamCtx, table, schema)
	if err != nil {
		return err
	}

	var messages [][]byte
	for _, record := range batch {
		pb, err := schema.RecordToPB(record)
		if err != nil {
			return errors.WithStack(err)
		}
		messages = append(messages, pb)
	}
	// Append rows to the stream
	result, err := stream.AppendRows(b.streamCtx, messages)
	if err != nil {
		return errors.WithStack(err)
	}

	// Wait for the result to complete
	_, err = result.GetResult(b.streamCtx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return errors.WithStack(err)
	}

	return nil
}

// Close closes all clients and connections
func (b *BigQueryClient) close() error {
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
	b.streamCancel()
	return errsGroup.Err()
}
