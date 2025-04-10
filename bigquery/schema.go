package bigquery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"github.com/zeebo/errs/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"storj.io/eventkit/pb"
)

// Schema represents the schema of a BigQuery table.
type Schema struct {
	name string

	schemeChangeLock sync.Locker
	tableMetadata    *bigquery.TableMetadata

	// messageDescriptor is used to create the dynamic pb message
	messageDescriptor protoreflect.MessageDescriptor
}

// NewSchema creates a new Schema instance for the given table in the dataset.
func NewSchema(ctx context.Context, ds *bigquery.Dataset, table string) (*Schema, error) {
	s := &Schema{
		name:             table,
		schemeChangeLock: &sync.Mutex{},
	}

	meta, err := LoadTableMetadata(ctx, ds, table)
	if err != nil {
		return nil, err
	}
	s.tableMetadata = meta
	s.messageDescriptor, err = toMessageDescriptor(s.tableMetadata)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Schema) UpdateIfRequired(ctx context.Context, tags []*pb.Tag, ds *bigquery.Dataset) (changed bool, err error) {
	s.schemeChangeLock.Lock()
	defer s.schemeChangeLock.Unlock()
	if !isTagMissing(s.tableMetadata.Schema, tags) {
		return false, nil
	}

	schema := s.tableMetadata.Schema
	origLen := len(schema)
tagloop:
	for _, tag := range tags {
		for _, field := range s.tableMetadata.Schema {
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
	if origLen == len(schema) {
		return false, nil
	}

	md, err := ds.Table(s.name).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: schema,
	}, s.tableMetadata.ETag)
	if err != nil {
		return true, errors.WithStack(err)
	}

	// TODO: Unfortunately, the ds.Table().Update is eventual consistent. It's not guaranteed to be finished.
	// Usually the next stream write fails with:
	// code = InvalidArgument desc = Input schema has more fields than BigQuery schema, extra fields: 'tag_foo2'
	// I don't have good solution for now. But because this should happen very rarely (usually during deploy time, when new tag was introduced),
	// we can just wait a bit. It's inconvenient for tools like eventkit-time, but this is what it is.
	time.Sleep(10 * time.Second)
	s.tableMetadata = md

	s.messageDescriptor, err = toMessageDescriptor(s.tableMetadata)
	return true, err
}

// toMessageDescriptor converts the BigQuery table metadata to a protobuf message descriptor.
func toMessageDescriptor(metadata *bigquery.TableMetadata) (protoreflect.MessageDescriptor, error) {
	descriptorProto := descriptorpb.DescriptorProto{
		Name:  proto.String("BqMessage"),
		Field: make([]*descriptorpb.FieldDescriptorProto, 0, len(metadata.Schema)),
	}

	for ix, field := range metadata.Schema {
		fieldDescriptor := &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(field.Name),
			JsonName: proto.String(field.Name),
			Number:   proto.Int32(int32(ix + 1)),
			Options:  &descriptorpb.FieldOptions{},
		}

		switch field.Type {
		case bigquery.StringFieldType:
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
		case bigquery.IntegerFieldType:
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
		case bigquery.FloatFieldType:
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
		case bigquery.BooleanFieldType:
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
		case bigquery.BytesFieldType:
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
		case bigquery.TimestampFieldType:
			// Use int64 for timestamp representation (microseconds since epoch)
			fieldDescriptor.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
		default:
			// Skip unknown types
			continue
		}

		descriptorProto.Field = append(descriptorProto.Field, fieldDescriptor)
	}

	fileDescriptorProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("message.proto"),
		Package:     proto.String("dynamic"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{&descriptorProto},
	}

	// Register the file descriptor
	files := []*descriptorpb.FileDescriptorProto{fileDescriptorProto}

	fileDesc, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{
		File: files,
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}

	fileDescriptor, err := fileDesc.FindFileByPath("message.proto")
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return fileDescriptor.Messages().ByName("BqMessage"), nil
}

// RecordToPB converts a Record to a protobuf message.
func (s *Schema) RecordToPB(record *Record) ([]byte, error) {
	s.schemeChangeLock.Lock()
	defer s.schemeChangeLock.Unlock()

	if s.messageDescriptor == nil {
		return nil, errors.New("message descriptor is not initialized")
	}

	msg := dynamicpb.NewMessage(s.messageDescriptor)

	if field := s.messageDescriptor.Fields().ByName("application_name"); field != nil {
		msg.Set(field, protoreflect.ValueOfString(record.Application.Name))
	}
	if field := s.messageDescriptor.Fields().ByName("application_version"); field != nil {
		msg.Set(field, protoreflect.ValueOfString(record.Application.Version))
	}
	if field := s.messageDescriptor.Fields().ByName("source_instance"); field != nil {
		msg.Set(field, protoreflect.ValueOfString(record.Source.Instance))
	}
	if field := s.messageDescriptor.Fields().ByName("source_ip"); field != nil {
		msg.Set(field, protoreflect.ValueOfString(record.Source.Address))
	}
	if field := s.messageDescriptor.Fields().ByName("received_at"); field != nil {
		// Convert time.Time to microseconds since epoch (1970-01-01)
		microseconds := record.ReceivedAt.UnixNano() / 1000
		msg.Set(field, protoreflect.ValueOfInt64(microseconds))
	}
	if field := s.messageDescriptor.Fields().ByName("timestamp"); field != nil {
		// Convert time.Time to microseconds since epoch (1970-01-01)
		microseconds := record.Timestamp.UnixNano() / 1000
		msg.Set(field, protoreflect.ValueOfInt64(microseconds))
	}
	if field := s.messageDescriptor.Fields().ByName("correction"); field != nil {
		msg.Set(field, protoreflect.ValueOfInt64(record.Correction.Nanoseconds()))
	}

	for _, tag := range record.Tags {
		fieldName := tagFieldName(tag.Key)
		if field := s.messageDescriptor.Fields().ByName(protoreflect.Name(fieldName)); field != nil {
			switch v := tag.Value.(type) {
			case *pb.Tag_Bool:
				msg.Set(field, protoreflect.ValueOfBool(v.Bool))
			case *pb.Tag_Bytes:
				msg.Set(field, protoreflect.ValueOfBytes(v.Bytes))
			case *pb.Tag_Double:
				msg.Set(field, protoreflect.ValueOfFloat64(v.Double))
			case *pb.Tag_DurationNs:
				msg.Set(field, protoreflect.ValueOfInt64(v.DurationNs))
			case *pb.Tag_Int64:
				msg.Set(field, protoreflect.ValueOfInt64(v.Int64))
			case *pb.Tag_String_:
				msg.Set(field, protoreflect.ValueOfString(string(v.String_)))
			case *pb.Tag_Timestamp:
				if v.Timestamp != nil {
					msg.Set(field, protoreflect.ValueOfInt64(v.Timestamp.Seconds*1_000_000+int64(v.Timestamp.Nanos/1000)))
				}

			}
		}
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

// PBDescriptor returns the protobuf descriptor for the schema.
func (s *Schema) PBDescriptor() *descriptorpb.DescriptorProto {
	s.schemeChangeLock.Lock()
	defer s.schemeChangeLock.Unlock()
	desc := descriptorpb.DescriptorProto{
		Name: proto.String("BqMessage"),
	}
	for ix, field := range s.tableMetadata.Schema {
		fd := &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(field.Name),
			JsonName: proto.String(field.Name),
			Number:   proto.Int32(int32(ix + 1)),
			Options:  &descriptorpb.FieldOptions{},
		}
		switch field.Type {
		case bigquery.StringFieldType:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
		case bigquery.IntegerFieldType, bigquery.TimestampFieldType:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
		case bigquery.FloatFieldType:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
		case bigquery.BooleanFieldType:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
		case bigquery.BytesFieldType:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
		default:
			fmt.Println("Warning: unknown field type", field.Type)
		}
		desc.Field = append(desc.Field, fd)
	}
	return &desc
}

// LoadTableMetadata loads the table metadata from BigQuery.
func LoadTableMetadata(ctx context.Context, ds *bigquery.Dataset, table string) (*bigquery.TableMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	meta, err := ds.Table(table).Metadata(ctx)
	switch e := err.(type) {
	case *googleapi.Error:
		if e.Code == 404 {
			err = ds.Table(table).Create(ctx, &bigquery.TableMetadata{
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
				return meta, errors.WithStack(err)
			}
			meta, err = ds.Table(table).Metadata(ctx)
			if err != nil {
				return meta, errors.WithStack(err)
			}
		}
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}
	return meta, nil
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
