package bigquery

import (
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"

	"storj.io/eventkit/pb"
)

var _ bigquery.ValueSaver = &Record{}

type Record struct {
	Application Application

	Source Source

	ReceivedAt time.Time
	Timestamp  time.Time
	Correction time.Duration

	Tags []*pb.Tag
}

type Application struct {
	Name    string
	Version string
}

type Source struct {
	Instance string
	Address  string
}

// ToJSON converts the Record to a JSON representation that BigQuery can ingest
func (r *Record) ToJSON() ([]byte, error) {
	// Create a map for all fields
	fields := make(map[string]interface{})
	
	// Add standard fields
	fields["application_name"] = r.Application.Name
	fields["application_version"] = r.Application.Version
	fields["source_instance"] = r.Source.Instance
	fields["source_ip"] = r.Source.Address
	fields["received_at"] = r.ReceivedAt.Format(time.RFC3339Nano)
	fields["timestamp"] = r.Timestamp.Format(time.RFC3339Nano)
	fields["correction"] = r.Correction.Nanoseconds()
	
	// Add tag fields
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
			fields[field] = string(v.String_)
		case *pb.Tag_Timestamp:
			fields[field] = time.Unix(v.Timestamp.Seconds, int64(v.Timestamp.Nanos)).Format(time.RFC3339Nano)
		}
	}
	
	return json.Marshal(fields)
}

// Save implements the bigquery.ValueSaver interface
func (r *Record) Save() (map[string]bigquery.Value, string, error) {
	fields := make(map[string]bigquery.Value)
	fields["application_name"] = r.Application.Name
	fields["application_version"] = r.Application.Version

	fields["source_instance"] = r.Source.Instance
	fields["source_ip"] = r.Source.Address

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
			fields[field] = string(v.String_)
		case *pb.Tag_Timestamp:
			fields[field] = time.Unix(v.Timestamp.Seconds, int64(v.Timestamp.Nanos))
		}
	}

	return fields, "", nil
}
