package main

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jtolio/eventkit/eventkitd/private/path"
	"github.com/jtolio/eventkit/eventkitd/private/protostream"
	"github.com/jtolio/eventkit/eventkitd/private/resumablecompressed"
	"github.com/jtolio/eventkit/pb"
)

type rowMap map[string]string

func tagValueToString(tag *pb.Tag) string {
	switch t := tag.Value.(type) {
	default:
		panic("unknown tag type")
	case *pb.Tag_String_:
		return t.String_
	case *pb.Tag_Int64:
		return fmt.Sprint(t.Int64)
	case *pb.Tag_Double:
		return fmt.Sprint(t.Double)
	case *pb.Tag_Bool:
		return fmt.Sprint(t.Bool)
	case *pb.Tag_Bytes:
		return hex.EncodeToString(t.Bytes)
	case *pb.Tag_DurationNs:
		return time.Duration(t.DurationNs).String()
	case *pb.Tag_Timestamp:
		return t.Timestamp.AsTime().String()
	}
}

func recordToRow(name string, scope []string, record *pb.Record) rowMap {
	rv := make(rowMap)
	rv["name"] = name
	rv["scope"] = path.EncodeScope(scope)
	rv["application"] = record.Application
	rv["version"] = record.ApplicationVersion
	rv["instance"] = record.Instance
	rv["source_addr"] = record.SourceAddr
	rv["timestamp"] = record.Timestamp.AsTime().String()
	rv["timestamp_correction"] = time.Duration(record.TimestampCorrectionNs).String()
	for _, tag := range record.Tags {
		rv[fmt.Sprintf("tag:%s", tag.Key)] = tagValueToString(tag)
	}
	return rv
}

func main() {
	flag.Parse()
	var rows []rowMap
	header := make(rowMap)
	for _, dpath := range flag.Args() {
		err := filepath.WalkDir(dpath, func(fpath string, d fs.DirEntry, err error) error {
			if d.Type().IsRegular() {
				name, scope, err := path.Parse(fpath)
				if err != nil {
					return nil
				}
				fh, err := os.Open(fpath)
				if err != nil {
					return nil
				}
				defer fh.Close()
				r := protostream.NewReader(resumablecompressed.NewReader(fh))
				for {
					var record pb.Record
					err := r.Unmarshal(&record)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						return nil
					}
					row := recordToRow(name, scope, &record)
					for key := range row {
						header[key] = ""
					}
					rows = append(rows, row)
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	headerVals := make([]string, 0, len(header))
	for field := range header {
		headerVals = append(headerVals, field)
	}
	sort.Strings(headerVals)
	w := csv.NewWriter(os.Stdout)
	err := w.Write(headerVals)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		vals := make([]string, 0, len(header))
		for _, field := range headerVals {
			vals = append(vals, row[field])
		}
		err = w.Write(vals)
		if err != nil {
			panic(err)
		}
	}
	w.Flush()
	err = w.Error()
	if err != nil {
		panic(err)
	}
}
