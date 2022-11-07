package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jtolio/eventkit/eventkitd/private/path"
	"github.com/jtolio/eventkit/eventkitd/private/protostream"
	"github.com/jtolio/eventkit/eventkitd/private/resumablecompressed"
	"github.com/jtolio/eventkit/pb"
)

type jsonRecord struct {
	*pb.Record

	Name  string
	Scope []string
}

func main() {
	flag.Parse()
	enc := json.NewEncoder(os.Stdout)
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

					err = enc.Encode(jsonRecord{
						Record: &record,
						Name:   name,
						Scope:  scope,
					})
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}
