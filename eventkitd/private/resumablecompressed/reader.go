package resumablecompressed

import (
	"compress/zlib"
	"errors"
	"io"

	"github.com/jtolio/eventkit/eventkitd/private/delimited"
)

type Reader struct {
	r       *delimited.Reader
	current io.ReadCloser
}

func NewReader(base io.Reader) *Reader {
	r := delimited.NewReader(base)
	return &Reader{
		r: r,
	}
}

type measuredReader struct {
	io.Reader
	Count int64
}

func (r *measuredReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.Count += int64(n)
	return n, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.current == nil {
		for {
			more := r.r.Next()
			if !more {
				err := r.r.Err()
				if err == nil {
					err = io.EOF
				}
				return 0, err
			}

			mr := &measuredReader{Reader: r.r}
			current, err := zlib.NewReader(mr)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) && mr.Count == 0 {
					continue
				}
				return 0, err
			}
			r.current = current
			break
		}
	}

	n, err = r.current.Read(p)
	if errors.Is(err, io.EOF) {
		err = r.current.Close()
		r.current = nil
		io.Copy(io.Discard, r.r)
	}
	return n, err
}

func (r *Reader) Close() error {
	if r.current != nil {
		return r.current.Close()
	}
	r.current = nil
	return nil
}
