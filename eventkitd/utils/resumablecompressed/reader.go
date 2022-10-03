package resumablecompressed

import (
	"compress/zlib"
	"errors"
	"io"

	"github.com/jtolio/eventkit/eventkitd/utils/delimited"
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

type readerFunc func(p []byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) { return f(p) }

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.current == nil {
		for r.r.Delimited() {
			r.r.Advance()
		}

		var read int64
		current, err := zlib.NewReader(readerFunc(func(p []byte) (n int, err error) {
			n, err = r.r.Read(p)
			read += int64(n)
			return n, err
		}))
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) && read == 0 {
				err = io.EOF
			}
			return 0, err
		}
		r.current = current
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
