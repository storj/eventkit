package resumablecompressed

import (
	"compress/zlib"
	"io"

	"storj.io/eventkit/eventkitd/private/delimited"
)

type Writer struct {
	base      io.WriteCloser
	delimited *delimited.Writer
	compress  *zlib.Writer
}

func NewWriter(base io.WriteCloser, level int) (*Writer, error) {
	w := delimited.NewWriter(base)
	if err := w.Delimit(); err != nil {
		return nil, err
	}
	compress, err := zlib.NewWriterLevel(w, level)
	if err != nil {
		w.Flush()
		return nil, err
	}
	return &Writer{
		base:      base,
		delimited: w,
		compress:  compress,
	}, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.compress.Write(p)
}

func (w *Writer) Close() error {
	compressErr := w.compress.Close()
	flushErr := w.delimited.Flush()
	closeErr := w.base.Close()
	if compressErr != nil {
		return compressErr
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
