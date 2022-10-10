package main

import (
	"compress/zlib"
	"os"
	"path/filepath"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.com/jtolio/eventkit/eventkitd/private/protostream"
	"github.com/jtolio/eventkit/eventkitd/private/resumablecompressed"
)

type handle struct {
	mtx   sync.Mutex
	close func() error
	ps    *protostream.Writer
}

type Writer struct {
	mtx     sync.Mutex
	handles map[string]*handle
}

func NewWriter() *Writer { // nolint:deadcode
	return &Writer{
		handles: map[string]*handle{},
	}
}

func (w *Writer) DropAll() {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	for path, handle := range w.handles {
		delete(w.handles, path)
		handle.Close()
	}
}

func (handle *handle) Close() {
	handle.mtx.Lock()
	defer handle.mtx.Unlock()
	_ = handle.close()
}

func (w *Writer) Close() {
	w.DropAll()
}

func (w *Writer) Append(path string, pb proto.Message) error {
	w.mtx.Lock()
	h, ok := w.handles[path]
	if !ok {
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			w.mtx.Unlock()
			return err
		}
		fh, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			w.mtx.Unlock()
			return err
		}

		rcw, err := resumablecompressed.NewWriter(fh, zlib.DefaultCompression)
		if err != nil {
			w.mtx.Unlock()
			fh.Close()
			return err
		}

		h = &handle{
			close: rcw.Close,
			ps:    protostream.NewWriter(rcw),
		}
		w.handles[path] = h
	}

	h.mtx.Lock()
	defer h.mtx.Unlock()
	w.mtx.Unlock()

	// TODO: consider refactoring protostream to allow for marshaling before grabbing
	// any locks
	return h.ps.Marshal(pb)
}
