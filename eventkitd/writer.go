package main

import (
	"os"
	"path/filepath"
	"sync"
)

type handle struct {
	mtx sync.Mutex
	fh  *os.File
}

type Writer struct {
	mtx     sync.Mutex
	handles map[string]*handle
}

func NewWriter() *Writer {
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
	_ = handle.fh.Close()
}

func (w *Writer) Close() {
	w.DropAll()
}

func (w *Writer) Append(path string, data []byte) error {
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
		h = &handle{
			fh: fh,
		}
		w.handles[path] = h
	}

	h.mtx.Lock()
	defer h.mtx.Unlock()
	w.mtx.Unlock()

	_, err := h.fh.Write(data)
	return err
}
