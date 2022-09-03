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

func (w *Writer) CloseAll() {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	for path, handle := range w.handles {
		delete(w.handles, path)
		handle.mtx.Lock()
		handle.fh.Close()
		handle.mtx.Unlock()
	}
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
	w.mtx.Unlock()
	_, err := h.fh.Write(data)
	h.mtx.Unlock()
	return err
}
