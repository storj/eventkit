package delimited

import (
	"bytes"
	"encoding/binary"
	"io"
)

const frameHeaderSize = 3 // (control byte + 2 bytes for size)
const maxFrameSize = 1<<16 - 1

type controlByte byte

const (
	controlData    controlByte = 0
	controlDelimit controlByte = 1
)

type Writer struct {
	base io.Writer
	buf  bytes.Buffer // optimization TODO: replace with [maxFrameSize]byte
}

func NewWriter(base io.Writer) *Writer {
	return &Writer{
		base: base,
	}
}

func (w *Writer) Write(p []byte) (n int, err error) {
	n, _ = w.buf.Write(p)
	for w.buf.Len() >= maxFrameSize {
		err = w.flushFrame()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (w *Writer) flushFrame() error {
	var buf [maxFrameSize]byte
	n, _ := w.buf.Read(buf[:])
	if n == 0 {
		return nil
	}
	var header [frameHeaderSize]byte
	header[0] = byte(controlData)
	binary.BigEndian.PutUint16(header[1:], uint16(n))
	_, err := w.base.Write(header[:])
	if err != nil {
		return err
	}
	_, err = w.base.Write(buf[:n])
	return err
}

func (w *Writer) Flush() error {
	for w.buf.Len() > 0 {
		err := w.flushFrame()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Delimit() error {
	err := w.Flush()
	if err != nil {
		return err
	}
	var header [frameHeaderSize]byte
	header[0] = byte(controlDelimit)
	_, err = w.base.Write(header[:])
	return err
}
