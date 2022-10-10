package delimited

import (
	"encoding/binary"
	"errors"
	"io"
)

type readerFrame struct {
	control   controlByte
	data      [maxFrameSize]byte
	remaining []byte
}

type Reader struct {
	base    io.Reader
	current readerFrame
	err     error
}

func NewReader(base io.Reader) *Reader {
	r := &Reader{
		base: base,
		current: readerFrame{
			control: controlDelimit,
		},
	}
	return r
}

func (r *Reader) Next() bool {
	for r.current.control != controlDelimit && r.err == nil {
		r.readFrame()
	}
	if r.err != nil {
		return false
	}
	last := r.current.control
	r.readFrame()
	return r.err == nil || (last == controlDelimit && errors.Is(r.err, io.EOF))
}

func (r *Reader) readFrame() {
	for {
		var header [frameHeaderSize]byte
		_, err := io.ReadFull(r.base, header[:])
		if err != nil {
			r.err = err
			return
		}

		switch controlByte(header[0]) {
		case controlDelimit:
			r.current.control = controlDelimit
		case controlData:
			r.current.control = controlData
		default:
			r.err = errors.New("unknown control byte")
		}
		if r.current.control != controlData {
			return
		}
		size := int(binary.BigEndian.Uint16(header[1:]))
		r.current.remaining = r.current.data[:size]
		_, err = io.ReadFull(r.base, r.current.remaining)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return
		}
		if size > 0 {
			return
		}
	}
}

func (r *Reader) Err() error {
	err := r.err
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.current.control != controlData {
		return 0, io.EOF
	}
	n = copy(p, r.current.remaining)
	r.current.remaining = r.current.remaining[n:]
	if len(r.current.remaining) == 0 {
		r.readFrame()
	}
	return n, nil

}
