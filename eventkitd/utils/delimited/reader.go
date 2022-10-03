package delimited

import (
	"encoding/binary"
	"errors"
	"io"
)

type Reader struct {
	base      io.Reader
	remaining int
	err       error
	delimited bool
}

func NewReader(base io.Reader) *Reader {
	r := &Reader{
		base: base,
	}
	r.unframe()
	return r
}

func (r *Reader) Read(p []byte) (n int, err error) {
	for {
		if r.err != nil {
			return 0, r.err
		}
		if r.delimited {
			return 0, io.EOF
		}
		if r.remaining > 0 {
			if len(p) > r.remaining {
				p = p[:r.remaining]
			}
			n, err = r.base.Read(p)
			r.remaining -= n
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				r.err = err
			}
			return n, err
		}
		r.unframe()
	}
}

func (r *Reader) unframe() {
	var header [frameHeaderSize]byte
	_, err := io.ReadFull(r.base, header[:])
	if err != nil {
		r.err = err
		return
	}

	switch controlByte(header[0]) {
	case controlData:
		r.remaining = int(binary.BigEndian.Uint16(header[1:]))
		r.delimited = false
	case controlDelimit:
		r.remaining = 0
		r.delimited = true
	default:
		r.err = errors.New("delimited stream: unknown control byte")
	}
}

// Delimited returns true if the delimited.Reader is sitting on top of a
// stream delimiter. There may be data before or after a delimiter, and there
// may be no data between delimiters.
func (r *Reader) Delimited() bool {
	return r.err == nil && r.delimited
}

// Advance moves exactly one delimiter and frees up the next range for
// reads.
func (r *Reader) Advance() {
	if r.err == nil && r.delimited && r.remaining == 0 {
		r.unframe()
	}
}
