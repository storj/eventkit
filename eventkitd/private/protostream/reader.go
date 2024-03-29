package protostream

import (
	"encoding/binary"
	"fmt"
	"io"

	"storj.io/picobuf"
)

type Reader struct {
	base io.Reader
}

func NewReader(base io.Reader) *Reader {
	return &Reader{
		base: base,
	}
}

func (r *Reader) Unmarshal(pb picobuf.Message) error {
	var header [4]byte
	_, err := io.ReadFull(r.base, header[:])
	if err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size > maxSerializedSize {
		return fmt.Errorf("frame size larger than max")
	}
	buf := make([]byte, size)
	_, err = io.ReadFull(r.base, buf)
	if err != nil {
		return err
	}
	return picobuf.Unmarshal(buf, pb)
}
