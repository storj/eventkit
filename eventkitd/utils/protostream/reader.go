package protostream

import (
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
)

type Reader struct {
	base io.Reader
}

func NewReader(base io.Reader) *Reader {
	return &Reader{
		base: base,
	}
}

func (r *Reader) Unmarshal(pb proto.Message) error {
	var header [4]byte
	_, err := io.ReadFull(r.base, header[:])
	if err != nil {
		return err
	}
	buf := make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err = io.ReadFull(r.base, buf)
	if err != nil {
		return err
	}
	return proto.Unmarshal(buf, pb)
}
