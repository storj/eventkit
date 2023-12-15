package protostream

import (
	"encoding/binary"
	"fmt"
	"io"

	"storj.io/picobuf"
)

const maxSerializedSize = 4 * 1024 * 1024

type Writer struct {
	base io.Writer
}

func NewWriter(base io.Writer) *Writer {
	return &Writer{
		base: base,
	}
}

func (w *Writer) Marshal(pb picobuf.Message) error {
	data, err := picobuf.Marshal(pb)
	if err != nil {
		return err
	}
	if len(data) > maxSerializedSize {
		return fmt.Errorf("too large of message for stream")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	_, err = w.base.Write(header[:])
	if err != nil {
		return err
	}
	_, err = w.base.Write(data)
	return err
}
