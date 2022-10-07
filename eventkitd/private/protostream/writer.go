package protostream

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
)

const maxSerializedSize = 4 * 1024 * 1024

type Writer struct {
	base io.Writer
	buf  proto.Buffer
}

func NewWriter(base io.Writer) *Writer {
	return &Writer{
		base: base,
	}
}

func (w *Writer) Marshal(pb proto.Message) error {
	w.buf.Reset()
	err := w.buf.Marshal(pb)
	if err != nil {
		return err
	}
	data := w.buf.Bytes()
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
