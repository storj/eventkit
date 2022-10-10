//go:build go1.18
// +build go1.18

package protostream

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jtolio/eventkit/pb"
)

func FuzzBasic(f *testing.F) {
	f.Add("")

	var out bytes.Buffer
	err := NewWriter(&out).Marshal(&pb.Event{Name: "test event"})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(out.String())

	f.Fuzz(func(t *testing.T, data string) {
		_ = NewReader(strings.NewReader(data)).Unmarshal(&pb.Event{})
	})
}
