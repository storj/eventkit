package protostream

import (
	"bytes"
	"testing"

	"github.com/jtolio/eventkit/pb"
)

func assert(val bool) {
	if !val {
		panic("assertion failed")
	}
}

func TestBasic(t *testing.T) {
	var out bytes.Buffer

	w := NewWriter(&out)
	x := &pb.Event{Name: "test event"}
	assert(w.Marshal(x) == nil)

	r := NewReader(&out)
	var y pb.Event
	assert(y.Name != "test event")
	assert(r.Unmarshal(&y) == nil)
	assert(y.Name == "test event")
}
