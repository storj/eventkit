package protostream

import (
	"bytes"
	"testing"

	"github.com/jtolio/eventkit/pb"
)

func assert(t testing.TB, val bool) {
	if !val {
		t.Fatal("assertion failed")
	}
}

func TestBasic(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	w := NewWriter(&out)
	x := &pb.Event{Name: "test event"}
	assert(t, w.Marshal(x) == nil)

	r := NewReader(&out)
	var y pb.Event
	assert(t, y.Name != "test event")
	assert(t, r.Unmarshal(&y) == nil)
	assert(t, y.Name == "test event")
}
