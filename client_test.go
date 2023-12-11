package eventkit

import (
	"testing"
	"time"

	"github.com/jtolio/eventkit/pb"
)

func BenchmarkOutgoingPacket(b *testing.B) {
	client := NewUDPClient("application", "v1.0.0", "instance", "127.0.0.1:99999")

	event := &Event{
		Name:  "Name",
		Scope: []string{"alpha", "beta"},
		Tags:  []*pb.Tag{String("key", "value")},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		packet := client.newOutgoingPacket()
		for k := 0; k < 70; k++ {
			event.Timestamp = event.Timestamp.Add(100 * time.Millisecond)
			full := packet.addEvent(&Event{})
			if full {
				b.Fatal("filled internal buffer")
			}
		}
		_ = packet.finalize()
	}
}
