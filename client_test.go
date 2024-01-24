package eventkit

import (
	"context"
	"reflect"
	"testing"
	"time"

	"storj.io/eventkit/pb"
	"storj.io/eventkit/transport"
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

func TestZeroValueType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l, err := transport.ListenUDP("127.0.0.1:0")
	requireNoError(t, err)
	defer l.Close()

	go func() {
		client := NewUDPClient("application", "v1.0.0", "instance", l.LocalAddr().String())
		client.FlushInterval = time.Millisecond

		go client.Run(ctx)

		event := &Event{
			Name:  "Name",
			Scope: []string{"package/name"},
			Tags:  []*pb.Tag{Int64("key", 0)},
		}
		client.Submit(event)
	}()
	payload, _, err := l.Next()
	requireNoError(t, err)
	packet, err := transport.ParsePacket(payload)
	requireNoError(t, err)

	requireEqual(t, len(packet.Events), 1)
	requireEqual(t, packet.Events[0].Name, "Name")
	requireEqual(t, packet.Events[0].Scope, []string{"package/name"})
	requireEqual(t, len(packet.Events[0].Tags), 1)
	requireEqual(t, packet.Events[0].Tags[0].Key, "key")
	val, ok := packet.Events[0].Tags[0].Value.(*pb.Tag_Int64)
	requireEqual(t, ok, true)
	requireEqual(t, val.Int64, int64(0))
}

func requireNoError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func requireEqual(t *testing.T, actual, expected any) {
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("equality expected: %q vs %q", actual, expected)
	}
}
