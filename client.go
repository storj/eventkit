package eventkit

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/klauspost/compress/zlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jtolio/eventkit/pb"
)

const (
	// TODO: configurable
	queueDepth           = 100
	maxUncompressedBytes = 1000
	compressionLevel     = zlib.BestCompression
	// TODO: jitter
	flushInterval = 15 * time.Second
)

type UDPClient struct {
	Application string
	Version     string
	Instance    string

	submitQueue chan *Event
	addr        string
}

func NewUDPClient(application, version, instance, addr string) *UDPClient {
	c := &UDPClient{
		Application: application,
		Version:     version,
		Instance:    instance,
		addr:        addr,
		submitQueue: make(chan *Event, queueDepth),
	}
	return c
}

var trailerSize = func() int {
	var buf bytes.Buffer
	zl, err := zlib.NewWriterLevel(&buf, compressionLevel)
	if err != nil {
		panic(err)
	}

	data, err := proto.Marshal(&pb.Packet{
		SendOffset: durationpb.New(time.Since(time.Unix(0, 0))),
	})
	if err != nil {
		panic(err)
	}

	_, err = zl.Write(data)
	if err != nil {
		panic(err)
	}

	err = zl.Close()
	if err != nil {
		panic(err)
	}

	return buf.Len()
}()

type outgoingPacket struct {
	buf       bytes.Buffer
	zl        *zlib.Writer
	written   int
	events    int
	startTime time.Time
}

func (c *UDPClient) newOutgoingPacket() *outgoingPacket {
	op := &outgoingPacket{startTime: time.Now()}
	_, err := op.buf.Write([]byte("EK"))
	if err != nil {
		panic(err)
	}
	op.zl, err = zlib.NewWriterLevel(&op.buf, compressionLevel)
	if err != nil {
		panic(err)
	}

	data, err := proto.Marshal(&pb.Packet{
		Application:        c.Application,
		ApplicationVersion: c.Version,
		Instance:           c.Instance,
		StartTimestamp:     timestamppb.New(op.startTime),
	})
	if err != nil {
		panic(err)
	}

	op.written += 2 + len(data)

	_, err = op.zl.Write(data)
	if err != nil {
		panic(err)
	}

	return op
}

func (op *outgoingPacket) finalize() []byte {
	data, err := proto.Marshal(&pb.Packet{
		SendOffset: durationpb.New(time.Since(op.startTime)),
	})
	if err != nil {
		panic(err)
	}

	_, err = op.zl.Write(data)
	if err != nil {
		panic(err)
	}

	err = op.zl.Close()
	if err != nil {
		panic(err)
	}
	return op.buf.Bytes()
}

func (op *outgoingPacket) addEvent(ev *Event) (full bool) {
	var v pb.Event

	v.Name = ev.Name
	v.Scope = ev.Scope
	v.TimestampOffset = durationpb.New(ev.Timestamp.Sub(op.startTime))
	v.Tags = ev.Tags

	data, err := proto.Marshal(&pb.Packet{Events: []*pb.Event{&v}})
	if err != nil {
		panic(err)
	}

	op.written += len(data)

	_, err = op.zl.Write(data)
	if err != nil {
		panic(err)
	}

	err = op.zl.Flush()
	if err != nil {
		panic(err)
	}

	op.events += 1
	return (op.written + trailerSize) > maxUncompressedBytes
}

func (c *UDPClient) Run(ctx context.Context) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	p := c.newOutgoingPacket()

	send := func() {
		_ = c.send(p, c.addr)
		p = c.newOutgoingPacket()
	}

	for {
		select {
		case em := <-c.submitQueue:
			if p.addEvent(em) {
				send()
			}
		case <-ticker.C:
			if p.events > 0 {
				send()
			}
		case <-ctx.Done():
			send()
			return
		}
	}
}

func (c *UDPClient) send(packet *outgoingPacket, addr string) error {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, laddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _, err = conn.WriteMsgUDP(packet.finalize(), nil, nil)
	return err
}

func (c *UDPClient) Submit(event *Event) {
	select {
	case c.submitQueue <- event:
	default:
	}
}
