package eventkit

import (
	"bytes"
	"context"
	"net"
	"sync"
	"time"

	"github.com/klauspost/compress/zlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jtolio/eventkit/pb"
	"github.com/jtolio/eventkit/utils"
)

const (
	defaultQueueDepth           = 100
	defaultMaxUncompressedBytes = 1000
	defaultCompressionLevel     = zlib.BestCompression
	defaultFlushInterval        = 15 * time.Second
)

// this is the size of a zlib compressed, serialized pb.Packet with SendOffset
// set to a reasonable value.
const trailerSize = 24

type UDPClient struct {
	Application string
	Version     string
	Instance    string
	Addr        string

	QueueDepth           int
	MaxUncompressedBytes int
	CompressionLevel     int
	FlushInterval        time.Duration

	initOnce    sync.Once
	submitQueue chan *Event
}

func NewUDPClient(application, version, instance, addr string) *UDPClient {
	c := &UDPClient{
		Application: application,
		Version:     version,
		Instance:    instance,
		Addr:        addr,

		QueueDepth:           defaultQueueDepth,
		MaxUncompressedBytes: defaultMaxUncompressedBytes,
		CompressionLevel:     defaultCompressionLevel,
		FlushInterval:        defaultFlushInterval,
	}
	return c
}

func (c *UDPClient) init() {
	c.initOnce.Do(func() {
		c.submitQueue = make(chan *Event, c.QueueDepth)
	})
}

type outgoingPacket struct {
	buf                      bytes.Buffer
	zl                       *zlib.Writer
	written, maxUncompressed int
	events                   int
	startTime                time.Time
}

func (c *UDPClient) newOutgoingPacket() *outgoingPacket {
	op := &outgoingPacket{
		startTime:       time.Now(),
		maxUncompressed: c.MaxUncompressedBytes,
	}
	_, err := op.buf.Write([]byte("EK"))
	if err != nil {
		panic(err)
	}
	op.zl, err = zlib.NewWriterLevel(&op.buf, c.CompressionLevel)
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
	return (op.written + trailerSize) > op.maxUncompressed
}

func (c *UDPClient) Run(ctx context.Context) {
	c.init()

	ticker := utils.NewJitteredTicker(c.FlushInterval)
	defer ticker.Stop()

	p := c.newOutgoingPacket()

	sendAndReset := func() {
		_ = c.send(p, c.Addr)
		p = c.newOutgoingPacket()
	}

	for {
		select {
		case em := <-c.submitQueue:
			if p.addEvent(em) {
				sendAndReset()
			}
		case <-ticker.C:
			if p.events > 0 {
				sendAndReset()
			}
		case <-ctx.Done():
			if p.events > 0 {
				_ = c.send(p, c.Addr)
			}
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
	c.init()

	select {
	case c.submitQueue <- event:
	default:
	}
}
