package eventkit

import (
	"bytes"
	"compress/zlib"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/jtolio/eventkit/pb"
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

	mu      sync.Mutex
	closed  bool
	active  sync.WaitGroup
	pending *outgoingPacket
	flush   chan struct{}
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
		StartTimestamp:     pb.AsTimestamp(op.startTime),
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
		SendOffsetNs: int64(time.Since(op.startTime)),
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
	v.TimestampOffsetNs = int64(ev.Timestamp.Sub(op.startTime))
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

func (c *UDPClient) Submit(event *Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	if c.pending == nil {
		c.pending = c.newOutgoingPacket()
		c.flush = make(chan struct{}, 1)
		c.active.Add(1)
		go c.waitForFull(c.pending, c.flush)
	}

	if c.pending.addEvent(event) {
		c.flush <- struct{}{}
		c.pending, c.flush = nil, nil
	}
}

func (c *UDPClient) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	c.active.Wait()
}

func (c *UDPClient) waitForFull(active *outgoingPacket, work chan struct{}) {
	defer c.active.Done()

	tick := time.NewTimer(c.FlushInterval) // TODO: jitter
	select {
	case <-work:
	case <-tick.C:
	}
	tick.Stop()

	c.mu.Lock()
	if c.pending == active {
		c.pending, c.flush = nil, nil
	}
	c.mu.Unlock()

	c.send(active, c.Addr)
}

func (c *UDPClient) send(packet *outgoingPacket, addr string) (err error) {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, laddr)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := conn.Close(); err == nil && errClose != nil {
			err = errClose
		}
	}()

	_, _, err = conn.WriteMsgUDP(packet.finalize(), nil, nil)
	return err
}
