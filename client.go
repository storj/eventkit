package eventkit

import (
	"bytes"
	"compress/zlib"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"olio.lol/eventkit/pb"
)

const (
	queueDepth           = 100
	maxUncompressedBytes = 1000
	compressionLevel     = zlib.BestCompression
	flushInterval        = 15 * time.Second
)

type UDPClient struct {
	Application string
	Instance    string

	submitQueue chan EventMap
}

func NewUDPClient(application, instance, addr string) *UDPClient {
	c := &UDPClient{
		Application: application,
		Instance:    instance,

		submitQueue: make(chan EventMap, queueDepth),
	}
	go c.run(addr)
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

func newOutgoingPacket(application, instance string) *outgoingPacket {
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
		Application:    application,
		Instance:       instance,
		StartTimestamp: timestamppb.New(op.startTime),
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

func (op *outgoingPacket) finalize() {
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
}

func (op *outgoingPacket) addEvent(em EventMap) (full bool) {
	var v pb.Event

tagsLoop:
	for key, val := range em {
		switch key {
		case "name":
			v.Name = val.(string)
		case "scope":
			v.Scope = val.([]string)
		case "timestamp":
			v.TimestampOffset = durationpb.New(val.(time.Time).Sub(op.startTime))
		default:
			tag := pb.Tag{
				Key: key,
			}
			switch v := val.(type) {
			case string:
				tag.Value = &pb.Tag_String_{String_: v}
			case int64:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case uint64:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case int:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case uint:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case int32:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case uint32:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case int16:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case uint16:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case int8:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case uint8:
				tag.Value = &pb.Tag_Int64{Int64: int64(v)}
			case float64:
				tag.Value = &pb.Tag_Double{Double: float64(v)}
			case float32:
				tag.Value = &pb.Tag_Double{Double: float64(v)}
			case bool:
				tag.Value = &pb.Tag_Bool{Bool: v}
			case []byte:
				tag.Value = &pb.Tag_Bytes{Bytes: v}
			case time.Duration:
				tag.Value = &pb.Tag_Duration{Duration: durationpb.New(v)}
			case time.Time:
				tag.Value = &pb.Tag_TimestampOffset{TimestampOffset: durationpb.New(v.Sub(op.startTime))}
			default:
				continue tagsLoop
			}
			v.Tags = append(v.Tags, &tag)
		}
	}

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

func (c *UDPClient) run(addr string) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	p := newOutgoingPacket(c.Application, c.Instance)

	send := func() {
		p.finalize()
		_ = c.send(p.buf.Bytes(), addr)
		p = newOutgoingPacket(c.Application, c.Instance)
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
		}
	}
}

func (c *UDPClient) send(packet []byte, addr string) error {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, laddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _, err = conn.WriteMsgUDP(packet, nil, nil)
	return err
}

func (c *UDPClient) QueueSend(event EventMap) {
	select {
	case c.submitQueue <- event:
	default:
	}
}
