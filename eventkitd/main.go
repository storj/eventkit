package main

import (
	"bytes"
	"compress/zlib"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/netip"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/jtolio/eventkit"
	"github.com/jtolio/eventkit/pb"
)

var (
	flagAddr    = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers = flag.Int("workers", runtime.NumCPU(), "number of workers")
)

func eventToEventMap(event *pb.Event, instance string, source netip.AddrPort, received time.Time) eventkit.EventMap {
	em := make(eventkit.EventMap, len(event.Tags)+3)
	for _, tag := range event.Tags {
		if len(tag.Key) == 0 {
			continue
		}
		switch v := tag.GetValue().(type) {
		case *pb.Tag_String_:
			em[tag.Key] = v.String_
		case *pb.Tag_Int64:
			em[tag.Key] = v.Int64
		case *pb.Tag_Double:
			em[tag.Key] = v.Double
		case *pb.Tag_Bytes:
			em[tag.Key] = v.Bytes
		case *pb.Tag_Bool:
			em[tag.Key] = v.Bool
		case *pb.Tag_Duration:
			em[tag.Key] = v.Duration.AsDuration()
		case *pb.Tag_TimestampOffset:
			em[tag.Key] = received.Add(v.TimestampOffset.AsDuration())
		default:
			continue
		}
	}
	name := event.Name
	if name == "" {
		name = em["name"]
	}
	delete(em, "name")
	scope := event.Scope
	if event.Scope == nil {
		scope = em["scope"]
	}
	delete(em, "scope")
	if event.TimestampOffset != nil {
		em["timestamp"] = received.Add(event.TimestampOffset.AsDuration())
	}
	em["instance"] = instance
	em["packet_source"] =
	return em,
}

func handleParsedPacket(packet *pb.Packet, source netip.AddrPort, received time.Time) error {
	for _, event := range packet.Events {
		fmt.Println(eventToEventMap(event, received))
	}
	return nil
}

func handlePacket(packet UDPPacket) error {
	defer packet.Return()
	if packet.n < 4 || string(packet.buf[:2]) != "EK" {
		return errors.New("missing magic number")
	}
	zl, err := zlib.NewReader(bytes.NewReader(packet.buf[2:packet.n]))
	if err != nil {
		return err
	}
	buf, err := ioutil.ReadAll(zl)
	if err != nil {
		return err
	}
	err = zl.Close()
	if err != nil {
		return err
	}
	var data pb.Packet
	err = proto.Unmarshal(buf, &data)
	if err != nil {
		return err
	}
	return handleParsedPacket(&data, packet.source, packet.ts)
}

func main() {
	flag.Parse()
	queue := make(chan UDPPacket, *flagWorkers*2)
	source, err := ListenUDP(*flagAddr)
	if err != nil {
		panic(err)
	}

	var eg errgroup.Group
	for i := 0; i < *flagWorkers; i++ {
		eg.Go(func() error {
			for packet := range queue {
				err := handlePacket(packet)
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
	}

	for {
		packet, err := source.Next()
		if err != nil {
			close(queue)
			eg.Wait()
			panic(err)
		}
		queue <- packet
	}
}
