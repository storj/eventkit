package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"runtime"
	"time"

	"github.com/klauspost/compress/zlib"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jtolio/eventkit/pb"
)

var (
	flagAddr    = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers = flag.Int("workers", runtime.NumCPU(), "number of workers")
)

func eventToRecord(packet *pb.Packet, event *pb.Event, source *net.UDPAddr, received time.Time) (rv *pb.Record, path string) {
	var record pb.Record
	record.Instance = packet.Instance
	record.Tags = event.Tags
	record.SourceAddr = source.String()

	// the event timestamp and the packet send timestamp are offsets from the packet's
	// start_timestamp, which is determined from the sender's system clock. the
	// sender's system clock may potentially be way off. but, we know the time we received the
	// packet. if we (falsely) assume that the packet was received instantaneously,
	// we can correct all event timestamps to be against the same clock, even if the sender's
	// clock is years off. this allows for more comparable times across events from across
	// clients with different clocks, even if it means we lose the packet transit time
	// (which might be highly variable!).
	//
	//  start_time   event_time    send_time/received (assumed instaneous)
	//       |            |                |
	//

	correctedStart := received.Add(-packet.SendOffset.AsDuration())
	eventTime := correctedStart.Add(event.TimestampOffset.AsDuration())
	record.Timestamp = timestamppb.New(eventTime)
	record.TimestampCorrection = durationpb.New(correctedStart.Sub(packet.StartTimestamp.AsTime()))

	return &record, computePath(eventTime, packet.Application, event.Scope, event.Name)
}

func handleParsedPacket(packet *pb.Packet, source *net.UDPAddr, received time.Time) error {
	for _, event := range packet.Events {
		record, path := eventToRecord(packet, event, source, received)
		_ = record
		_ = path
		// TODO
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

