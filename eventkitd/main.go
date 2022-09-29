package main

import (
	"flag"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/jtolio/eventkit/transport"
	"golang.org/x/sync/errgroup"

	"github.com/jtolio/eventkit/pb"
)

var (
	flagAddr    = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers = flag.Int("workers", runtime.NumCPU(), "number of workers")
)

func eventToRecord(packet *pb.Packet, event *pb.Event, source *net.UDPAddr, received time.Time) (rv *pb.Record, path string) {
	var record pb.Record
	record.Application = packet.Application
	record.ApplicationVersion = packet.ApplicationVersion
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

	correctedStart := received.Add(-time.Duration(packet.SendOffsetNs))
	eventTime := correctedStart.Add(time.Duration(event.TimestampOffsetNs))
	record.Timestamp = pb.AsTimestamp(eventTime)
	record.TimestampCorrectionNs = int64(correctedStart.Sub(packet.StartTimestamp.AsTime()))

	return &record, computePath(eventTime, event.Scope, event.Name)
}

func handleParsedPacket(packet *pb.Packet, source *net.UDPAddr, received time.Time) error {
	for _, event := range packet.Events {
		record, path := eventToRecord(packet, event, source, received)
		_ = record
		_ = path
		// TODO
		fmt.Println(path)
	}
	return nil
}

type Packet struct {
	Packet     *pb.Packet
	Source     *net.UDPAddr
	ReceivedAt time.Time
}

func main() {
	flag.Parse()
	queue := make(chan *Packet, *flagWorkers*2)
	listener, err := transport.ListenUDP(*flagAddr)
	if err != nil {
		panic(err)
	}

	var eg errgroup.Group
	for i := 0; i < *flagWorkers; i++ {
		eg.Go(func() error {
			for packet := range queue {
				err := handleParsedPacket(packet.Packet, packet.Source, packet.ReceivedAt)
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
	}

	for {
		packet, source, err := listener.Next()
		if err != nil {
			close(queue)
			eg.Wait()
			panic(err)
		}

		queue <- &Packet{
			Packet:     packet,
			Source:     source,
			ReceivedAt: time.Now(),
		}
	}
}
