package main

import (
	"flag"
	"fmt"
	"net"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/jtolio/eventkit/eventkitd/private/path"
	"github.com/jtolio/eventkit/pb"
	"github.com/jtolio/eventkit/transport"
)

var (
	flagAddr    = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers = flag.Int("workers", runtime.NumCPU(), "number of workers")
	flagPath    = flag.String("base-path", "./data/", "path to write to")
)

func eventToRecord(packet *pb.Packet, event *pb.Event, source *net.UDPAddr, received time.Time) (rv *pb.Record, recordPath string) {
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

	return &record, path.Compute(*flagPath, eventTime, event.Scope, event.Name)
}

func handleParsedPacket(writer *Writer, packet *pb.Packet, source *net.UDPAddr, received time.Time) error {
	for _, event := range packet.Events {
		record, path := eventToRecord(packet, event, source, received)
		err := writer.Append(path, record)
		if err != nil {
			return err
		}
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
	writer := NewWriter()

	go func() {
		t := time.NewTicker(15 * time.Second)
		for range t.C {
			writer.DropAll()
		}
	}()

	var eg errgroup.Group
	for i := 0; i < *flagWorkers; i++ {
		eg.Go(func() error {
			for packet := range queue {
				err := handleParsedPacket(writer, packet.Packet, packet.Source, packet.ReceivedAt)
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
			_ = eg.Wait()
			panic(err)
		}

		queue <- &Packet{
			Packet:     packet,
			Source:     source,
			ReceivedAt: time.Now(),
		}
	}
}
