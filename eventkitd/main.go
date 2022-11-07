package main

import (
	"flag"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"golang.org/x/sync/errgroup"

	"github.com/jtolio/eventkit/eventkitd/private/path"
	"github.com/jtolio/eventkit/pb"
	"github.com/jtolio/eventkit/transport"
)

var (
	flagAddr      = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers   = flag.Int("workers", runtime.NumCPU(), "number of workers")
	flagPath      = flag.String("base-path", "./data/", "path to write to")
	flagPCAPIface = flag.String("pcap-iface", "", "if set, use pcap for udp packets on this interface. must be on linux")
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
	Payload    []byte
	Source     *net.UDPAddr
	ReceivedAt time.Time
}

func main() {
	flag.Parse()
	queue := make(chan *Packet, *flagWorkers*2)
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
			for unparsed := range queue {
				packet, err := transport.ParsePacket(unparsed.Payload)
				if err != nil {
					fmt.Println(err)
					continue
				}
				err = handleParsedPacket(writer, packet, unparsed.Source, unparsed.ReceivedAt)
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
	}

	if *flagPCAPIface != "" {
		handle, supported, err := NewEthernetHandle(*flagPCAPIface)
		if err != nil {
			panic(err)
		}
		if supported {
			addr, err := net.ResolveUDPAddr("udp", *flagAddr)
			if err != nil {
				panic(err)
			}

			src := gopacket.NewPacketSource(handle, layers.LinkTypeEthernet)
			for {
				packet, err := src.NextPacket()
				if err != nil {
					close(queue)
					_ = eg.Wait()
					panic(err)
				}
				udp, _ := packet.Layer(layers.LayerTypeUDP).(*layers.UDP)
				if udp == nil || int(udp.DstPort) != addr.Port {
					continue
				}

				source := net.UDPAddr{Port: addr.Port}

				if ip4, _ := packet.Layer(layers.LayerTypeIPv4).(*layers.IPv4); ip4 != nil {
					source.IP = ip4.SrcIP
				} else if ip6, _ := packet.Layer(layers.LayerTypeIPv6).(*layers.IPv6); ip6 != nil {
					source.IP = ip6.SrcIP
				} else {
					continue
				}

				queue <- &Packet{
					Payload:    udp.Payload,
					Source:     &source,
					ReceivedAt: time.Now(),
				}
			}
		}
	}

	listener, err := transport.ListenUDP(*flagAddr)
	if err != nil {
		panic(err)
	}
	for {
		payload, source, err := listener.Next()
		if err != nil {
			close(queue)
			_ = eg.Wait()
			panic(err)
		}

		queue <- &Packet{
			Payload:    payload,
			Source:     source,
			ReceivedAt: time.Now(),
		}
	}
}
