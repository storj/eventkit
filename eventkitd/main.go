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

	"olio.lol/eventkit/pb"
)

var (
	flagAddr    = flag.String("addr", ":9002", "udp address to listen on")
	flagWorkers = flag.Int("workers", runtime.NumCPU(), "number of workers")
)

func handleParsedPacket(packet *pb.Packet, source netip.AddrPort, received time.Time) error {
	_, err := fmt.Println(packet, source, received)
	return err
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
