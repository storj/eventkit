package listener

import (
	"context"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/jtolio/eventkit/pb"
	"github.com/jtolio/eventkit/transport"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type Handler func(ctx context.Context, unparsed *Packet, packet *pb.Packet) error

func ProcessPackages(workers int, PCAPIface string, address string, handler Handler) {
	log, _ := zap.NewProduction()

	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	queue := make(chan *Packet, workers)
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case unparsed := <-queue:
					packet, err := transport.ParsePacket(unparsed.Payload)
					if err != nil {
						fmt.Println(err)
						continue
					}
					err = handler(ctx, unparsed, packet)
					if err != nil {
						fmt.Println(err)
						continue
					}
				}
			}
		})
	}
	eg.Go(func() error {
		sigusr := make(chan os.Signal)
		signal.Notify(sigusr, syscall.SIGUSR1)
		defer done()

		for {
			select {
			// sigterm
			case <-ctx.Done():
				return nil
			case <-sigusr:
				buf := make([]byte, 10240)
				for {
					n := runtime.Stack(buf, true)
					if n < len(buf) {
						fmt.Println(string(buf))
						break
					}
					buf = make([]byte, 2*len(buf))
				}
			}

		}
	})

	if PCAPIface != "" {
		handle, supported, err := NewEthernetHandle(PCAPIface)
		if err != nil {
			panic(err)
		}
		if supported {
			addr, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				panic(err)
			}

			src := gopacket.NewPacketSource(handle, layers.LinkTypeEthernet)

			noWait := &errgroup.Group{}
			noWait.Go(func() error {
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
			})

		}
	} else {
		listener, err := transport.ListenUDP(address)
		if err != nil {
			panic(err)
		}

		noWait := &errgroup.Group{}
		noWait.Go(func() error {
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
		})
	}
	<-ctx.Done()
	log.Info("shutting down")

	err := eg.Wait()
	if err != nil {
		log.Error("encountered error during shutdown", zap.Error(err))
		os.Exit(1)
		return
	}
}
