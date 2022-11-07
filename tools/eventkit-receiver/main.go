package main

import (
	"context"
	"log"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	ui "github.com/elek/bubbles"
	"github.com/jtolio/eventkit/transport"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	c := cobra.Command{
		Use:   "eventkit-receiver",
		Short: "Interactive eventkit message receiver command line application",
	}
	listenAddress := c.Flags().StringP("listen", "l", "localhost:9000", "UDP host:port for receiving messages")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return run(*listenAddress)
	}

	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func run(address string) error {

	app := tea.NewProgram(ui.NewKillable(NewPanel()))

	queue := make(chan *Packet)
	listener, err := transport.ListenUDP(address)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	var eg errgroup.Group

	eg.Go(func() error {
		for {
			select {
			case msg := <-queue:
				app.Send(msg)
			case <-ctx.Done():
				return nil
			}

		}
	})

	eg.Go(func() error {
		for {
			payload, source, err := listener.Next()
			if err != nil {
				return errs.Wrap(err)
			}
			packet, err := transport.ParsePacket(payload)
			if err != nil {
				return errs.Wrap(err)
			}

			queue <- &Packet{
				Packet:     packet,
				Source:     source,
				ReceivedAt: time.Now(),
			}
		}
	})

	eg.Go(func() error {
		err = app.Start()
		_ = listener.Close()
		cancel()
		return err
	})
	return eg.Wait()
}
