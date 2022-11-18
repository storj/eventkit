package main

import (
	"context"
	"fmt"
	"github.com/charmbracelet/lipgloss"
	"github.com/jtolio/eventkit/eventkitd/listener"
	"github.com/jtolio/eventkit/pb"
	"github.com/spf13/cobra"
	"log"
	"strings"
	"time"
)

var (
	Green  = lipgloss.Color("#01a252")
	Yellow = lipgloss.Color("#fded02")
	Red    = lipgloss.Color("#db2d20")
)

func main() {
	c := cobra.Command{
		Use:   "eventkit-receiver",
		Short: "Simple command line evenkit receiver",
	}
	listenAddress := c.Flags().StringP("listen", "l", "localhost:9002", "UDP host:port for receiving messages")
	flagPCAPIface := c.Flags().StringP("pcap-iface", "i", "", "if set, use pcap for udp packets on this interface. must be on linux")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		return run(*listenAddress, *flagPCAPIface)
	}

	err := c.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func run(address string, iface string) error {
	listener.ProcessPackages(10, iface, address, func(ctx context.Context, unparsed *listener.Packet, packet *pb.Packet) error {
		for _, event := range packet.Events {
			var tags []string
			for _, v := range event.Tags {
				tags = append(tags, fmt.Sprintf("%s=%s", v.Key, lipgloss.NewStyle().Foreground(Yellow).Render(v.ValueString())))
			}
			fmt.Printf("%s %s %s %s %s %s\n",
				lipgloss.NewStyle().Foreground(Red).Render(unparsed.ReceivedAt.Format(time.RFC3339)),
				packet.Application,
				packet.Instance,
				event.Scope,
				lipgloss.NewStyle().Foreground(Green).Render(strings.Join(event.Scope, ".")),
				strings.Join(tags, " "))
		}
		return nil
	})
	return nil
}
