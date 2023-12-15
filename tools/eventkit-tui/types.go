package main

import (
	"net"
	"strings"
	"time"

	"storj.io/eventkit/pb"
)

type Packet struct {
	Packet     *pb.Packet
	Source     *net.UDPAddr
	ReceivedAt time.Time
}

type Event struct {
	Event      *pb.Event
	Source     *net.UDPAddr
	ReceivedAt time.Time
}

func (e Event) ScopeStr() string {
	return strings.Join(e.Event.Scope, ".")
}
