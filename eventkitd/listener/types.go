package listener

import (
	"net"
	"time"
)

type Packet struct {
	Payload    []byte
	Source     *net.UDPAddr
	ReceivedAt time.Time
}
