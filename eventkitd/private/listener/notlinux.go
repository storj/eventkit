//go:build !linux
// +build !linux

package listener

import "github.com/google/gopacket"

func NewEthernetHandle(iface string) (_ gopacket.PacketDataSource, supported bool, err error) {
	return nil, false, nil
}
