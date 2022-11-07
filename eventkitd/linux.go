//go:build linux
// +build linux

package main

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
)

func NewEthernetHandle(iface string) (_ gopacket.PacketDataSource, supported bool, err error) {
	source, err := pcapgo.NewEthernetHandle(iface)
	return source, true, err
}
