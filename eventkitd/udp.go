package main

import (
	"net"
	"sync"
	"time"
)

type UDPPacket struct {
	buf    []byte
	n      int
	source *net.UDPAddr
	ts     time.Time
	pool   *sync.Pool
}

func (p *UDPPacket) Return() {
	if p.buf != nil {
		p.n = 0
		p.pool.Put(p.buf)
		p.buf = nil
	}
}

type UDPSource struct {
	addr string
	pool sync.Pool
	conn *net.UDPConn
}

func ListenUDP(addr string) (*UDPSource, error) {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	return &UDPSource{
		addr: addr,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 10*1024)
			},
		},
		conn: conn,
	}, nil
}

func (u *UDPSource) Next() (res UDPPacket, err error) {
	buf := u.pool.Get().([]byte)
	n, source, err := u.conn.ReadFromUDP(buf)
	return UDPPacket{
		buf:    buf,
		n:      n,
		source: source,
		ts:     time.Now(),
		pool:   &u.pool,
	}, err
}

