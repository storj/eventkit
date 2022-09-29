package transport

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io/ioutil"
	"net"
	"sync"

	"github.com/jtolio/eventkit/pb"
	"google.golang.org/protobuf/proto"
)

// ListenUDP sets up a UDP server that receives packets containing events.
func ListenUDP(addr string) (*UDPListener, error) {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}

	return &UDPListener{
		addr: addr,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 10*1024)
			},
		},
		conn: conn,
	}, nil
}

// UDPListener handles reading packets from the underlying UDP connection.
type UDPListener struct {
	addr string
	pool sync.Pool
	conn *net.UDPConn
}

// Next returns the next packet from UDP and it's associated source address. Should an error occur, then it is returned.
// A source address may be returned alongside an error for further reporting in the event of abuse/debugging.
func (u *UDPListener) Next() (packet *pb.Packet, source *net.UDPAddr, err error) {
	buf := u.pool.Get().([]byte)
	defer func() {
		if buf != nil {
			u.pool.Put(buf)
		}
	}()

	n, source, err := u.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, nil, err
	}

	// TODO: handle malformed packet more gracefully... return source address for further reporting
	packet, err = parsePacket(n, buf)
	if err != nil {
		return nil, source, err
	}

	return packet, source, err
}

func parsePacket(n int, buf []byte) (*pb.Packet, error) {
	if n < 4 || string(buf[:2]) != "EK" {
		return nil, errors.New("missing magic number")
	}

	zl, err := zlib.NewReader(bytes.NewReader(buf[2:n]))
	if err != nil {
		return nil, err
	}

	defer func() { _ = zl.Close() }()

	buf, err = ioutil.ReadAll(zl)
	if err != nil {
		return nil, err
	}

	var data pb.Packet
	err = proto.Unmarshal(buf, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
