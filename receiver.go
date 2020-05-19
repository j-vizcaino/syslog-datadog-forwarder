package main

import (
	"net"
	"time"

	"github.com/influxdata/go-syslog/v3/rfc3164"
)

type Receiver struct {
	socket *net.UDPConn
}

func NewReceiver(port int) (*Receiver, error) {
	sock, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, err
	}
	return &Receiver{
		socket: sock,
	}, nil
}

func (r *Receiver) ReceiveMessages(out chan <- *rfc3164.SyslogMessage) {
	parser := rfc3164.NewParser()
	readBuffer := [1500]byte{}

	for {
		sizeBytes, err := r.socket.Read(readBuffer[:])
		fatalIf(err)

		datagram := readBuffer[:sizeBytes]
		generic, err := parser.Parse(datagram)
		fatalIf(err)
		msg := generic.(*rfc3164.SyslogMessage)
		now := time.Now()
		if msg.Timestamp == nil {
			msg.Timestamp = &now
		} else if msg.Timestamp.Year() == 0 {
			// Fix missing year from timestamp
			fixedTS := msg.Timestamp.AddDate(now.Year(), 0, 0)
			msg.Timestamp = &fixedTS
		}
		out <- msg
	}
}

func (r *Receiver) Cancel() {
	_ = r.socket.Close()
}