package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/influxdata/go-syslog/v3/rfc3164"
)

const LogsIntakeURL = "https://http-intake.logs.datadoghq.com/v1/input"

func fatalIf(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	const ListenPort = 10119
	recv, err := NewReceiver(ListenPort)
	fatalIf(err)
	fmt.Println("Waiting for syslog UDP messages on port ", ListenPort)

	rawMessageChan := make(chan *rfc3164.SyslogMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())

	publisher := NewPublisher(os.Getenv("DD_API_KEY"), LogsIntakeURL)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		recv.ReceiveMessages(rawMessageChan)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		TransformMessages(ctx, rawMessageChan, publisher)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		publisher.Run(ctx)
		wg.Done()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<- sigChan

	cancel()
	_ = recv.socket.Close()
	wg.Wait()
}