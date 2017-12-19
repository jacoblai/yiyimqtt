package main

import (
	"strconv"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"transport"
	"broker"
)

func main() {
	wsMsgPort := 9019
	wshttpServer, err := transport.NewWebSocketServer(":" + strconv.Itoa(wsMsgPort))
	if err != nil {
		panic(err)
	}
	tcpMsgPort := 9020
	TcpServer, err := transport.Launch("tcp://:" + strconv.Itoa(tcpMsgPort))
	if err != nil {
		panic(err)
	}
	engine := broker.NewEngine()
	engine.Accept(wshttpServer)
	engine.Accept(TcpServer)

	fmt.Println("yiyimqtt ws port", strconv.Itoa(wsMsgPort))
	fmt.Println("yiyimqtt tcp port", strconv.Itoa(tcpMsgPort))

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
