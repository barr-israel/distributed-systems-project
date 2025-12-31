package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
)

func main() {
	setupConf()
	var wg sync.WaitGroup
	client := EtcdSetup(&wg)
	// RecoverData(&data)
	client.RefreshLeader(context.Background())
	server, reads, writes, commited := paxos.SetupgRPC(context.Background(), &client)
	awaitInterrupt()
	log.Println("Shutting down")
	client.keepaliveCancel()
	wg.Wait()
	err := client.client.Close()
	if err != nil {
		log.Panicln("Error closing etcd client")
	}
	log.Println("Shutdown complete")
}

func awaitInterrupt() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
