package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
)

func main() {
	setupConf()
	var wg sync.WaitGroup
	cli, kaCancel := EtcdSetup(&wg)
	data := map[string]string{}
	RecoverData(&data)
	awaitInterrupt()
	log.Println("Shutting down")
	kaCancel()
	wg.Wait()
	err := cli.Close()
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
