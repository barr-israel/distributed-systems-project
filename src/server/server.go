package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"server/config"
	"server/etcd"
	"server/paxos"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	config.SetupConf()
	client := etcd.EtcdSetup()
	// RecoverData(&data)
	paxosServer := paxos.SetupGRPC(context.Background(), &client)
	StartHTTPServer(paxosServer)
	awaitInterrupt()
	slog.Info("Shutting down")
	client.Close()
	slog.Info("Shutdown complete")
}

func awaitInterrupt() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
