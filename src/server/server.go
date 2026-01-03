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
	if config.PaxosMyID == 0 {
		value := "world"
		paxosServer.Write(context.Background(), "hello", &value)
		slog.Debug("Write completed, trying to read")
		slog.Debug("Read:", slog.String("value", *paxosServer.LinearizedRead(context.Background(), "hello")))
		paxosServer.Write(context.Background(), "hello", nil)
		slog.Debug("Delete completed, trying to read")
		receivedValue := paxosServer.LinearizedRead(context.Background(), "hello")
		if receivedValue != nil {
			slog.Debug("Deletion failed, found ", slog.String("value", *receivedValue))
		} else {
			slog.Debug("Deletion succeeded")
		}
	}
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
