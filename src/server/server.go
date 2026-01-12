package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"server/cluster"
	"server/config"
	"server/etcd"
	"server/httpserver"
)

func main() {
	config.SetupConf()
	if config.Verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	} else {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}
	slog.Info("Server starting", slog.Uint64("Peer ID", config.MyPeerID))
	client := etcd.EtcdSetup()
	paxosServer := cluster.SetupGRPC(context.Background(), &client)
	httpserver.StartHTTPServer(paxosServer)
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
