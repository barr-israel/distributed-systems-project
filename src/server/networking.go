package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
)

type PaxosServerState struct {
	UnimplementedPaxosServer
	ongoingPaxos map[uint64]Paxos
}

func (self *PaxosServerState) Accept(ctx context.Context, msg *AcceptMessage) (*AcceptedMessage, error) {
	reply := AcceptedMessage{Round: msg.Round}
	return &reply, nil
}

func (self *PaxosServerState) Prepare(ctx context.Context, msg *PrepareMessage) (*PromiseMessage, error) {
	reply := PromiseMessage{Round: msg.Round}

	return &reply, nil
}

func setupgRPC() {
	listen, err := net.Listen("tcp", GRPC_LISTEN_ADDRESS)
	if err != nil {
		log.Panicln("error listening")
	}
	grpcServer := grpc.NewServer()
	RegisterPaxosServer(grpcServer, newPaxosServerState())
	err = grpcServer.Serve(listen)
	if err != nil {
		log.Panicln("error serving grpc server")
	}
}

func newPaxosServerState() *PaxosServerState {
	return &PaxosServerState{}
}
