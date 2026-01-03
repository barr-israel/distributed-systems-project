package paxos

import (
	context "context"
	"log/slog"
)

func (server *PaxosServerState) Read(ctx context.Context, key string) *string {
	return server.data[key]
}

func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) *string {
	res, err := server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return res.Value
}

func (server *PaxosServerState) AsyncWrite(ctx context.Context, key string, value *string) {
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, replyChannel: nil}
	server.notifyLeader(ctx)
}

func (server *PaxosServerState) Write(ctx context.Context, key string, value *string) {
	replyChannel := make(chan byte)
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, replyChannel: replyChannel}
	server.notifyLeader(ctx)
	<-replyChannel
}

func (server *PaxosServerState) notifyLeader(ctx context.Context) {
	req := InitiatiationRequest{NextPaxosId: server.maxPaxosID + 1}
	_, err := server.peers[server.leader].InitiateRound(ctx, &req)
	for err != nil {
		slog.Warn("Error notifying leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		_, err = server.peers[server.leader].InitiateRound(ctx, &req)
	}
}
