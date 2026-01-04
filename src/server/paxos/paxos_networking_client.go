package paxos

import (
	context "context"
	"log/slog"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *PaxosServerState) Read(ctx context.Context, key string) *DataEntry {
	return server.data[key]
}

func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) *DataEntry {
	res, err := server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	if res.Value != nil {
		return &DataEntry{Value: res.Value, Revision: *res.Revision}
	} else {
		return nil
	}
}

func (server *PaxosServerState) AsyncWrite(ctx context.Context, key string, value *string, revision *uint64) {
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, revision: revision, replyChannel: nil}
	server.notifyLeader(ctx)
}

func (server *PaxosServerState) Write(ctx context.Context, key string, value *string, revision *uint64) uint64 {
	replyChannel := make(chan uint64)
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, revision: revision, replyChannel: replyChannel}
	server.notifyLeader(ctx)
	return <-replyChannel
}

func (server *PaxosServerState) ListKeysLinearized(ctx context.Context) []string {
	res, err := server.peers[server.leader].ReadListFromLeader(ctx, &emptypb.Empty{})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadListFromLeader(ctx, &emptypb.Empty{})
	}
	return res.Keys
}

func (server *PaxosServerState) ListKeys(ctx context.Context) []string {
	return server.getKeys()
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
