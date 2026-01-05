package paxos

import (
	context "context"
	"log/slog"
)

func (server *PaxosServerState) Read(ctx context.Context, key string) *DataEntry {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return server.data[key]
}

func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) *DataEntry {
	res, err := server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return &DataEntry{Value: res.Value, Revision: res.Revision}
}

func (server *PaxosServerState) Write(ctx context.Context, key string, value *string, revision *uint64) LocalWriteReply {
	res, err := server.peers[server.leader].WriteToLeader(ctx, &Action{Key: key, Value: value, Revision: revision})
	for err != nil {
		slog.Warn("Error writing to leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].WriteToLeader(ctx, &Action{Key: key, Value: value, Revision: revision})
	}
	return LocalWriteReply{Success: res.Success, Revision: res.Revision}
}

func (server *PaxosServerState) ListKeysLinearized(ctx context.Context, omitDeleted bool) []string {
	res, err := server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	}
	return res.Keys
}

func (server *PaxosServerState) ListKeys(ctx context.Context, omitDeleted bool) []string {
	return server.getKeys(omitDeleted)
}
