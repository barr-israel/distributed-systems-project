package paxos

import (
	context "context"
	"log/slog"
)

func (server *PaxosServerState) ReadRevision(ctx context.Context, key string) uint64 {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	entry, contains := server.data[key]
	if contains {
		return entry.Revision
	} else {
		return 0
	}
}

func (server *PaxosServerState) LinearizedReadRevision(ctx context.Context, key string) uint64 {
	res, err := server.peers[server.leader].ReadRevisionFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadRevisionFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return res.Revision
}

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

func (server *PaxosServerState) ListKeysLinearized(ctx context.Context, omitDeleted bool) []*KeyRev {
	res, err := server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	}
	return res.Keyrevs
}

func (server *PaxosServerState) ListKeys(ctx context.Context, omitDeleted bool) []*KeyRev {
	return server.getKeys(omitDeleted)
}
