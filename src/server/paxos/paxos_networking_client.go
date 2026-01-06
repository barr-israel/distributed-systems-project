package paxos

import (
	context "context"
	"log/slog"
)

// ReadRevision returns the revision of a given key
// This function is sequentially consistent
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

// LinearizedReadRevision returns the revision of a given key
// This function is linearized by contacting the leader, which serializes all of its incoming requests
func (server *PaxosServerState) LinearizedReadRevision(ctx context.Context, key string) uint64 {
	res, err := server.peers[server.leader].ReadRevisionFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadRevisionFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return res.Revision
}

// Read returns the value and revision of a given key
// This function is sequentially consistent
func (server *PaxosServerState) Read(ctx context.Context, key string) *DataEntry {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return server.data[key]
}

// LinearizedRead returns the value and revision of a given key
// This function is linearized by contacting the leader, which serializes all of its incoming requests
func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) *DataEntry {
	res, err := server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return &DataEntry{Value: res.Value, Revision: res.Revision}
}

// Write conditionally writes a key-value pair into the database
// If a revision is supplied, the write will only be applied if it is equal to the current revision
// This function is linearized by contacting the leader, which serializes all of its incoming requests
func (server *PaxosServerState) Write(ctx context.Context, key string, value *string, revision *uint64) LocalWriteReply {
	res, err := server.peers[server.leader].WriteToLeader(ctx, &Action{Key: key, Value: value, Revision: revision})
	for err != nil {
		slog.Warn("Error writing to leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].WriteToLeader(ctx, &Action{Key: key, Value: value, Revision: revision})
	}
	return LocalWriteReply{Success: res.Success, Revision: res.Revision}
}

// LinearizedListKeys returns a list of all the keys and their revisions
// This function is linearized by contacting the leader, which serializes all of its incoming requests
func (server *PaxosServerState) LinearizedListKeys(ctx context.Context, omitDeleted bool) []*KeyRev {
	res, err := server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadListFromLeader(ctx, &ListRequest{OmitDeleted: omitDeleted})
	}
	return res.Keyrevs
}

// ListKeys returns a list of all the keys and their revisions
// This function is sequentially consistent
func (server *PaxosServerState) ListKeys(ctx context.Context, omitDeleted bool) []*KeyRev {
	return server.getKeys(omitDeleted)
}
