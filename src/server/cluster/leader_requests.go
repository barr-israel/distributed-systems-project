package cluster

/*
* The leader side of the client API requests that are passed to the leader
 */

import (
	context "context"
	"errors"

	"server/config"
)

// WriteToLeader performs a linearized conditional write into the database
func (server *ServerState) WriteToLeader(_ context.Context, msg *Action) (*WriteReply, error) {
	replyChannel := make(chan LocalWriteReply, 1)
	request := LocalWriteRequest{key: msg.Key, value: msg.Value, revision: msg.Revision, replyChannel: replyChannel}
	server.incomingRequests <- &request
	reply := <-replyChannel
	return &WriteReply{Success: reply.Success, Revision: reply.Revision}, nil
}

// ReadRevisionFromLeader performs a linearized read of a revision of a given key
func (server *ServerState) ReadRevisionFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadRevisionReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leaderPeerID != config.MyPeerID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	entry := server.data[msg.Key]
	if entry != nil {
		return &ReadRevisionReply{Revision: entry.Revision}, nil
	} else {
		return &ReadRevisionReply{Revision: 0}, nil
	}
}

// ReadFromLeader performs a linearized read of a value and revision of a given key
func (server *ServerState) ReadFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leaderPeerID != config.MyPeerID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	entry := server.data[msg.Key]
	if entry != nil {
		return &ReadReply{Value: entry.Value, Revision: entry.Revision}, nil
	} else {
		return &ReadReply{Value: nil, Revision: 0}, nil
	}
}

// ReadListFromLeader performs a linearized read of all the keys and their revisions
func (server *ServerState) ReadListFromLeader(_ context.Context, msg *ListRequest) (*KeyRevsList, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leaderPeerID != config.MyPeerID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return &KeyRevsList{Keyrevs: server.getKeys(msg.OmitDeleted)}, nil
}
