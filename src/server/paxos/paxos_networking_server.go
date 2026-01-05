/*
Package paxos is responsible for communication between the servers, using the paxos algorithm to order incoming requests
*/
package paxos

import (
	"context"
	"log/slog"
	"time"

	"server/config"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *PaxosServerState) getOrCreatePaxosInstance(paxosID uint64) *PaxosInstance {
	instance, contains := server.ongoingPaxos[paxosID]
	if !contains {
		slog.Info("Starting new paxos", slog.Uint64("Paxos ID", paxosID))
		instance = NewPaxosInstance()
		server.ongoingPaxos[paxosID] = instance
		server.maxPaxosID = max(server.maxPaxosID, paxosID)
	}
	return instance
}

func (server *PaxosServerState) Close() {
	for _, conn := range server.connections {
		err := conn.Close()
		if err != nil {
			slog.Error("Error disconnecting from peer")
		}
	}
}

func (server *PaxosServerState) sendAccepted(peerID uint64, msg *AcceptedMessage) {
	slog.Debug("Sending accepted", slog.Uint64("Peer ID", uint64(peerID)), slog.String("msg", msg.String()))
	peer := server.peers[peerID]
	_, err := peer.Accepted(context.Background(), msg)
	for err != nil {
		slog.Error("Error sending accepted to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetry * time.Millisecond)
		_, err = peer.Accepted(context.Background(), msg)
	}
	slog.Debug("Received accepted response", slog.Uint64("PeerID", uint64(peerID)))
}

func (server *PaxosServerState) sendCommitedIDRequest(peerID uint64, responses chan<- uint64) {
	slog.Debug("Sending min ID request", slog.Uint64("Peer ID", uint64(peerID)))
	peer := server.peers[peerID]
	res, err := peer.GetCommitedPaxosID(server.ctx, &emptypb.Empty{})
	for err != nil {
		slog.Error("Error sending min ID request to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetry * time.Millisecond)
		res, err = peer.GetCommitedPaxosID(server.ctx, &emptypb.Empty{})
	}
	slog.Debug("Received min ID request response", slog.Uint64("PeerID", uint64(peerID)), slog.String("response", res.String()))
	responses <- res.CommitedPaxosId
}

func (server *PaxosServerState) Prepare(ctx context.Context, msg *PrepareMessage) (*PromiseMessage, error) {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	slog.Debug("Received prepare", slog.String("msg", msg.String()))
	if server.minPaxosID > msg.PaxosId {
		// the leader will eventually realize it is an old Paxos ID
		return &PromiseMessage{LastGoodRound: 0, Ack: false, Proposal: nil}, nil
	}
	res := server.getOrCreatePaxosInstance(msg.PaxosId).Prepare(msg)
	slog.Debug("Responding", slog.String("response", res.String()))
	return res, nil
}

func (server *PaxosServerState) Accept(ctx context.Context, msg *AcceptMessage) (*AcceptResponse, error) {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	slog.Debug("Received accept", slog.String("msg", msg.String()))
	if server.minPaxosID > msg.PaxosId {
		// the leader will eventually realize it is an old Paxos ID
		return &AcceptResponse{Ack: false}, nil
	}
	response, ack := server.getOrCreatePaxosInstance(msg.PaxosId).Accept(msg)
	if ack {
		for peerID := range config.PaxosMemberCount {
			go server.sendAccepted(peerID, response)
		}
	}
	res := &AcceptResponse{Ack: ack}
	slog.Debug("Responding", slog.String("response", res.String()))
	return res, nil
}

func (server *PaxosServerState) Accepted(ctx context.Context, msg *AcceptedMessage) (*emptypb.Empty, error) {
	// TODO: add proposal fill-in
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	slog.Debug("Received accepted", slog.String("msg", msg.String()))
	if server.minPaxosID > msg.PaxosId {
		return nil, nil
	}
	p := server.getOrCreatePaxosInstance(msg.PaxosId)
	commit := p.Accepted(msg)
	if commit {
		server.tryCommitPaxos()
	}
	return &emptypb.Empty{}, nil
}

func (server *PaxosServerState) GetCommitedPaxosID(ctx context.Context, _ *emptypb.Empty) (*CommitedPaxosID, error) {
	return &CommitedPaxosID{CommitedPaxosId: server.commitedPaxosID}, nil
}

func (server *PaxosServerState) tryCommitPaxos() {
	paxos := server.ongoingPaxos[server.commitedPaxosID+1]
	for paxos != nil && paxos.decided {
		server.commitedPaxosID++
		server.commitActions(*paxos.preference, server.commitedPaxosID)
		slog.Info("Commited paxos:", slog.Uint64("paxosID", server.commitedPaxosID), slog.Any("actions", paxos.proposals))
		if paxos.done {
			server.deletePaxos(server.commitedPaxosID)
		}
		paxos = server.ongoingPaxos[server.commitedPaxosID+1]
	}
	server.commitCond.Broadcast()
	server.cleanUpOldPaxos()
}

func (server *PaxosServerState) cleanUpOldPaxos() {
	// minPaxosID will be 1 above commitedPaxosID if there are no old instances
	if 1+server.commitedPaxosID-server.minPaxosID > config.PaxosCleanupThreshold {
		responses := make(chan uint64, config.PaxosMemberCount)
		for peerID := range config.PaxosMemberCount {
			go server.sendCommitedIDRequest(peerID, responses)
		}
		maxCommitedID := uint64(0)
		for range config.PaxosMemberCount {
			commitedID := <-responses
			maxCommitedID = max(maxCommitedID, commitedID)
		}
		for paxosID := server.minPaxosID; paxosID <= maxCommitedID; paxosID++ {
			server.deletePaxos(paxosID)
		}
		server.minPaxosID = maxCommitedID + 1
	}
}

func (server *PaxosServerState) commitActions(actionLocal []ActionLocal, paxosID uint64) {
	activeRequestsCount := len(server.activeWrites)
	for _, action := range actionLocal {
		entry, exists := server.data[action.key]
		slog.Debug("Commiting action with", slog.String("key", action.key))
		success := false
		// an action will applied into the database in 1 of 3 cases:
		// 1. the action did not request a revision check
		// 2. the action requested a revision check and it passes
		// 3. the action requested a revision check of 0, which means "only if doesnt exist" and the key doesnt exist
		if action.revision == nil || (exists && entry.Revision == *action.revision) || (!exists && *action.revision == 0) {
			success = true
			server.data[action.key] = &DataEntry{Value: action.value, Revision: paxosID}
		}
		// reply to active writes, only leader has active writes
		if server.leader == config.PaxosMyID {
			for i := 0; i < activeRequestsCount; i++ {
				request := server.activeWrites[i]
				// checking just the key is sufficient because any group of writes can be commited by commiting one of them and pretending the rest were commited just before it
				// however, when revision dependant updates are involved, we can only do this if the request is already stale
				if action.key == request.key &&
					((action.revision == nil && request.revision == nil) || (*action.revision == *request.revision)) &&
					((action.value == nil && request.value == nil) || (*action.value == *request.value)) {
					entry, exists := server.data[request.key]
					if exists {
						request.replyChannel <- LocalWriteReply{Success: success, Revision: entry.Revision}
					} else {
						request.replyChannel <- LocalWriteReply{Success: success, Revision: 0}
					}
					if action.value != nil {
						slog.Info("Commited write", slog.String("key", action.key), slog.String("value", *action.value))
					} else {
						slog.Info("Commited delete", slog.String("key", action.key))
					}
					close(request.replyChannel)
					// remove request from active requests
					server.activeWrites[i] = server.activeWrites[activeRequestsCount-1]
					activeRequestsCount--
				}
			}
		}
	}
	// only keep unfulfilled requests
	server.activeWrites = server.activeWrites[:activeRequestsCount]
	slog.Debug("remaining", slog.Int("requests", len(server.activeWrites)))
}

func (server *PaxosServerState) deletePaxos(paxosID uint64) {
	slog.Debug("Deleting paxos", slog.Uint64("paxos ID", paxosID))
	delete(server.ongoingPaxos, paxosID)
	server.minPaxosID = min(server.minPaxosID, paxosID+1)
}
