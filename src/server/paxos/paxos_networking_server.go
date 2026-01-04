/*
Package paxos is responsible for communication between the servers, using the paxos algorithm to order incoming requests
*/
package paxos

import (
	"context"
	"log"
	"log/slog"
	"time"

	"server/config"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *PaxosServerState) fillActiveRequests() {
	for len(server.incomingRequests) > 0 && len(server.activeWrites) < int(config.PaxosMaxReqPerRound) {
		req := <-server.incomingRequests
		server.activeWrites = append(server.activeWrites, &req)
	}
}

func (server *PaxosServerState) getOrCreatePaxosInstance(paxosID uint64) *PaxosInstance {
	instance, contains := server.ongoingPaxos[paxosID]
	if !contains {
		slog.Info("Starting new paxos", slog.Uint64("Paxos ID", paxosID))
		server.fillActiveRequests()
		instance = NewPaxosInstance(server.activeWrites)
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
		return nil, nil
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
		return nil, nil
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
	} else if p.IsDone() {
		server.deletePaxos(msg.PaxosId)
	}
	return &emptypb.Empty{}, nil
}

func (server *PaxosServerState) GetCommitedPaxosID(ctx context.Context, _ *emptypb.Empty) (*CommitedPaxosID, error) {
	return &CommitedPaxosID{CommitedPaxosId: server.commitedPaxosID}, nil
}

func (server *PaxosServerState) tryCommitPaxos() {
	paxos := server.ongoingPaxos[server.commitedPaxosID+1]
	for paxos != nil && paxos.decided {
		server.commitActions(paxos.proposal)
		server.commitedPaxosID++
		slog.Debug("Commited paxos:", slog.Uint64("paxosID", server.commitedPaxosID), slog.Any("actions", paxos.proposal))
		if paxos.IsDone() {
			server.deletePaxos(server.commitedPaxosID)
		}
		paxos = server.ongoingPaxos[server.commitedPaxosID+1]
	}
	if len(server.activeWrites) != 0 {
		// not all writes were commited, need another round
		go server.notifyLeader(context.Background())
	}
	server.cleanUpOldPaxos()
}

func (server *PaxosServerState) cleanUpOldPaxos() {
	if server.commitedPaxosID-server.minPaxosID < config.PaxosCleanupThreshold {
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
	}
}

func (server *PaxosServerState) commitActions(actionLocal []ActionLocal) {
	activeRequestsCount := len(server.activeWrites)
	for _, action := range actionLocal {
		entry, exists := server.data[action.key]
		if action.revision == nil {
			// no revision check requested, update or delete normally
			if action.value != nil {
				revision := uint64(1)
				if exists {
					revision = entry.Revision + uint64(1)
				}
				server.data[action.key] = &DataEntry{Value: action.value, Revision: revision}
			} else {
				delete(server.data, action.key)
			}
		} else if exists && *action.revision == entry.Revision {
			// revision check requested, so the entry must exist with the given revision
			if action.value != nil {
				server.data[action.key] = &DataEntry{Value: action.value, Revision: entry.Revision + 1}
			} else {
				delete(server.data, action.key)
			}
		} else if *action.revision == 0 {
			// revision specifically requested to write if doesnt exist
			if action.value != nil {
				server.data[action.key] = &DataEntry{Value: action.value, Revision: 1}
			}
		}
		for i := 0; i < activeRequestsCount; i++ {
			request := server.activeWrites[i]
			// checking just the key is sufficient because any group of writes can be commited by commiting one of them and pretending the rest were commited just before it
			// however, when revision dependant updates are involved, we can only do this if the request is already stale
			if action.key == request.key && (request.revision == nil || action.revision == nil || *request.revision <= *action.revision) {
				if action.value != nil {
					entry, contains := server.data[request.key]
					if contains {
						request.replyChannel <- entry.Revision
					} else {
						request.replyChannel <- 0
					}
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
	// only keep unfulfilled requests
	server.activeWrites = server.activeWrites[:activeRequestsCount]
	log.Println("remaining requests: ", server.activeWrites)
}

func (server *PaxosServerState) deletePaxos(paxosID uint64) {
	slog.Debug("Deleting paxos", slog.Uint64("paxos ID", paxosID))
	delete(server.ongoingPaxos, paxosID)
	server.minPaxosID = min(server.minPaxosID, paxosID+1)
}
