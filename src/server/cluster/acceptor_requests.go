package cluster

/*
* Implementation for the gRPC endpoints directed at paxos acceptors, and related functions such as commiting and log compaction.
 */

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"server/config"

	"google.golang.org/protobuf/types/known/emptypb"
)

// gets the paxos instance of a given Paxos ID, or creates a new one if it doesnt exist
func (server *ServerState) getOrCreatePaxosInstance(paxosID uint64) *PaxosInstance {
	instance, contains := server.ongoingPaxos[paxosID]
	if !contains {
		slog.Info("Starting new paxos", slog.Uint64("Paxos ID", paxosID))
		instance = NewPaxosInstance()
		server.ongoingPaxos[paxosID] = instance
	}
	return instance
}

// RELIABLY sends an ACCEPTED message,
// To ensure the message arrives even if the current peer crashes, it retries until the peer replies
func (server *ServerState) sendAccepted(peerID uint64, msg *AcceptedMessage) {
	slog.Debug("Sending accepted", slog.Uint64("Peer ID", uint64(peerID)), slog.String("msg", msg.String()))
	peer := server.peers[peerID]
	_, err := peer.Accepted(context.Background(), msg)
	for err != nil {
		slog.Error("Error sending accepted to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetryDelay)
		_, err = peer.Accepted(context.Background(), msg)
	}
	slog.Debug("Received accepted response", slog.Uint64("PeerID", uint64(peerID)))
}

// RELIABLY requests the commited ID of a peer, to be used for log compaction
// To ensure the message arrives even if the current peer crashes, it retries until the peer replies
func (server *ServerState) sendCommitedIDRequest(peerID uint64, responses chan<- uint64) {
	slog.Debug("Sending min ID request", slog.Uint64("Peer ID", uint64(peerID)))
	peer := server.peers[peerID]
	res, err := peer.GetCommitedPaxosID(server.ctx, &emptypb.Empty{})
	for err != nil {
		slog.Error("Error sending min ID request to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetryDelay)
		res, err = peer.GetCommitedPaxosID(server.ctx, &emptypb.Empty{})
	}
	slog.Debug("Received min ID request response", slog.Uint64("PeerID", uint64(peerID)), slog.String("response", res.String()))
	responses <- res.CommitedPaxosId
}

// Prepare is the gRPC endpoint for PREPARE messages
func (server *ServerState) Prepare(ctx context.Context, msg *PrepareMessage) (*PromiseMessage, error) {
	time.Sleep(config.PaxosArtificialDelay)
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

// Accept is the gRPC endpoint for ACCEPT messages
func (server *ServerState) Accept(ctx context.Context, msg *AcceptMessage) (*AcceptResponse, error) {
	time.Sleep(config.PaxosArtificialDelay)
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

// Accepted is the gRPC endpoint for ACCEPTED messages
func (server *ServerState) Accepted(ctx context.Context, msg *AcceptedMessage) (*emptypb.Empty, error) {
	time.Sleep(config.PaxosArtificialDelay)
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	slog.Debug("Received accepted", slog.String("msg", msg.String()))
	if server.minPaxosID > msg.PaxosId {
		return nil, nil
	}
	p := server.getOrCreatePaxosInstance(msg.PaxosId)
	commit := p.Accepted(msg)
	// In this variation of Paxos, ACCEPTED messages save bandwidth by not sending the proposal,
	// An acceptor knows that the peer that sent them the ACCEPTED has the proposal so they ask them for it only if they don't already have it
	// The peer that sent the ACCEPTED could have crashed, but least one of the quorum of ACCEPTED this peer will receive must survive this Paxos instance.
	if p.missingProposal(msg.Round) {
		slog.Debug("Trying to fill in")
		res, err := server.peers[msg.SenderId].FillInProposal(ctx, &FillInProposalMessage{PaxosId: msg.PaxosId, Round: msg.Round})
		if err == nil {
			slog.Debug("Fill in successful")
			p.fillInProposal(msg.Round, res)
		} else {
			slog.Warn("Fill in failed", slog.String("error", err.Error()))
		}
	}
	if commit {
		server.tryCommitPaxos()
	}
	return &emptypb.Empty{}, nil
}

// FillInProposal is the gRPC endpoint to request a missing proposal from a peer
// It is only called in response to this acceptor sending an ACCEPTED for the given round,
// so the missing proposal will be present as long as this acceptor did not crash since
func (server *ServerState) FillInProposal(ctx context.Context, msg *FillInProposalMessage) (*Proposal, error) {
	time.Sleep(config.PaxosArtificialDelay)
	slog.Debug("Received Fill In Request", slog.Uint64("Paxos ID", msg.PaxosId), slog.Uint64("round", msg.Round))
	// This request only arrives we are still waiting for this peer to acknowledge our "ACCEPTED",
	// so locking is unncessary because the proposal for the given round is already "locked"(locking here will also cause a dead-lock)
	proposal, exists := server.ongoingPaxos[msg.PaxosId]
	if !exists {
		// possible if we just recovered from a crash
		return nil, errors.New("requested paxos ID not found")
	}
	return proposal.getProposalForRound(msg.Round)
}

// GetCommitedPaxosID is the gRPC endpoint to get the commitedPaxosID from a peer
func (server *ServerState) GetCommitedPaxosID(ctx context.Context, _ *emptypb.Empty) (*CommitedPaxosID, error) {
	time.Sleep(config.PaxosArtificialDelay)
	return &CommitedPaxosID{CommitedPaxosId: server.commitedPaxosID}, nil
}

// try to commit any uncommited paxos instanced IN ORDER
// assumes acceptorLock is held
func (server *ServerState) tryCommitPaxos() {
	paxos := server.ongoingPaxos[server.commitedPaxosID+1]
	for paxos != nil && paxos.decided {
		server.commitedPaxosID++
		server.commitActions(paxos.getDecidedValue(), server.commitedPaxosID)
		slog.Info("Commited paxos:", slog.Uint64("paxosID", server.commitedPaxosID))
		if paxos.done {
			server.deletePaxos(server.commitedPaxosID)
		}
		paxos = server.ongoingPaxos[server.commitedPaxosID+1]
	}
	// notify a waiting leader the commit they are waiting for may have been commited
	server.commitCond.Broadcast()
	server.cleanUpOldPaxos()
}

// Perform log compaction by querying the commitedPaxosID of all other peers and deleting any Paxos instance with a lower ID
// assumes acceptorLock is held
func (server *ServerState) cleanUpOldPaxos() {
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

// commit a given set of actions into the database with a given Paxos ID as the revision
func (server *ServerState) commitActions(actionLocal *[]ActionLocal, paxosID uint64) {
	activeRequestsCount := len(server.activeWrites)
	for _, action := range *actionLocal {
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
		if server.leaderPeerID == config.MyPeerID {
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
					if action.revision != nil {
						success = false
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
			// only keep unfulfilled requests
			server.activeWrites = server.activeWrites[:activeRequestsCount]
			slog.Debug("remaining", slog.Int("requests", len(server.activeWrites)))
		}
	}
}

// delete the paxos instance corresponding to the given Paxos ID(because all peers already send ACCEPTED or commited this instance)
func (server *ServerState) deletePaxos(paxosID uint64) {
	slog.Debug("Deleting paxos", slog.Uint64("paxos ID", paxosID))
	delete(server.ongoingPaxos, paxosID)
	server.minPaxosID = min(server.minPaxosID, paxosID+1)
}
