package paxos

import (
	context "context"
	"errors"
	"log/slog"
	"time"

	"server/config"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// RELIABLY send a PREPARE message to an acceptor
// To ensure the message arrives even if the current peer crashes, it retries until the peer replies
func (server *PaxosServerState) sendPrepare(ctx context.Context, peerID uint64, msg *PrepareMessage, responses chan<- *PromiseMessage) {
	slog.Debug("Sending prepare", slog.Uint64("Peer ID", uint64(peerID)), slog.String("msg", msg.String()))
	peer := server.peers[peerID]
	res, err := peer.Prepare(ctx, msg)
	for err != nil {
		if ctx.Err() != nil {
			// leader has finished already
			return
		}
		slog.Error("Error sending prepare to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetryDelay)
		res, err = peer.Prepare(context.Background(), msg)
	}
	slog.Debug("Received prepare response", slog.Uint64("PeerID", uint64(peerID)), slog.String("promise", res.String()))
	responses <- res
}

// RELIABLY send a ACCEPT message to an acceptor
// To ensure the message arrives even if the current peer crashes, it retries until the peer replies
func (server *PaxosServerState) sendAccept(ctx context.Context, peerID uint64, msg *AcceptMessage, responses chan<- *AcceptResponse) {
	slog.Debug("Sending accept", slog.Uint64("Peer ID", uint64(peerID)), slog.String("msg", msg.String()))
	peer := server.peers[peerID]
	res, err := peer.Accept(ctx, msg)
	for err != nil {
		if ctx.Err() != nil {
			// leader has finished already
			return
		}
		slog.Error("Error sending accept to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(config.PaxosRetryDelay)
		res, err = peer.Accept(ctx, msg)
	}
	slog.Debug("Received accept response", slog.Uint64("PeerID", uint64(peerID)), slog.String("response", res.String()))
	responses <- res
}

// fill the activeWrites buffer from the incomingRequests channel before starting a Paxos instance
func (server *PaxosServerState) fillActiveWrites() {
	req := <-server.incomingRequests
	// only lock after a request has arrived to prevent holding the lock unnecessarily
	server.leaderLock.Lock()
	server.activeWrites = append(server.activeWrites, req)
	for len(server.incomingRequests) != 0 && len(server.activeWrites) < int(config.PaxosMaxReqPerRound) {
		req := <-server.incomingRequests
		server.activeWrites = append(server.activeWrites, req)
	}
}

// run the leader Paxos algorithm as long as this peer is the leader
func (server *PaxosServerState) runLeader() {
	for {
		thisPaxosID := server.commitedPaxosID + 1
		server.fillActiveWrites()
		slog.Debug("Leader starting Paxos", slog.Uint64("Paxos ID", thisPaxosID))
		round := uint64(1)
		preference := reqsToActionArray(&server.activeWrites)
	PaxosInstance:
		for {
			msg := PrepareMessage{PaxosId: thisPaxosID, Round: round}
			responses := make(chan *PromiseMessage, config.PaxosMemberCount)
			for peerID := range config.PaxosMemberCount {
				go server.sendPrepare(context.Background(), peerID, &msg, responses)
			}
			prepareAckCount := uint64(0)
			prepareNAckCount := uint64(0)
			largestReceivedRound := uint64(0)
			largestReceivedAckRound := uint64(0)
			// read PREPARE responses until we have enough ACKs or until it is impossible to have enough
			for prepareNAckCount <= config.PaxosMemberCount/2 {
				res := <-responses
				largestReceivedRound = max(res.LastGoodRound, largestReceivedRound)
				if res.Ack {
					prepareAckCount++
					if res.LastGoodRound > largestReceivedAckRound {
						// adopt proposal from acceptor
						largestReceivedAckRound = res.LastGoodRound
						preference = res.Proposal.Actions
					}
					if prepareAckCount > config.PaxosMemberCount/2 {
						// there are enough PREPARE ACKs
						msg := AcceptMessage{PaxosId: thisPaxosID, Round: round, Proposal: &Proposal{Actions: preference}}
						responses := make(chan *AcceptResponse, config.PaxosMemberCount)
						for peerID := range config.PaxosMemberCount {
							go server.sendAccept(context.Background(), peerID, &msg, responses)
						}
						acceptAckCount := uint64(0)
						acceptNAckCount := uint64(0)
						// read ACCEPT responses until we have enough or until it is impossible to have enough
						for acceptNAckCount < config.PaxosMemberCount/2 {
							res := <-responses
							if res.Ack {
								acceptAckCount++
								if acceptAckCount > config.PaxosMemberCount/2 {
									// there are enough ACCEPT ACKs
									slog.Info("Leader completed paxos", slog.Uint64("Paxos ID", thisPaxosID))
									break PaxosInstance
								}
							} else {
								acceptNAckCount++
							}
						}
					}
				} else {
					prepareNAckCount++
				}
			}
			// skip ahead of every peer
			round = max(round, largestReceivedRound) + 1
			server.refreshLeader(context.Background())
			if server.leader != config.MyPeerID {
				// this leader was demoted
				return
			}
		}
		server.leaderLock.Unlock()
		// the leader must wait for the acceptor that is on the same peer to commit this Paxos instance before continuing to the next
		server.waitForCommit(thisPaxosID)
	}
}

// waits for a given paxos ID to be commited
func (server *PaxosServerState) waitForCommit(paxosID uint64) {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	for server.commitedPaxosID != paxosID {
		server.commitCond.Wait()
	}
}

// WriteToLeader performs a linearized conditional write into the database
func (server *PaxosServerState) WriteToLeader(_ context.Context, msg *Action) (*WriteReply, error) {
	replyChannel := make(chan LocalWriteReply, 1)
	request := LocalWriteRequest{key: msg.Key, value: msg.Value, revision: msg.Revision, replyChannel: replyChannel}
	server.incomingRequests <- &request
	reply := <-replyChannel
	return &WriteReply{Success: reply.Success, Revision: reply.Revision}, nil
}

// ReadRevisionFromLeader performs a linearized read of a revision of a given key
func (server *PaxosServerState) ReadRevisionFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadRevisionReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.MyPeerID {
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
func (server *PaxosServerState) ReadFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.MyPeerID {
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
func (server *PaxosServerState) ReadListFromLeader(_ context.Context, msg *ListRequest) (*KeyRevsList, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.MyPeerID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return &KeyRevsList{Keyrevs: server.getKeys(msg.OmitDeleted)}, nil
}

// RequestState replies with all the data needed for a recovering peer to join in the next Paxos instance
func (server *PaxosServerState) RequestState(_ context.Context, _ *emptypb.Empty) (*State, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.MyPeerID {
		return nil, errors.New("server is not the leader")
	}
	slog.Debug("got state req")
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	dataState := make([]*Action, len(server.data))
	i := 0
	for key, entry := range server.data {
		dataState[i] = &Action{Key: key, Value: entry.Value, Revision: &entry.Revision}
		i++
	}
	return &State{MinPaxosId: server.minPaxosID, CommitedPaxosId: server.commitedPaxosID, DataState: &Proposal{Actions: dataState}}, nil
}

// SuggestPromoteSelf gets the leader, attempts to become the leader if there isn't one, and replies with the current leader
func (server *PaxosServerState) SuggestPromoteSelf(ctx context.Context, _ *emptypb.Empty) (*PromotionReply, error) {
	server.refreshLeader(ctx)
	return &PromotionReply{Leader: server.leader}, nil
}
