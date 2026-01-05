package paxos

import (
	context "context"
	"errors"
	"log/slog"
	"time"

	"server/config"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

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
		time.Sleep(config.PaxosRetry * time.Millisecond)
		res, err = peer.Prepare(context.Background(), msg)
	}
	slog.Debug("Received prepare response", slog.Uint64("PeerID", uint64(peerID)), slog.String("promise", res.String()))
	responses <- res
}

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
		time.Sleep(config.PaxosRetry * time.Millisecond)
		res, err = peer.Accept(ctx, msg)
	}
	slog.Debug("Received accept response", slog.Uint64("PeerID", uint64(peerID)), slog.String("response", res.String()))
	responses <- res
}

func (server *PaxosServerState) tryFastPaxos(paxosID uint64) bool {
	req := <-server.incomingRequests
	// must wait for the first request to grab the lock
	server.leaderLock.Lock()
	server.activeWrites = append(server.activeWrites, req)
	for len(server.incomingRequests) != 0 {
		req := <-server.incomingRequests
		server.activeWrites = append(server.activeWrites, req)
	}
	proposal := reqsToActionArray(&server.activeWrites)
	msg := AcceptMessage{PaxosId: paxosID, Round: 1, Proposal: &Proposal{Actions: proposal}}
	responses := make(chan *AcceptResponse, config.PaxosMemberCount)
	for peerID := range config.PaxosMemberCount {
		go server.sendAccept(context.Background(), peerID, &msg, responses)
	}
	acceptAckCount := uint64(0)
	acceptNAckCount := uint64(0)
	for acceptNAckCount < config.PaxosMemberCount/2 {
		res := <-responses
		if res.Ack {
			acceptAckCount++
			if acceptAckCount > config.PaxosMemberCount/2 {
				slog.Info("Leader completed paxos", slog.Uint64("Paxos ID", paxosID))
				// not cancelling the context here because the accept messages are still useful to spread
				return true
			}
		} else {
			acceptNAckCount++
		}
	}
	return false
}

func (server *PaxosServerState) runLeader() {
	for {
		thisPaxosID := server.commitedPaxosID + 1
		slog.Debug("Trying Fast Paxos for", slog.Uint64("Paxos ID", thisPaxosID))
		succeeded := server.tryFastPaxos(thisPaxosID)
		if succeeded {
			server.leaderLock.Unlock()
			server.waitForCommit(thisPaxosID)
			continue
		}
		slog.Debug("Fast Paxos failed, falling back to normal")
		round := uint64(2)
		preference := make([]*Action, 0)
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
			for prepareNAckCount <= config.PaxosMemberCount/2 {
				res := <-responses
				largestReceivedRound = max(res.LastGoodRound, largestReceivedRound)
				if res.Ack {
					prepareAckCount++
					if res.LastGoodRound > largestReceivedAckRound {
						largestReceivedAckRound = res.LastGoodRound
						preference = res.Proposal.Actions
					}
					if prepareAckCount > config.PaxosMemberCount/2 {
						msg := AcceptMessage{PaxosId: thisPaxosID, Round: round, Proposal: &Proposal{Actions: preference}}
						responses := make(chan *AcceptResponse, config.PaxosMemberCount)
						for peerID := range config.PaxosMemberCount {
							go server.sendAccept(context.Background(), peerID, &msg, responses)
						}
						acceptAckCount := uint64(0)
						acceptNAckCount := uint64(0)
						for acceptNAckCount < config.PaxosMemberCount/2 {
							res := <-responses
							if res.Ack {
								acceptAckCount++
								if acceptAckCount > config.PaxosMemberCount/2 {
									slog.Info("Leader completed paxos", slog.Uint64("Paxos ID", thisPaxosID))
									// not cancelling the context here because the accept messages are still useful to spread
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
			// any message about this round is useless at this point
			// skip ahead of every peer
			round = max(round, largestReceivedRound) + 1
			server.refreshLeader(context.Background())
			if server.leader != config.PaxosMyID {
				// this leader was demoted
				return
			}
		}
		server.leaderLock.Unlock()
		server.waitForCommit(thisPaxosID)
	}
}

func (server *PaxosServerState) waitForCommit(thisPaxosID uint64) {
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	for server.commitedPaxosID != thisPaxosID {
		server.commitCond.Wait()
	}
}

func (server *PaxosServerState) WriteToLeader(_ context.Context, msg *Action) (*WriteReply, error) {
	replyChannel := make(chan LocalWriteReply, 1)
	request := LocalWriteRequest{key: msg.Key, value: msg.Value, revision: msg.Revision, replyChannel: replyChannel}
	server.incomingRequests <- &request
	reply := <-replyChannel
	return &WriteReply{Success: reply.Success, Revision: reply.Revision}, nil
}

func (server *PaxosServerState) ReadRevisionFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadRevisionReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
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

func (server *PaxosServerState) ReadFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadReply, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
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

func (server *PaxosServerState) ReadListFromLeader(_ context.Context, msg *ListRequest) (*KeyRevsList, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return &KeyRevsList{Keyrevs: server.getKeys(msg.OmitDeleted)}, nil
}

func (server *PaxosServerState) RequestState(_ context.Context, _ *emptypb.Empty) (*State, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
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
	return &State{MinPaxosId: server.minPaxosID, MaxPaxosId: server.maxPaxosID, CommitedPaxosId: server.commitedPaxosID, DataState: &Proposal{Actions: dataState}}, nil
}

func (server *PaxosServerState) SuggestPromoteSelf(ctx context.Context, _ *emptypb.Empty) (*PromotionReply, error) {
	server.refreshLeader(ctx)
	return &PromotionReply{Leader: server.leader}, nil
}
