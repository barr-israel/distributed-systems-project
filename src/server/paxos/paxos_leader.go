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

func (server *PaxosServerState) InitiateRound(ctx context.Context, msg *InitiatiationRequest) (*emptypb.Empty, error) {
	// TODO: rewrite into the faster implementation
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	if msg.NextPaxosId < server.commitedPaxosID {
		// already ran the requested round
		return &emptypb.Empty{}, nil
	}
	slog.Info("Received round request with ID", slog.Uint64("Paxos ID", msg.NextPaxosId))
	round := uint64(1)
	preference := make([]*Action, 0)
	server.maxPaxosID++
	thisPaxosID := server.maxPaxosID
	for {
		if server.leader != config.PaxosMyID {
			// this leader was demoted
			return nil, errors.New("server is not the leader")
		}
		msg := PrepareMessage{PaxosId: thisPaxosID, Round: round}
		responses := make(chan *PromiseMessage, config.PaxosMemberCount)
		for peerID := range config.PaxosMemberCount {
			go server.sendPrepare(ctx, peerID, &msg, responses)
		}
		prepareAckCount := uint64(0)
		largestReceivedRound := uint64(0)
		largestReceivedAckRound := uint64(0)
		for range config.PaxosMemberCount {
			res := <-responses
			largestReceivedRound = max(res.LastGoodRound, largestReceivedRound)
			if res.Ack {
				prepareAckCount++
				if res.LastGoodRound > largestReceivedAckRound {
					largestReceivedAckRound = res.LastGoodRound
					preference = res.Proposal.Actions
				}
				if round == 1 {
					// common case optimization: the leader can override its own proposal on the first round
					preference = append(preference, res.Proposal.Actions...)
				}
				if prepareAckCount > config.PaxosMemberCount/2 {
					if round == 1 {
						// merge pernding responses as well
						for len(responses) != 0 {
							res := <-responses
							if res.Ack {
								preference = append(preference, res.Proposal.Actions...)
							}
						}
					}
					msg := AcceptMessage{PaxosId: thisPaxosID, Round: round, Proposal: &Proposal{Actions: preference}}
					responses := make(chan *AcceptResponse, config.PaxosMemberCount)
					for peerID := range config.PaxosMemberCount {
						go server.sendAccept(ctx, peerID, &msg, responses)
					}
					acceptAckCount := uint64(0)
					for range config.PaxosMemberCount {
						res := <-responses
						if res.Ack {
							acceptAckCount++
							if acceptAckCount > config.PaxosMemberCount/2 {
								slog.Info("Leader completed paxos", slog.Uint64("Paxos ID", thisPaxosID))
								return &emptypb.Empty{}, nil
							}
						}
					}
				}
			}
		}
		// skip ahead of every peer
		round = max(round, largestReceivedRound) + 1
		server.refreshLeader(ctx)
	}
}

func (server *PaxosServerState) ReadFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadReply, error) {
	// TODO: needs to wait for acceptor to write
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	entry := server.data[msg.Key]
	if entry != nil {
		return &ReadReply{Value: entry.Value, Revision: &entry.Revision}, nil
	} else {
		return &ReadReply{Value: nil, Revision: nil}, nil
	}
}

func (server *PaxosServerState) ReadListFromLeader(_ context.Context, msg *emptypb.Empty) (*KeyList, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return &KeyList{Keys: server.getKeys()}, nil
}

func (server *PaxosServerState) RequestState(_ context.Context, _ *emptypb.Empty) (*State, error) {
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	dataState := make([]*Action, len(server.data))
	for key, entry := range server.data {
		dataState = append(dataState, &Action{Key: key, Value: entry.Value, Revision: &entry.Revision})
	}
	return &State{MinPaxosId: server.minPaxosID, MaxPaxosId: server.maxPaxosID, CommitedPaxosId: server.commitedPaxosID, DataState: &Proposal{Actions: dataState}}, nil
}

func (server *PaxosServerState) SuggestPromoteSelf(ctx context.Context, _ *emptypb.Empty) (*PromotionReply, error) {
	server.refreshLeader(ctx)
	return &PromotionReply{Leader: server.leader}, nil
}
