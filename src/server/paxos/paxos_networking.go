/*
Package paxos is responsible for communication between the servers, using the paxos algorithm to order incoming requests
*/
package paxos

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"sync"
	"time"

	"server/config"
	"server/etcd"
	"server/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PaxosLocalRequest struct {
	key          string
	value        *string
	replyChannel chan<- byte
}

type PaxosServerState struct {
	UnimplementedPaxosServer
	etcdClient       *etcd.EtcdClient
	leader           uint64
	connections      []*grpc.ClientConn
	peers            []PaxosClient
	data             map[string]*string
	leaderLock       sync.Mutex
	acceptorLock     sync.Mutex
	minPaxosID       uint64
	maxPaxosID       uint64
	commitedPaxosID  uint64
	ongoingPaxos     map[uint64]*PaxosInstance
	incomingRequests chan PaxosLocalRequest
	activeWrites     []*PaxosLocalRequest
}

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
		time.Sleep(1 * time.Second)
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
		time.Sleep(1 * time.Second)
		res, err = peer.Accept(ctx, msg)
	}
	slog.Debug("Received accept response", slog.Uint64("PeerID", uint64(peerID)), slog.String("response", res.String()))
	responses <- res
}

func (server *PaxosServerState) sendAccepted(peerID uint64, msg *AcceptedMessage) {
	slog.Debug("Sending accepted", slog.Uint64("Peer ID", uint64(peerID)), slog.String("msg", msg.String()))
	peer := server.peers[peerID]
	_, err := peer.Accepted(context.Background(), msg)
	for err != nil {
		slog.Error("Error sending accepted to peer, retrying:", slog.Uint64("peer ID", uint64(peerID)), slog.String("error", err.Error()))
		time.Sleep(1 * time.Second)
		_, err = peer.Accepted(context.Background(), msg)
	}
	slog.Debug("Received accepted response", slog.Uint64("PeerID", uint64(peerID)))
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
}

func (server *PaxosServerState) commitActions(actionLocal []ActionLocal) {
	activeRequestsCount := len(server.activeWrites)
	for _, action := range actionLocal {
		if action.value != nil {
			server.data[action.key] = action.value
		} else {
			delete(server.data, action.key)
		}
		for i := 0; i < len(server.activeWrites); i++ {
			for i := 0; i < activeRequestsCount; i++ {
				request := server.activeWrites[i]
				// checking just the key is sufficient because any group of writes can be commited by commiting one of them and pretending the rest were commited just before it
				if action.key == request.key {
					if action.value != nil {
						slog.Info("Commited write", slog.String("key", action.key), slog.String("value", *action.value))
					} else {
						slog.Info("Commited delete", slog.String("key", action.key))
					}
					// request fulfilled, notify requster
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
	log.Println("remaining requests: ", server.activeWrites)
}

func (server *PaxosServerState) InitiateRound(ctx context.Context, msg *InitiatiationRequest) (*emptypb.Empty, error) {
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	if msg.NextPaxosId < server.maxPaxosID {
		// already ran the requested round
		return &emptypb.Empty{}, nil
	}
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	slog.Info("Received round request with ID", slog.Uint64("Paxos ID", msg.NextPaxosId))
	round := uint64(1)
	preference := make([]*Action, 0)
	server.maxPaxosID++
	thisPaxosID := server.maxPaxosID
	for {
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
				if res.LastGoodRound > largestReceivedAckRound {
					largestReceivedAckRound = res.LastGoodRound
					preference = res.Proposal.Actions
				}
				// round = max(round, largestReceivedRound)
				if largestReceivedAckRound == 0 {
					// common case optimization: the leader can act as an N+1 participant with r'=0 with any proposal it wants, including merging all known proposals
					preference = append(preference, res.Proposal.Actions...)
				}
				prepareAckCount++
				if prepareAckCount > config.PaxosMemberCount/2 {
					// read remaining promises incase they have more to merge, and once we have enough promises, it is guaranteed again that no accepts have been made
					for len(responses) != 0 {
						res := <-responses
						if res.Ack {
							preference = append(preference, res.Proposal.Actions...)
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
	}
}

func (server *PaxosServerState) ReadFromLeader(_ context.Context, msg *ReadRequestMessage) (*ReadReply, error) {
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	server.acceptorLock.Lock()
	defer server.acceptorLock.Unlock()
	return &ReadReply{Value: server.data[msg.Key]}, nil
}

func (server *PaxosServerState) refreshLeader(ctx context.Context) {
	server.leader = server.etcdClient.RefreshLeader(ctx)
	slog.Info("Leader refreshed:", slog.Uint64("leader", uint64(server.leader)))
}

func (server *PaxosServerState) notifyLeader(ctx context.Context) {
	req := InitiatiationRequest{NextPaxosId: server.maxPaxosID + 1}
	_, err := server.peers[server.leader].InitiateRound(ctx, &req)
	for err != nil {
		slog.Warn("Error notifying leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		_, err = server.peers[server.leader].InitiateRound(ctx, &req)
	}
}

func (server *PaxosServerState) deletePaxos(paxosID uint64) {
	slog.Debug("Deleting paxos", slog.Uint64("paxos ID", paxosID))
	delete(server.ongoingPaxos, paxosID)
	server.minPaxosID = min(server.minPaxosID, paxosID+1)
}

func (server *PaxosServerState) Read(ctx context.Context, key string) *string {
	return server.data[key]
}

func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) *string {
	res, err := server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	for err != nil {
		slog.Warn("Error reading from leader, retrying", slog.String("error", err.Error()))
		server.refreshLeader(ctx)
		res, err = server.peers[server.leader].ReadFromLeader(ctx, &ReadRequestMessage{Key: key})
	}
	return res.Value
}

func (server *PaxosServerState) AsyncWrite(ctx context.Context, key string, value *string) {
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, replyChannel: nil}
	server.notifyLeader(ctx)
}

func (server *PaxosServerState) Write(ctx context.Context, key string, value *string) {
	replyChannel := make(chan byte)
	server.incomingRequests <- PaxosLocalRequest{key: key, value: value, replyChannel: replyChannel}
	server.notifyLeader(ctx)
	<-replyChannel
}

func (server *PaxosServerState) ServegRPC() {
	listen, err := net.Listen("tcp", config.PaxosListenAddress)
	if err != nil {
		util.SlogPanic("error listening")
	}
	grpcServer := grpc.NewServer()
	RegisterPaxosServer(grpcServer, server)
	err = grpcServer.Serve(listen)
	if err != nil {
		util.SlogPanic("error serving grpc server")
	}
}

func SetupGRPC(ctx context.Context, cli *etcd.EtcdClient) *PaxosServerState {
	connections := make([]*grpc.ClientConn, config.PaxosMemberCount)
	peers := make([]PaxosClient, config.PaxosMemberCount)
	for peerID := range config.PaxosMemberCount {
		conn, err := grpc.NewClient(cli.GetPeerAddress(ctx, peerID), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			util.SlogPanic("Error creating grpc client", slog.Uint64("client", uint64(peerID)), slog.String("error", err.Error()))
		}
		grpcClient := NewPaxosClient(conn)
		connections[peerID] = conn
		peers[peerID] = grpcClient
	}
	incomingRequests := make(chan PaxosLocalRequest, config.PaxosMaxReqPerRound)
	server := &PaxosServerState{etcdClient: cli, leader: cli.RefreshLeader(ctx), connections: connections, peers: peers, incomingRequests: incomingRequests, ongoingPaxos: make(map[uint64]*PaxosInstance), data: make(map[string]*string)}
	go server.ServegRPC()
	return server
}
