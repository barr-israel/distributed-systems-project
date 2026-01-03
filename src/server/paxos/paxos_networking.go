package paxos

import (
	context "context"
	"log/slog"
	"net"
	"strconv"
	sync "sync"

	"server/config"
	"server/etcd"
	"server/util"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type PaxosLocalRequest struct {
	key          string
	value        *string
	replyChannel chan<- struct{}
}

type PaxosServerState struct {
	UnimplementedPaxosServer
	ctx              context.Context
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

func (server *PaxosServerState) tryRecover(ctx context.Context) {
	readyPeers := server.etcdClient.GetReadyPeers(ctx)
	for _, peer := range readyPeers.Kvs {
		peerIDString := string(peer.Key[len("ready/"):])
		slog.Debug("got ready peer", slog.String("peer ID", peerIDString))
		peerID, err := strconv.ParseUint(string(peerIDString), 10, 64)
		if err != nil {
			util.SlogPanic("can't parse peer ID")
		}
		leader, err := server.peers[peerID].SuggestPromoteSelf(ctx, &emptypb.Empty{})
		if err != nil {
			slog.Warn("Failed promoting peer", slog.Uint64("Peer ID", peerID))
		} else {
			server.leader = leader.Leader
			slog.Debug("Found leader", slog.Uint64("leader ID", server.leader))
			state, err := server.peers[server.leader].RequestState(ctx, &emptypb.Empty{})
			if err != nil {
				slog.Warn("Failed to get state from leader", slog.Uint64("Leader ID", leader.Leader))
			}
			for _, action := range state.DataState.Actions {
				server.data[action.Key] = action.Value
			}
			server.minPaxosID = state.MinPaxosId
			server.maxPaxosID = state.MaxPaxosId
			server.commitedPaxosID = state.CommitedPaxosId
			slog.Info("Recovered state", slog.Uint64("Leader ID", server.leader), slog.Uint64("Commited Paxos ID", server.commitedPaxosID))
			return
		}
	}
	// all failed, there is no state to recover
	server.refreshLeader(ctx)
	slog.Info("No recoverable state found, starting fresh")
}

func (server *PaxosServerState) refreshLeader(ctx context.Context) {
	server.leader = server.etcdClient.RefreshLeader(ctx)
	slog.Info("Leader refreshed:", slog.Uint64("leader", uint64(server.leader)))
}

func (server *PaxosServerState) servegRPC() {
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
	server := &PaxosServerState{ctx: ctx, etcdClient: cli, connections: connections, peers: peers, incomingRequests: incomingRequests, ongoingPaxos: make(map[uint64]*PaxosInstance), data: make(map[string]*string)}
	server.tryRecover(ctx)
	cli.PublishReady()
	cli.WaitForEnoughReady()
	go server.servegRPC()
	return server
}
