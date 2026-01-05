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

type LocalWriteRequest struct {
	key          string
	value        *string
	revision     *uint64
	replyChannel chan<- LocalWriteReply
}

type DataEntry struct {
	Value    *string
	Revision uint64
}

type LocalWriteReply struct {
	Success  bool
	Revision uint64
}

type PaxosServerState struct {
	UnimplementedPaxosServer
	ctx              context.Context
	etcdClient       *etcd.EtcdClient
	runningAsLeader  bool
	leader           uint64
	connections      []*grpc.ClientConn
	peers            []PaxosClient
	data             map[string]*DataEntry
	leaderLock       sync.Mutex
	acceptorLock     sync.Mutex
	commitCond       sync.Cond
	minPaxosID       uint64
	maxPaxosID       uint64
	commitedPaxosID  uint64
	ongoingPaxos     map[uint64]*PaxosInstance
	incomingRequests chan *LocalWriteRequest
	activeWrites     []*LocalWriteRequest
}

func (server *PaxosServerState) getKeys(omitDeleted bool) []*KeyRev {
	keyrevs := make([]*KeyRev, len(server.data))
	i := 0
	for key, value := range server.data {
		if !omitDeleted || value.Value != nil {
			keyrevs[i] = &KeyRev{Key: key, Revision: value.Revision}
			i++
		}
	}
	return keyrevs[:i]
}

func (server *PaxosServerState) Recover(ctx context.Context) {
	for {
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
				continue
			}
			server.leader = leader.Leader
			slog.Debug("Found leader", slog.Uint64("leader ID", server.leader))
			state, err := server.peers[server.leader].RequestState(ctx, &emptypb.Empty{})
			if err != nil {
				slog.Warn("Failed to get state from leader", slog.Uint64("Leader ID", leader.Leader))
				continue
			}
			for _, action := range state.DataState.Actions {
				slog.Debug(*action.Value)
				server.data[action.Key] = &DataEntry{Value: action.Value, Revision: *action.Revision}
			}
			server.minPaxosID = state.MinPaxosId
			server.maxPaxosID = state.MaxPaxosId
			server.commitedPaxosID = state.CommitedPaxosId
			slog.Info("Recovered state", slog.Uint64("Leader ID", server.leader), slog.Uint64("Commited Paxos ID", server.commitedPaxosID))
			return
		}
	}
}

func (server *PaxosServerState) refreshLeader(ctx context.Context) {
	server.leader = server.etcdClient.RefreshLeader(ctx)
	if server.leader == config.PaxosMyID && !server.runningAsLeader {
		server.leaderLock.Lock()
		if !server.runningAsLeader {
			server.runningAsLeader = true
			go server.runLeader()
		}
		server.leaderLock.Unlock()
	}
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
	incomingRequests := make(chan *LocalWriteRequest, config.PaxosMaxReqPerRound)
	server := &PaxosServerState{ctx: ctx, etcdClient: cli, connections: connections, peers: peers, incomingRequests: incomingRequests, ongoingPaxos: make(map[uint64]*PaxosInstance), data: make(map[string]*DataEntry), minPaxosID: 1}
	server.commitCond = *sync.NewCond(&server.acceptorLock)
	if config.Recover {
		server.Recover(ctx)
	} else {
		server.refreshLeader(ctx)
	}
	cli.PublishReady()
	cli.WaitForEnoughReady()
	go server.servegRPC()
	return server
}
