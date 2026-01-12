package cluster

/*
* Setup functions for the server state
 */

import (
	context "context"
	"log/slog"
	"net"
	"strconv"
	sync "sync"

	"server/config"
	"server/etcd"
	"server/util"

	clientv3 "go.etcd.io/etcd/client/v3"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Recover recovers the state of the server instead of starting from scratch,
// This is done by repeatedly finding or promoting a leader that is already participating,
// and asking it for the current state.
// If a Paxos instance is currently running, the recovery will complete when it ends.
func (server *ServerState) Recover(readyPeers *clientv3.GetResponse) {
	for {
		for _, peer := range readyPeers.Kvs {
			peerIDString := string(peer.Key[len("ready/"):])
			slog.Debug("got ready peer", slog.String("peer ID", peerIDString))
			peerID, err := strconv.ParseUint(string(peerIDString), 10, 64)
			if err != nil {
				util.SlogPanic("can't parse peer ID")
			}
			leader, err := server.peers[peerID].SuggestPromoteSelf(server.ctx, &emptypb.Empty{})
			if err != nil {
				slog.Warn("Failed promoting peer", slog.Uint64("Peer ID", peerID))
				continue
			}
			server.setLeader(leader.Leader)
			slog.Debug("Found leader", slog.Uint64("leader ID", leader.Leader))
			state, err := server.leaderClient.RequestState(server.ctx, &emptypb.Empty{})
			if err != nil {
				slog.Warn("Failed to get state from leader", slog.Uint64("Leader ID", leader.Leader))
				continue
			}
			for _, action := range state.DataState.Actions {
				server.data[action.Key] = &DataEntry{Value: action.Value, Revision: *action.Revision}
			}
			server.minPaxosID = state.MinPaxosId
			server.commitedPaxosID = state.CommitedPaxosId
			slog.Info("Recovered state", slog.Uint64("Leader ID", leader.Leader), slog.Uint64("Commited Paxos ID", server.commitedPaxosID))
			return
		}
	}
}

// Start listening on the gRPC endpoints
func (server *ServerState) servegRPC() {
	listen, err := net.Listen("tcp", config.PaxosListenAddress)
	if err != nil {
		util.SlogPanic("error listening")
	}
	grpcPaxosServer := grpc.NewServer()
	RegisterPaxosServer(grpcPaxosServer, server)
	grpcLeaderServer := grpc.NewServer()
	RegisterLeaderRequestsServer(grpcPaxosServer, server)
	err = grpcPaxosServer.Serve(listen)
	if err != nil {
		util.SlogPanic("error serving grpc server")
	}
	err = grpcLeaderServer.Serve(listen)
	if err != nil {
		util.SlogPanic("error serving grpc server")
	}
}

func (server *ServerState) TryRecover() {
	slog.Info("Checking peers' status")
	readyPeers, err := server.etcdClient.Client.Get(context.Background(), "ready/", clientv3.WithPrefix())
	if err != nil {
		util.SlogPanic("Error reading from etcd")
	}
	readyCount := readyPeers.Count
	slog.Info("", slog.Int64("ready", readyPeers.Count))
	// will only see when the cluster has already started running
	if readyCount > int64(config.PaxosMemberCount/2) {
		slog.Info("Enough peers already up, trying to recover")
		server.Recover(readyPeers)
		server.etcdClient.PublishReady()
		return
	}
	// publish ready when youre sure there is no state to recover
	server.etcdClient.PublishReady()
	server.refreshLeader(server.ctx)
	slog.Info("Waiting for enough peers to start")
	ctx, cancelFunc := context.WithCancel(context.Background())
	watchChannel := server.etcdClient.Client.Watch(ctx, "ready/", clientv3.WithPrefix(), clientv3.WithRev(readyPeers.Header.Revision+1))
	for update := range watchChannel {
		for _, e := range update.Events {
			switch e.Type {
			case clientv3.EventTypePut:
				readyCount++
				if readyCount > int64(config.PaxosMemberCount/2) {
					cancelFunc()
					slog.Info("Enough peers connected, starting")
					return
				}
			case clientv3.EventTypeDelete:
				readyCount--
			}
		}
	}
	cancelFunc()
	util.SlogPanic("Watch failed before enough peers connected")
}

// SetupGRPC sets up the server
func SetupGRPC(ctx context.Context, cli *etcd.EtcdClient) *ServerState {
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
	server := &ServerState{ctx: ctx, etcdClient: cli, connections: connections, peers: peers, incomingRequests: incomingRequests, ongoingPaxos: make(map[uint64]*PaxosInstance), data: make(map[string]*DataEntry), minPaxosID: 1}
	server.commitCond = *sync.NewCond(&server.acceptorLock)
	server.TryRecover()
	go server.servegRPC()
	return server
}
