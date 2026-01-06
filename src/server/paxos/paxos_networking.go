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
	// general context that can be used by the server
	ctx context.Context
	// the connection to the etcd server, used for leader selection
	etcdClient *etcd.EtcdClient
	// who is the current leader(as far as we currently are up to date)
	leader uint64
	// grpc connections to peers
	connections []*grpc.ClientConn
	// gRPC RPC endpoints to peers
	peers []PaxosClient
	// the actual database
	data map[string]*DataEntry
	// the minimum Paxos ID of all instances that are currently stored, all lower IDs have been deleted
	// because they will never be needed again
	minPaxosID uint64
	// the maximum Paxos ID that has been commited(in this server)
	commitedPaxosID uint64
	// all the Paxos instances that are currently stored(including: commited but possibly needed by peers and uncommited)
	ongoingPaxos map[uint64]*PaxosInstance
	// lock used to synchronize access to all the acceptor related data and the database itself
	acceptorLock sync.Mutex

	// variables only relevant to the current leader

	// TODO: do we actually need this lock
	// lock used to serialize actions performed on the leader, ensures that when the leader replies to a linearized read, there is no ongoing paxos
	leaderLock sync.Mutex
	// whether the leader Paxos algorithm is currently running(to prevent running it twice at the same time for no benefit)
	runningAsLeader bool
	// condition variable used to notify the leader that a paxos instance has been commited
	commitCond sync.Cond
	// incoming write requests to the leader
	incomingRequests chan *LocalWriteRequest
	// write requests that have been pulled from incomingRequests and are waiting to be included in a commited Paxos instance
	activeWrites []*LocalWriteRequest
}

// get a list of all the keys in the database and their revision
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

// Close shuts down the server
func (server *PaxosServerState) Close() {
	for _, conn := range server.connections {
		err := conn.Close()
		if err != nil {
			slog.Error("Error disconnecting from peer")
		}
	}
}

// Recover recovers the state of the server instead of starting from scratch,
// This is done by repeatedly finding or promoting a leader that is already participating,
// and asking it for the current state.
// If a Paxos instance is currently running, the recovery will complete when it ends.
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
			server.commitedPaxosID = state.CommitedPaxosId
			slog.Info("Recovered state", slog.Uint64("Leader ID", server.leader), slog.Uint64("Commited Paxos ID", server.commitedPaxosID))
			return
		}
	}
}

// Get the current leader or become one if there is no current leader
func (server *PaxosServerState) refreshLeader(ctx context.Context) {
	server.leader = server.etcdClient.RefreshLeader(ctx)
	if server.leader == config.MyPeerID && !server.runningAsLeader {
		server.leaderLock.Lock()
		if !server.runningAsLeader {
			server.runningAsLeader = true
			go server.runLeader()
		}
		server.leaderLock.Unlock()
	}
	slog.Info("Leader refreshed:", slog.Uint64("leader", uint64(server.leader)))
}

// Start listening on the gRPC endpoints
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

// SetupGRPC sets up the server
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
