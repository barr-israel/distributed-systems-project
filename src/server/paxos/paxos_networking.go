/*
Package paxos is responsible for communication between the servers, using the paxos algorithm to order incoming requests
*/
package paxos

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"sync"

	"server/config"
	"server/etcd"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PaxosServerState struct {
	UnimplementedPaxosServer
	etcdClient      *etcd.EtcdClient
	leader          uint16
	connections     []*grpc.ClientConn
	peers           []PaxosClient
	leaderLock      sync.Mutex
	data            map[string]string
	minimumPaxosID  uint64
	commitedPaxosID uint64
	nextPaxosID     uint64
	ongoingPaxos    map[uint64]*PaxosInstance
	// TODO: servers that are behind but still alive dont work correctly
}

func (server *PaxosServerState) getOrCreatePaxosInstance(paxosID uint64) *PaxosInstance {
	instance, contains := server.ongoingPaxos[paxosID]
	if !contains {
		instance = NewPaxosInstance()
		server.ongoingPaxos[paxosID] = instance
	}
	return instance
}

func (server *PaxosServerState) Close() {
	for _, conn := range server.connections {
		err := conn.Close()
		if err != nil {
			log.Println("Error disconnecting from peer", err)
		}
	}
}

func sendPrepare(ctx context.Context, peer PaxosClient, msg *PrepareMessage, responses chan<- *PromiseMessage) {
	res, err := peer.Prepare(ctx, msg)
	if err != nil {
		log.Println("Error sending prepare to peer")
	}
	responses <- res
}

func sendAccept(ctx context.Context, peer PaxosClient, msg *AcceptMessage, responses chan<- *AcceptResponse) {
	res, err := peer.Accept(ctx, msg)
	if err != nil {
		log.Println("Error sending prepare to peer")
	}
	responses <- res
}

func sendAccepted(ctx context.Context, peer PaxosClient, msg *AcceptedMessage) {
	_, err := peer.Accepted(ctx, msg)
	if err != nil {
		log.Println("Error sending accepted to peer")
	}
}

func (server *PaxosServerState) Accept(ctx context.Context, msg *AcceptMessage) (*AcceptResponse, error) {
	response := server.getOrCreatePaxosInstance(msg.PaxosId).Accept(msg)
	for _, peer := range server.peers {
		go sendAccepted(ctx, peer, response)
	}
	return &AcceptResponse{Ack: response.Ack}, nil
}

func (server *PaxosServerState) Prepare(ctx context.Context, msg *PrepareMessage) (*PromiseMessage, error) {
	return server.getOrCreatePaxosInstance(msg.PaxosId).Prepare(msg), nil
}

func (server *PaxosServerState) Accepted(ctx context.Context, msg *AcceptedMessage) (*emptypb.Empty, error) {
	server.getOrCreatePaxosInstance(msg.PaxosId).Accepted(msg)
	return &emptypb.Empty{}, nil
}

func (server *PaxosServerState) InitiateRound(ctx context.Context, msg *InitiatiationRequest) (*emptypb.Empty, error) {
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	if msg.NextPaxosId < server.nextPaxosID {
		// already ran the requested round
		return &emptypb.Empty{}, nil
	}
	server.leaderLock.Lock()
	defer server.leaderLock.Unlock()
	round := uint64(1)
	var preference *Proposal = nil
	for {
		currentPaxosID := server.nextPaxosID
		msg := PrepareMessage{PaxosId: currentPaxosID, Round: round}
		responses := make(chan *PromiseMessage)
		for _, peer := range server.peers {
			go sendPrepare(ctx, peer, &msg, responses)
		}
		prepareAckCount := uint16(0)
		for range config.PaxosMemberCount {
			res := <-responses
			if res.LastGoodRound > round {
				round = res.LastGoodRound
				preference = &Proposal{key: res.Proposal.Key, value: res.Proposal.Value}
			}
			if res.Ack {
				prepareAckCount++
				if prepareAckCount > config.PaxosMemberCount/2 {
					msg := AcceptMessage{PaxosId: server.nextPaxosID, Round: round, Proposal: &ProposalMessage{Key: preference.key, Value: preference.value}}
					responses := make(chan *AcceptResponse)
					for _, peer := range server.peers {
						go sendAccept(ctx, peer, &msg, responses)
					}
					acceptAckCount := uint16(0)
					for range config.PaxosMemberCount {
						res := <-responses
						if res.Ack {
							acceptAckCount++
							if acceptAckCount > config.PaxosMemberCount/2 {
								server.nextPaxosID = max(currentPaxosID+1, server.nextPaxosID)
								return &emptypb.Empty{}, nil
							}
						}
					}
				}
			}
		}
		round++
	}
}

func (server *PaxosServerState) OrderRead(ctx context.Context, msg *ReadRequestMessage) (*ReadResponse, error) {
	if server.leader != config.PaxosMyID {
		return nil, errors.New("server is not the leader")
	}
	server.leaderLock.Lock()
	value, contains := server.data[msg.Key]
	server.leaderLock.Unlock()
	if contains {
		return &ReadResponse{Value: &value}, nil
	} else {
		return &ReadResponse{Value: nil}, nil
	}
}

func (server *PaxosServerState) deletePaxos(paxosID uint64) {
	server.ongoingPaxos[paxosID] = nil
}

func (server *PaxosServerState) Read(ctx context.Context, key string) string {
	return server.data[key]
}

func (server *PaxosServerState) LinearizedRead(ctx context.Context, key string) {
	// TODO: implement by joining the next paxos
}

func (server *PaxosServerState) Write(ctx context.Context, key string, value string) {
	// TODO: implement using a queue and a reply
}

func (server *PaxosServerState) refreshLeader(ctx context.Context) {
	server.leader = server.etcdClient.RefreshLeader(ctx)
}

func (server *PaxosServerState) ServegRPC() {
	listen, err := net.Listen("tcp", config.GRPCListenAddress)
	if err != nil {
		log.Panicln("error listening")
	}
	grpcServer := grpc.NewServer()
	RegisterPaxosServer(grpcServer, server)
	err = grpcServer.Serve(listen)
	if err != nil {
		log.Panicln("error serving grpc server")
	}
}

func SetupgRPC(ctx context.Context, cli *etcd.EtcdClient) *PaxosServerState {
	connections := make([]*grpc.ClientConn, config.PaxosMemberCount)
	peers := make([]PaxosClient, config.PaxosMemberCount)
	for i := range config.PaxosMemberCount {
		conn, err := grpc.NewClient("localhost:" + strconv.FormatUint(uint64(config.PaxosStartingPort+i), 10))
		if err != nil {
			log.Panicln("Error creating grpc client", i)
		}
		grpcClient := NewPaxosClient(conn)
		connections[i] = conn
		peers[i] = grpcClient
	}
	server := &PaxosServerState{etcdClient: cli, leader: cli.RefreshLeader(ctx), connections: connections, peers: peers, nextPaxosID: 1}
	return server
}
