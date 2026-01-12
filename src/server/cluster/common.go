/*
Package cluster is responsible for the state and communications of the cluster, using gRPC to pass messages between servers.
*/
package cluster

/*
* The server state structure itself and common functions used by other files in the package
 */

import (
	context "context"
	"log/slog"
	sync "sync"

	"server/config"
	"server/etcd"

	grpc "google.golang.org/grpc"
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

type ServerState struct {
	UnimplementedPaxosServer
	UnimplementedLeaderRequestsServer
	// general context that can be used by the server
	ctx context.Context
	// the connection to the etcd server, used for leader selection
	etcdClient *etcd.EtcdClient
	// who is the current leader(as far as we currently are up to date)
	leaderPeerID uint64
	leaderClient LeaderRequestsClient
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

func (server *ServerState) setLeader(leaderPeerID uint64) {
	server.leaderPeerID = leaderPeerID
	server.leaderClient = NewLeaderRequestsClient(server.connections[leaderPeerID])
}

// get a list of all the keys in the database and their revision
func (server *ServerState) getKeys(omitDeleted bool) []*KeyRev {
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
func (server *ServerState) Close() {
	for _, conn := range server.connections {
		err := conn.Close()
		if err != nil {
			slog.Error("Error disconnecting from peer")
		}
	}
}

// Get the current leader or become one if there is no current leader
func (server *ServerState) refreshLeader(ctx context.Context) {
	leaderPeerID := server.etcdClient.RefreshLeader(ctx)
	server.setLeader(leaderPeerID)
	if server.leaderPeerID == config.MyPeerID && !server.runningAsLeader {
		server.leaderLock.Lock()
		if !server.runningAsLeader {
			server.runningAsLeader = true
			go server.runLeader()
		}
		server.leaderLock.Unlock()
	}
	slog.Info("Leader refreshed:", slog.Uint64("leader", leaderPeerID))
}
