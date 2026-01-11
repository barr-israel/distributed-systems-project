/*
Package etcd is responsible for all communications with the etcd cluster for failure detection and leader selection purposes
*/
package etcd

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"server/config"
	"server/util"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClient struct {
	Client          *clientv3.Client
	leaseID         clientv3.LeaseID
	keepAliveCancel context.CancelFunc
	keepAliveDone   chan struct{}
}

// reads a string from the etcd server
func readString(client *clientv3.Client, key string) string {
	res, err := client.Get(context.Background(), key)
	if err != nil || res.Count == 0 {
		util.SlogPanic("Can't read from etcd", slog.String("key", key))
	}
	return string(res.Kvs[0].Value)
}

// reads an integer from the etcd server
func readInt(client *clientv3.Client, key string) uint64 {
	str := readString(client, key)
	parsed, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		util.SlogPanic("invalid etcd config value", slog.String("key", key))
	}
	return parsed
}

// reads all the configuration values from the etcd server
func readConfig(client *clientv3.Client) {
	config.PaxosListenAddress = readString(client, "paxos_listen_address/"+config.MyPeerIDStr)
	config.HTTPListenAddress = readString(client, "http_listen_address/"+config.MyPeerIDStr)
	config.PaxosMemberCount = readInt(client, "paxos_member_count")
	config.PaxosMaxReqPerRound = readInt(client, "paxos_max_req_per_round")
	config.PaxosCleanupThreshold = readInt(client, "paxos_cleanup_threshold")
	config.PaxosRetryDelay = time.Duration(int64(readInt(client, "paxos_retry_milliseconds"))) * time.Millisecond
	config.EtcdLeaseTTL = int64(readInt(client, "etcd_lease_ttl_seconds"))
	config.PaxosArtificialDelay = time.Duration(int64(readInt(client, "paxos_artificial_delay_milliseconds"))) * time.Millisecond
}

// PublishReady adds this peer to the list of ready peers on the etcd server,
// the addition is using the lease of this peer and will be deleted if it expires
func (client *EtcdClient) PublishReady() {
	paxosIDStr := strconv.FormatUint(uint64(config.MyPeerID), 10)
	myReadyKey := "ready/" + paxosIDStr
	cmp := clientv3.Compare(clientv3.CreateRevision(myReadyKey), "=", 0)
	put := clientv3.OpPut(myReadyKey, config.PaxosListenAddress, clientv3.WithLease(client.leaseID))
	res, err := client.Client.Txn(context.Background()).If(cmp).Then(put).Commit()
	if err != nil {
		util.SlogPanic("Error registering self")
	}
	if !res.Succeeded {
		util.SlogPanic("Error registering self, ID already exists")
	}
}

// Close shuts down the etcd client and gracefully revokes the lease this peer was using
func (client *EtcdClient) Close() {
	client.keepAliveCancel()
	<-client.keepAliveDone
	err := client.Client.Close()
	if err != nil {
		slog.Error("Error closing etcd server")
	}
}

// hand written keep alive loop instead of session to allow revoking on manual disconnection
func keepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, doneChannel chan struct{}) {
	keepaliveCh, err := cli.KeepAlive(ctx, leaseID)
	if err != nil {
		util.SlogPanic("Failed to start KeepAlive")
	}
	for range keepaliveCh {
		// log.Println("Alive")
	}
	slog.Debug("Revoking lease")
	revokeCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.EtcdLeaseTTL)*time.Second)
	_, err = cli.Revoke(revokeCtx, leaseID)
	if err != nil {
		slog.Error("Error revoking lease")
	} else {
		slog.Debug("Lease revoked")
	}
	close(doneChannel)
	cancel()
}

// EtcdSetup sets up the etcd client, obtains a lease and launches the keep alive loop
func EtcdSetup() EtcdClient {
	slog.Info("Starting etcd client")
	cli, err := clientv3.NewFromURL(config.EtcdListenAddress)
	if err != nil {
		util.SlogPanic("Failed to connect to etcd")
	}
	readConfig(cli)
	lease, err := cli.Grant(context.Background(), config.EtcdLeaseTTL)
	if err != nil {
		util.SlogPanic("Failed to grant lease")
	}
	kaCtx, kaCancel := context.WithCancel(context.Background())
	keepAliveDone := make(chan struct{})
	go keepAlive(kaCtx, cli, lease.ID, keepAliveDone)
	slog.Info("etcd client connected")
	client := EtcdClient{Client: cli, leaseID: lease.ID, keepAliveCancel: kaCancel, keepAliveDone: keepAliveDone}
	return client
}

// RefreshLeader gets the current leader or becomes the leader if there isnt one,
// returns the current leader
func (client *EtcdClient) RefreshLeader(ctx context.Context) uint64 {
	leader, err := client.tryGetLeader(ctx)
	if err != nil {
		leader = client.tryBecomeLeader(ctx)
	}
	return leader
}

// GetReadyPeers returns the list of peers that are ready
func (client *EtcdClient) GetReadyPeers(ctx context.Context) *clientv3.GetResponse {
	alivePeers, err := client.Client.Get(ctx, "ready/", clientv3.WithPrefix())
	if err != nil {
		util.SlogPanic("Cant get alive peers list")
	}
	return alivePeers
}

// GetPeerAddress gets the gRPC listen address of a given peer
func (client *EtcdClient) GetPeerAddress(ctx context.Context, peerID uint64) string {
	key := "paxos_listen_address/" + strconv.FormatUint(peerID, 10)
	res, err := client.Client.Get(ctx, key)
	if err != nil || res.Count == 0 {
		util.SlogPanic("Error reading peer listen address from etcd", slog.Uint64("Peer ID", peerID))
	}
	return string(res.Kvs[0].Value)
}

// tries to become the leader if there isnt one
// using a transaction to effectively implement compare_exchange(&leader,nil,PaxosMyID)
func (client *EtcdClient) tryBecomeLeader(ctx context.Context) uint64 {
	cmp := clientv3.Compare(clientv3.CreateRevision("leader"), "=", 0)
	put := clientv3.OpPut("leader", strconv.FormatUint(uint64(config.MyPeerID), 10), clientv3.WithLease(client.leaseID))
	get := clientv3.OpGet("leader")
	res, err := client.Client.Txn(ctx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		util.SlogPanic("Error trying to become leader")
	}
	rangeRes := res.Responses[0].GetResponseRange()
	if rangeRes == nil {
		return config.MyPeerID
	} else {
		leader, _ := strconv.Atoi(string(rangeRes.Kvs[0].Value))
		return uint64(leader)
	}
}

func (client *EtcdClient) tryGetLeader(ctx context.Context) (uint64, error) {
	res, err := client.Client.Get(ctx, "leader")
	if err != nil {
		util.SlogPanic("Error checking leader")
	}
	if res.Count == 1 {
		// leader exists
		leader, _ := strconv.Atoi(string(res.Kvs[0].Value))
		return uint64(leader), nil
	}
	return 0, errors.New("no leader assigned")
}
