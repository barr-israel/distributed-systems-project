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
	client          *clientv3.Client
	leaseID         clientv3.LeaseID
	keepAliveCancel context.CancelFunc
	keepAliveDone   chan byte
}

func (client *EtcdClient) WaitForEnoughReady() {
	slog.Info("Waiting for enough peers to start")
	res, err := client.client.Get(context.Background(), "ready/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		util.SlogPanic("Error reading from etcd")
	}
	readyCount := res.Count
	if readyCount > int64(config.PaxosMemberCount/2) {
		slog.Info("Enough peers connected, starting")
		return
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	watchChannel := client.client.Watch(ctx, "ready/", clientv3.WithPrefix(), clientv3.WithRev(res.Header.Revision+1))
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

func (client *EtcdClient) readString(key string) string {
	res, err := client.client.Get(context.Background(), key)
	if err != nil || res.Count == 0 {
		util.SlogPanic("Can't read from etcd", slog.String("key", key))
	}
	return string(res.Kvs[0].Value)
}

func (client *EtcdClient) readInt(key string) uint64 {
	str := client.readString(key)
	parsed, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		util.SlogPanic("invalid etcd config value", slog.String("key", key))
	}
	return parsed
}

func (client *EtcdClient) readConfig() {
	config.PaxosListenAddress = client.readString("paxos_listen_address/" + config.PaxosMyIDStr)
	config.HTTPListenAddress = client.readString("http_listen_address/" + config.PaxosMyIDStr)
	config.PaxosMemberCount = client.readInt("paxos_member_count")
	config.PaxosMaxReqPerRound = client.readInt("paxos_max_req_per_round")
	config.PaxosCleanupThreshold = client.readInt("paxos_cleanup_threshold")
	config.PaxosRetry = time.Duration(int64(client.readInt("paxos_retry_milliseconds")))
}

func (client *EtcdClient) PublishReady() {
	paxosIDStr := strconv.FormatUint(uint64(config.PaxosMyID), 10)
	myReadyKey := "ready/" + paxosIDStr
	cmp := clientv3.Compare(clientv3.CreateRevision(myReadyKey), "=", 0)
	put := clientv3.OpPut(myReadyKey, config.PaxosListenAddress, clientv3.WithLease(client.leaseID))
	res, err := client.client.Txn(context.Background()).If(cmp).Then(put).Commit()
	if err != nil {
		util.SlogPanic("Error registering self")
	}
	if !res.Succeeded {
		util.SlogPanic("Error registering self, ID already exists")
	}
}

func (client *EtcdClient) Close() {
	client.keepAliveCancel()
	<-client.keepAliveDone
	err := client.client.Close()
	if err != nil {
		slog.Error("Error closing etcd server")
	}
}

// hand written keep alive loop instead of session to allow revoking on manual disconnection
func keepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, doneChannel chan byte) {
	keepaliveCh, err := cli.KeepAlive(ctx, leaseID)
	if err != nil {
		util.SlogPanic("Failed to start KeepAlive")
	}
	for range keepaliveCh {
		// log.Println("Alive")
	}
	slog.Debug("Revoking lease")
	_, err = cli.Revoke(context.Background(), leaseID)
	if err != nil {
		slog.Error("Error revoking lease")
	}
	slog.Debug("Lease revoked")
	close(doneChannel)
}

func EtcdSetup() EtcdClient {
	slog.Info("Starting etcd client")
	cli, err := clientv3.NewFromURL("127.0.0.1:2379")
	if err != nil {
		util.SlogPanic("Failed to connect to etcd")
	}
	lease, err := cli.Grant(context.Background(), 5)
	if err != nil {
		util.SlogPanic("Failed to grant lease")
	}
	kaCtx, kaCancel := context.WithCancel(context.Background())
	keepAliveDone := make(chan byte)
	go keepAlive(kaCtx, cli, lease.ID, keepAliveDone)
	slog.Info("etcd client connected")
	client := EtcdClient{client: cli, leaseID: lease.ID, keepAliveCancel: kaCancel, keepAliveDone: keepAliveDone}
	client.readConfig()
	return client
}

func (client *EtcdClient) RefreshLeader(ctx context.Context) uint64 {
	leader, err := client.tryGetLeader(ctx)
	if err != nil {
		leader = client.tryBecomeLeader(ctx)
	}
	return leader
}

func (client *EtcdClient) GetReadyPeers(ctx context.Context) *clientv3.GetResponse {
	alivePeers, err := client.client.Get(ctx, "ready/", clientv3.WithPrefix())
	if err != nil {
		util.SlogPanic("Cant get alive peers list")
	}
	return alivePeers
}

func (client *EtcdClient) GetPeerAddress(ctx context.Context, peerID uint64) string {
	key := "paxos_listen_address/" + strconv.FormatUint(peerID, 10)
	res, err := client.client.Get(ctx, key)
	if err != nil || res.Count == 0 {
		util.SlogPanic("Error reading peer listen address from etcd", slog.Uint64("Peer ID", peerID))
	}
	return string(res.Kvs[0].Value)
}

func (client *EtcdClient) tryBecomeLeader(ctx context.Context) uint64 {
	cmp := clientv3.Compare(clientv3.CreateRevision("leader"), "=", 0)
	put := clientv3.OpPut("leader", strconv.FormatUint(uint64(config.PaxosMyID), 10), clientv3.WithLease(client.leaseID))
	get := clientv3.OpGet("leader")
	res, err := client.client.Txn(ctx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		util.SlogPanic("Error trying to become leader")
	}
	rangeRes := res.Responses[0].GetResponseRange()
	if rangeRes == nil {
		return config.PaxosMyID
	} else {
		leader, _ := strconv.Atoi(string(rangeRes.Kvs[0].Value))
		return uint64(leader)
	}
}

func (client *EtcdClient) tryGetLeader(ctx context.Context) (uint64, error) {
	res, err := client.client.Get(ctx, "leader")
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
