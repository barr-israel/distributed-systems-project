/*
Package etcd is responsible for all communications with the etcd cluster for failure detection and leader selection purposes
*/
package etcd

import (
	"context"
	"errors"
	"log/slog"
	"strconv"

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
	paxosIDStr := strconv.FormatUint(uint64(config.PaxosMyID), 10)
	myAliveKey := "alive/" + paxosIDStr
	cmp := clientv3.Compare(clientv3.CreateRevision(myAliveKey), "=", 0)
	put := clientv3.OpPut(myAliveKey, config.PaxosListenAddress, clientv3.WithLease(lease.ID))
	res, err := cli.Txn(context.Background()).If(cmp).Then(put).Commit()
	if err != nil {
		util.SlogPanic("Error registering self")
	}
	if !res.Succeeded {
		util.SlogPanic("Error registering self, ID already exists")
	}
	keepAliveDone := make(chan byte)
	go keepAlive(kaCtx, cli, lease.ID, keepAliveDone)
	slog.Info("etcd client connected")
	return EtcdClient{client: cli, leaseID: lease.ID, keepAliveCancel: kaCancel, keepAliveDone: keepAliveDone}
}

func (client *EtcdClient) RefreshLeader(ctx context.Context) uint64 {
	leader, err := client.tryGetLeader(ctx)
	if err != nil {
		leader = client.tryBecomeLeader(ctx)
	}
	return leader
}

func (client *EtcdClient) GetPeerAddress(ctx context.Context, peerID uint64) string {
	key := "alive/" + strconv.FormatUint(peerID, 10)
	res, err := client.client.Get(ctx, key)
	if err != nil {
		util.SlogPanic("Error getting peer", slog.Uint64("Peer ID", peerID))
	}
	for len(res.Kvs) == 0 {
		res, err = client.client.Get(ctx, key)
		if err != nil {
			util.SlogPanic("Error getting peer", slog.Uint64("Peer ID", peerID))
		}
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
