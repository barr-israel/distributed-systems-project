/*
Package etcd is responsible for all communications with the etcd cluster for failure detection and leader selection purposes
*/
package etcd

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"

	"server/config"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClient struct {
	client          *clientv3.Client
	leaseID         clientv3.LeaseID
	keepaliveCancel context.CancelFunc
}

// hand written keep alive loop instead of session to allow revoking on manual disconnection
func keepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID) {
	keepaliveCh, err := cli.KeepAlive(ctx, leaseID)
	if err != nil {
		log.Panicln("Failed to start KeepAlive")
	}
	for range keepaliveCh {
		// log.Println("Alive")
	}
	log.Println("Revoking lease")
	_, err = cli.Revoke(context.Background(), leaseID)
	if err != nil {
		log.Panicln("Error revoking lease")
	} else {
		log.Println("Lease revoked")
		return
	}
}

func EtcdSetup(wg *sync.WaitGroup) EtcdClient {
	log.Println("Starting server")
	cli, err := clientv3.NewFromURL("127.0.0.1:2379")
	if err != nil {
		log.Panicln("Failed to connect to etcd")
	}
	lease, err := cli.Grant(context.Background(), 10)
	if err != nil {
		log.Panicln("Failed to grant lease")
	}
	kaCtx, kaCancel := context.WithCancel(context.Background())
	_, err = cli.Put(context.Background(), "alive/"+strconv.FormatInt(int64(lease.ID), 10), os.Getenv("ADDRESS"), clientv3.WithLease(lease.ID))
	if err != nil {
		log.Panicln("Error registering self")
	}
	wg.Go(func() { keepAlive(kaCtx, cli, lease.ID) })
	log.Println("etcd connected, leaseID:", lease.ID)
	return EtcdClient{client: cli, leaseID: lease.ID, keepaliveCancel: kaCancel}
}

func (client *EtcdClient) RefreshLeader(ctx context.Context) uint16 {
	leader, err := client.tryGetLeader(ctx)
	if err != nil {
		leader = client.tryBecomeLeader(ctx)
	}
	println("leader:", leader)
	return leader
}

func (client *EtcdClient) tryBecomeLeader(ctx context.Context) uint16 {
	cmp := clientv3.Compare(clientv3.CreateRevision("leader"), "=", 0)
	put := clientv3.OpPut("leader", strconv.FormatUint(uint64(config.PaxosMyID), 10), clientv3.WithLease(client.leaseID))
	get := clientv3.OpGet("leader")
	res, err := client.client.Txn(ctx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		log.Panicln("Error trying to become leader")
	}
	rangeRes := res.Responses[0].GetResponseRange()
	if rangeRes == nil {
		return config.PaxosMyID
	} else {
		leader, _ := strconv.Atoi(string(rangeRes.Kvs[0].Value))
		return uint16(leader)
	}
}

func (client *EtcdClient) tryGetLeader(ctx context.Context) (uint16, error) {
	res, err := client.client.Get(ctx, "leader")
	if err != nil {
		log.Panicln("Error checking leader")
	}
	if res.Count == 1 {
		// leader exists
		leader, _ := strconv.Atoi(string(res.Kvs[0].Value))
		return uint16(leader), nil
	}
	return 0, errors.New("no leader assigned")
}
