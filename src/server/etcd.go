package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// hand written keep alive loop instead of session to allow revoking on manual disconnection
func keepAlive(ctx context.Context, cli *clientv3.Client, ID clientv3.LeaseID) {
	keepaliveCh, err := cli.KeepAlive(ctx, ID)
	if err != nil {
		log.Panicln("Failed to start KeepAlive")
	}
	for range keepaliveCh {
		log.Println("Alive")
	}
	log.Println("Revoking lease")
	_, err = cli.Revoke(context.Background(), ID)
	if err != nil {
		log.Panicln("Error revoking lease")
	} else {
		log.Println("Lease revoked")
		return
	}
}

func EtcdSetup(wg *sync.WaitGroup) (*clientv3.Client, context.CancelFunc) {
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
	wg.Go(func() { keepAlive(kaCtx, cli, lease.ID) })
	_, err = cli.Put(context.Background(), "alive/"+strconv.FormatInt(int64(lease.ID), 10), os.Getenv("ADDRESS"), clientv3.WithLease(lease.ID))
	if err != nil {
		log.Panicln("Error registering self")
	}
	log.Println("etcd connected")
	return cli, kaCancel
}
