#! /bin/bash

etcdctl put paxos_member_count "3"
etcdctl put paxos_listen_address/0 "localhost:9000"
etcdctl put http_listen_address/0 "localhost:8080"
etcdctl put paxos_listen_address/1 "localhost:9001"
etcdctl put http_listen_address/1 "localhost:8081"
etcdctl put paxos_listen_address/2 "localhost:9002"
etcdctl put http_listen_address/2 "localhost:8082"
etcdctl put paxos_max_req_per_round "64"
etcdctl put paxos_cleanup_threshold "5"
etcdctl put paxos_retry_milliseconds "1000"
etcdctl put etcd_lease_ttl_seconds "5"
