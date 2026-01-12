#!/bin/sh
set -e

export ETCDCTL_ENDPOINTS="http://etcd:2379"

etcdctl del "" --from-key
etcdctl put paxos_member_count "5"
etcdctl put paxos_listen_address/0 "paxos0:9000"
etcdctl put http_listen_address/0 "0.0.0.0:8080"
etcdctl put paxos_listen_address/1 "paxos1:9000"
etcdctl put http_listen_address/1 "0.0.0.0:8081"
etcdctl put paxos_listen_address/2 "paxos2:9000"
etcdctl put http_listen_address/2 "0.0.0.0:8082"
etcdctl put paxos_listen_address/3 "paxos3:9000"
etcdctl put http_listen_address/3 "0.0.0.0:8083"
etcdctl put paxos_listen_address/4 "paxos4:9000"
etcdctl put http_listen_address/4 "0.0.0.0:8084"
etcdctl put paxos_max_req_per_round "64"
etcdctl put paxos_cleanup_threshold "5"
etcdctl put paxos_retry_milliseconds "1000"
etcdctl put etcd_lease_ttl_seconds "5"
etcdctl put paxos_artificial_delay_milliseconds "0"
