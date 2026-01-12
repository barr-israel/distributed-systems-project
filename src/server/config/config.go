/*
Package config is responsible for parsing and storing enviroment variable derived configuration values
*/
package config

import (
	"flag"
	"server/util"
	"strconv"
	"time"
)

// various configuration values needed across the program, some obtained from command line argument and some from the etcd server
var (
	// the address of the etcd server
	EtcdListenAddress string = "etcd:2379"
	// the peer ID of this server
	MyPeerID uint64 = 0
	// amount of servers participating in the Paxos algorithm
	PaxosMemberCount uint64 = 0
	// amount of requests that may be commited in a paxos instance
	PaxosMaxReqPerRound uint64 = 0
	// how many already commited Paxos instances to keep before trying active log compaction
	PaxosCleanupThreshold uint64 = 0
	// how much time to wait before retrying a failed request
	PaxosRetryDelay time.Duration = time.Duration(0)
	// artificial delay to add to any gRPC request for testing purposes
	PaxosArtificialDelay time.Duration = time.Duration(0)
	// MyPeerID but string
	MyPeerIDStr string = ""
	// the address to run the gRPC server on
	PaxosListenAddress string = ""
	// the address to run the HTTP server on
	HTTPListenAddress string = ""
	// TTL for the etcd lease
	EtcdLeaseTTL int64 = 0
	// whether to output verbose(Debug level) output(otherwise Info level)
	Verbose bool = false
)

// SetupConf parses the command line arguments given to the program
func SetupConf() {
	paxosID := flag.Int("id", -1, "peer ID for this server, must be unique across the cluster")
	flag.BoolVar(&Verbose, "v", false, "verbose output, activates debug level logging")
	flag.Parse()
	if *paxosID < 0 {
		util.SlogPanic("a non-negative --id <paxos id> is required")
	}
	MyPeerID = uint64(*paxosID)
	MyPeerIDStr = strconv.FormatUint(MyPeerID, 10)
}
