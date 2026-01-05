/*
Package config is responsible for parsing and storing enviroment variable derived configuration values
*/
package config

import (
	"flag"
	"strconv"
	"time"

	"server/util"
)

var (
	PaxosMyID             uint64        = 0
	PaxosMemberCount      uint64        = 0
	PaxosMaxReqPerRound   uint64        = 0
	PaxosCleanupThreshold uint64        = 0
	PaxosRetry            time.Duration = time.Duration(0)
	PaxosMyIDStr          string        = ""
	PaxosListenAddress    string        = ""
	HTTPListenAddress     string        = ""
	EtcdLeaseTTL          int64         = 0
	Recover               bool          = false
	Verbose               bool          = false
)

func SetupConf() {
	paxosID := flag.Int("id", -1, "paxos ID for this server, must be unique across the cluster")
	flag.BoolVar(&Verbose, "v", false, "verbose output, activates debug level logging")
	flag.BoolVar(&Recover, "recover", false, "Perform state recovery")
	flag.Parse()
	if *paxosID < 0 {
		util.SlogPanic("a non-negative --id <paxos id> is required")
	}
	PaxosMyID = uint64(*paxosID)
	PaxosMyIDStr = strconv.FormatUint(PaxosMyID, 10)
}
