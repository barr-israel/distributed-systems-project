package config

import (
	"log"
	"os"
	"strconv"
)

var (
	PaxosMemberCount  uint16 = 0
	PaxosStartingPort uint16 = 0
	PaxosMyID         uint16 = 0
	GRPCListenAddress string = ""
)

func setupConf() {
	GRPCListenAddress = os.Getenv("GRPC_ADDRESS")
	memberCountStr := os.Getenv("PAXOS_MEMBER_COUNT")
	memberCount, err := strconv.ParseUint(memberCountStr, 10, 16)
	if err != nil {
		log.Panicln("PAXOS_MEMBER_COUNT not set/invalid")
	}
	PaxosMemberCount = uint16(memberCount)
	startingPortStr := os.Getenv("PAXOS_STARTING_PORT")
	startingPort, err := strconv.ParseUint(startingPortStr, 10, 16)
	if err != nil {
		log.Println("PAXOS_STARTING_PORT not set/invalid, defaulting to 9000")
		startingPort = 9000
	}
	PaxosStartingPort = uint16(startingPort)
	myIDStr := os.Getenv("PAXOS_MY_ID")
	paxosMyID, err := strconv.ParseUint(myIDStr, 10, 16)
	if err != nil {
		log.Panicln("PAXOS_MY_ID not set/invalid")
	}
	PaxosMyID = uint16(paxosMyID)
}
