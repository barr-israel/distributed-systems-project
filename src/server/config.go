package main

import (
	"log"
	"os"
	"strconv"
)

var (
	PAXOS_MEMEBR_COUNT  uint64
	GRPC_LISTEN_ADDRESS string
)

func setupConf() {
	GRPC_LISTEN_ADDRESS = os.Getenv("GRPC_ADDRESS")
	memberCountStr := os.Getenv("PAXOS_MEMBER_COUNT")
	paxosMemberCount, err := strconv.ParseUint(memberCountStr, 10, 64)
	if err != nil {
		log.Panicln("PAXOS_MEMBER_COUNT not set/invalid")
	}
	PAXOS_MEMEBR_COUNT = paxosMemberCount
}
