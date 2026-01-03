/*
Package config is responsible for parsing and storing enviroment variable derived configuration values
*/
package config

import (
	"log/slog"
	"os"
	"strconv"

	"server/util"
)

var (
	PaxosMemberCount    uint64 = 0
	PaxosMyID           uint64 = 0
	PaxosMaxReqPerRound int    = 0
	PaxosListenAddress  string = ""
)

func parseStrConf(name string, defaultValue ...uint64) uint64 {
	value := os.Getenv(name)
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		if len(defaultValue) == 0 {
			slog.Error("config value not set/invalid:", slog.String("name", name))
			panic("config value not set/invalid: " + name)
		} else {
			slog.Warn("config value not set/invalid:", slog.String("name", name), slog.Uint64("default value", defaultValue[0]))
		}
		parsed = defaultValue[0]
	}
	return parsed
}

func SetupConf() {
	PaxosMemberCount = (parseStrConf("PAXOS_MEMBER_COUNT"))
	PaxosMyID = (parseStrConf("PAXOS_MY_ID"))
	PaxosMaxReqPerRound = int(parseStrConf("PAXOS_MAX_REQ_PER_ROUND", 64))
	PaxosListenAddress = os.Getenv("PAXOS_LISTEN_ADDRESS")
	if len(PaxosListenAddress) == 0 {
		util.SlogPanic("PAXOS_LISTEN_ADDRESS not set")
	}
}
