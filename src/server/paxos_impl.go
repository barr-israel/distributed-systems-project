package main

type Paxos struct {
	leaderLease   uint64
	lastRound     uint64
	lastGoodRound uint64
	decided       bool
	value         string
}

func NewPaxos(leaderLease uint64, value string) Paxos {
	return Paxos{leaderLease: leaderLease, lastRound: 0, lastGoodRound: 0, value: value, decided: false}
}
