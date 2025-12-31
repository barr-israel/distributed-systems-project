package paxos

import "server/config"

type Proposal struct {
	key   string
	value string
}

type AcceptedKey struct {
	round    uint64
	proposal Proposal
}

type AcceptedValue struct {
	ack   uint16
	total uint16
}

type PaxosInstance struct {
	lastRound        uint64
	lastGoodRound    uint64
	decided          bool
	proposal         *Proposal
	acceptedReceived map[AcceptedKey]AcceptedValue
	done             bool
}

func (p *PaxosInstance) Prepare(msg *PrepareMessage) *PromiseMessage {
	if msg.Round > p.lastRound && !p.decided {
		p.lastRound = msg.Round
		return &PromiseMessage{LastGoodRound: p.lastGoodRound, Ack: true, Proposal: &ProposalMessage{Key: p.proposal.key, Value: p.proposal.value}}
	} else {
		return &PromiseMessage{LastGoodRound: p.lastGoodRound, Ack: false, Proposal: nil}
	}
}

func (p *PaxosInstance) Accept(msg *AcceptMessage) *AcceptedMessage {
	proposal := Proposal{key: msg.Proposal.Key, value: msg.Proposal.Value}
	ack := msg.Round >= p.lastRound || p.lastRound == 0
	if ack && !p.decided {
		p.lastRound = msg.Round
		p.lastGoodRound = msg.Round
		p.proposal = &proposal
	}
	return &AcceptedMessage{PaxosId: msg.PaxosId, Round: msg.Round, Proposal: msg.Proposal, Ack: ack}
}

func (p *PaxosInstance) Accepted(msg *AcceptedMessage) {
	proposal := Proposal{key: msg.Proposal.Key, value: msg.Proposal.Value}
	key := AcceptedKey{round: msg.Round, proposal: proposal}
	received := p.acceptedReceived[key]
	if msg.Ack {
		received.ack++
	}
	received.total++
	p.acceptedReceived[key] = received
	p.decided = p.decided || received.ack > config.PaxosMemberCount/2
	p.done = p.done || received.total == config.PaxosMemberCount
}

func NewPaxosInstance() *PaxosInstance {
	return &PaxosInstance{lastRound: 0, lastGoodRound: 0, proposal: nil, decided: false}
}
