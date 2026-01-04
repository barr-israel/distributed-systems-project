package paxos

import (
	"server/config"
)

type ActionLocal struct {
	key      string
	value    *string
	revision *uint64
}

func toActionLocalArray(as []*Action) []ActionLocal {
	actions := make([]ActionLocal, len(as))
	for i, a := range as {
		actions[i] = a.toActionLocal()
	}
	return actions
}

func reqsToActionLocalArray(reqs []*PaxosLocalRequest) []ActionLocal {
	actions := make([]ActionLocal, len(reqs))
	for i, a := range reqs {
		actions[i] = a.toActionLocal()
	}
	return actions
}

func toActionArray(as []ActionLocal) []*Action {
	actions := make([]*Action, len(as))
	for i, a := range as {
		action := a.toAction()
		actions[i] = &action
	}
	return actions
}

func (a *Action) toActionLocal() ActionLocal {
	return ActionLocal{key: a.Key, value: a.Value, revision: a.Revision}
}

func (a *ActionLocal) toAction() Action {
	return Action{Key: a.key, Value: a.value, Revision: a.revision}
}

func (req *PaxosLocalRequest) toActionLocal() ActionLocal {
	return ActionLocal{key: req.key, value: req.value, revision: req.revision}
}

type PaxosInstance struct {
	lastRound        uint64
	lastGoodRound    uint64
	decided          bool
	proposal         []ActionLocal
	acceptedReceived uint64
}

func (p *PaxosInstance) Prepare(msg *PrepareMessage) *PromiseMessage {
	if msg.Round > p.lastRound && !p.decided {
		p.lastRound = msg.Round
		actions := toActionArray(p.proposal)
		return &PromiseMessage{LastGoodRound: p.lastGoodRound, Ack: true, Proposal: &Proposal{Actions: actions}}
	} else {
		return &PromiseMessage{LastGoodRound: p.lastGoodRound, Ack: false, Proposal: nil}
	}
}

func (p *PaxosInstance) Accept(msg *AcceptMessage) (*AcceptedMessage, bool) {
	ack := msg.Round >= p.lastRound || p.lastRound == 0
	if ack && !p.decided {
		p.lastRound = msg.Round
		p.lastGoodRound = msg.Round
		p.acceptedReceived = 0
		p.proposal = toActionLocalArray(msg.Proposal.Actions)
		return &AcceptedMessage{PaxosId: msg.PaxosId, Round: msg.Round, Proposal: msg.Proposal}, true
	}
	return nil, false
}

func (p *PaxosInstance) Accepted(msg *AcceptedMessage) bool {
	if msg.Round < p.lastGoodRound {
		return false
	}
	if msg.Round > p.lastGoodRound {
		p.proposal = toActionLocalArray(msg.Proposal.Actions)
		p.lastGoodRound = msg.Round
		p.acceptedReceived = 0
	}
	p.acceptedReceived++
	pastThreshold := p.acceptedReceived > config.PaxosMemberCount/2
	commit := !p.decided && pastThreshold
	p.decided = p.decided || pastThreshold
	return commit
}

func (p *PaxosInstance) IsDone() bool {
	return p.acceptedReceived == config.PaxosMemberCount
}

func NewPaxosInstance(requests []*PaxosLocalRequest) *PaxosInstance {
	return &PaxosInstance{lastRound: 0, lastGoodRound: 0, proposal: reqsToActionLocalArray(requests), decided: false}
}
