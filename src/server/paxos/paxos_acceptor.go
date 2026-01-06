package paxos

import (
	"errors"
	"log/slog"

	"server/config"
)

type ActionLocal struct {
	key      string
	value    *string
	revision *uint64
}

func toActionLocalArray(as *[]*Action) *[]ActionLocal {
	actions := make([]ActionLocal, len(*as))
	for i, a := range *as {
		actions[i] = a.toActionLocal()
	}
	return &actions
}

func toActionArray(as *[]ActionLocal) []*Action {
	actions := make([]*Action, len(*as))
	for i, a := range *as {
		action := a.toAction()
		actions[i] = &action
	}
	return actions
}

func reqsToActionArray(as *[]*LocalWriteRequest) []*Action {
	actions := make([]*Action, len(*as))
	for i, a := range *as {
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

func (req *LocalWriteRequest) toAction() Action {
	return Action{Key: req.key, Value: req.value, Revision: req.revision}
}

type PaxosInstance struct {
	lastRound        uint64
	lastGoodRound    uint64
	decided          bool
	done             bool
	preference       uint64
	proposals        []*[]ActionLocal
	acceptedReceived []uint64
}

func (p *PaxosInstance) getDecidedValue() *[]ActionLocal {
	return p.proposals[p.preference]
}

func (p *PaxosInstance) getActionsForRound(round uint64) (*Proposal, error) {
	if int(round) >= len(p.proposals) {
		return nil, errors.New("requested round not found")
	}
	proposal := p.proposals[round]
	if proposal == nil {
		return nil, errors.New("requested round not found")
	}
	return &Proposal{Actions: toActionArray(proposal)}, nil
}

func (p *PaxosInstance) FillInProposal(round uint64, res *Proposal) {
	p.proposals[round] = toActionLocalArray(&res.Actions)
}

func (p *PaxosInstance) missingProposal(round uint64) bool {
	return p.proposals[round] == nil && round == p.preference
}

func (p *PaxosInstance) extendBuffersTo(size int) {
	if len(p.proposals) > size {
		return
	}
	if size <= cap(p.proposals) {
		p.proposals = p.proposals[:size]
	} else {
		newProposals := make([]*[]ActionLocal, size)
		copy(newProposals, p.proposals)
		p.proposals = newProposals
	}
	if size <= cap(p.acceptedReceived) {
		p.acceptedReceived = p.acceptedReceived[:size]
	} else {
		newAcceptedReceived := make([]uint64, size)
		copy(newAcceptedReceived, p.acceptedReceived)
		p.acceptedReceived = newAcceptedReceived
	}
}

func (p *PaxosInstance) Prepare(msg *PrepareMessage) *PromiseMessage {
	if msg.Round > p.lastRound && !p.decided {
		p.lastRound = msg.Round
		actions := toActionArray(p.proposals[p.preference])
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
		p.extendBuffersTo(int(msg.Round) + 1)
		if p.missingProposal(msg.Round) {
			p.proposals[msg.Round] = toActionLocalArray(&msg.Proposal.Actions)
		}
		p.preference = msg.Round
		return &AcceptedMessage{PaxosId: msg.PaxosId, Round: msg.Round, SenderId: config.PaxosMyID}, true
	}
	return nil, false
}

func (p *PaxosInstance) Accepted(msg *AcceptedMessage) bool {
	p.extendBuffersTo(int(msg.Round) + 1)
	p.acceptedReceived[msg.Round]++
	pastThreshold := p.acceptedReceived[msg.Round] > config.PaxosMemberCount/2
	commit := !p.decided && pastThreshold
	if commit {
		p.decided = true
		p.preference = msg.Round
	}
	slog.Debug("Accepted received for this round", slog.Uint64("count", p.acceptedReceived[msg.Round]))
	p.done = p.done || p.acceptedReceived[msg.Round] == config.PaxosMemberCount
	return commit
}

func NewPaxosInstance() *PaxosInstance {
	proposals := make([]*[]ActionLocal, 2)
	return &PaxosInstance{proposals: proposals, preference: 1}
}
