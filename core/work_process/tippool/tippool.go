package tippool

import (
	"sort"
	"sync"

	"github.com/lunfardo314/proxima/core/vertex"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/util/queue"
)

type (
	Environment interface {
		global.NodeGlobal
		GetStateReaderForTheBranch(branch *ledger.TransactionID) global.IndexedStateReader
		BranchHasTransaction(branchID, txid *ledger.TransactionID) (*multistate.RootRecord, bool)
	}

	Input struct {
		VID *vertex.WrappedTx
	}

	SequencerTips struct {
		*queue.Queue[Input]
		Environment
		mutex            sync.RWMutex
		latestMilestones map[ledger.ChainID]milestoneData
	}

	milestoneData struct {
		*vertex.WrappedTx
		BranchID ledger.TransactionID
	}

	RootData struct {
		BranchID   ledger.TransactionID
		RootRecord multistate.RootRecord
	}

	RootDataJSONAble struct {
		BranchID   string                        `json:"branch_id"`
		RootRecord multistate.RootRecordJSONAble `json:"root_record"`
	}

	TxInclusion struct {
		RootData
		Included bool
	}

	TxInclusionJSONAble struct {
		RootDataJSONAble `json:"root_data"`
		Included         bool `json:"included"`
	}
)

const (
	Name           = "tippool"
	TraceTag       = Name
	chanBufferSize = 10
)

func New(env Environment) *SequencerTips {
	return &SequencerTips{
		Queue:            queue.NewQueueWithBufferSize[Input](Name, chanBufferSize, env.Log().Level(), nil),
		Environment:      env,
		latestMilestones: make(map[ledger.ChainID]milestoneData),
	}
}

func (t *SequencerTips) Start() {
	t.MarkWorkProcessStarted(Name)
	t.AddOnClosed(func() {
		t.MarkWorkProcessStopped(Name)
	})
	t.Queue.Start(t, t.Environment.Ctx())
}

func (t *SequencerTips) Consume(inp Input) {
	seqIDIncoming, ok := inp.VID.SequencerIDIfAvailable()
	t.Assertf(ok, "sequencer milestone expected")
	t.Environment.Tracef(TraceTag, "seq milestone IN: %s of %s", inp.VID.IDShortString, seqIDIncoming.StringShort)

	t.mutex.Lock()
	defer t.mutex.Unlock()

	storedNew := false
	old, prevExists := t.latestMilestones[seqIDIncoming]
	if prevExists {
		if old.WrappedTx == inp.VID {
			// repeating, ignore
			return
		}
		if ledger.TooCloseOnTimeAxis(&old.ID, &inp.VID.ID) {
			t.Environment.Log().Warnf("tippool: %s and %s: too close on time axis", old.IDShortString(), inp.VID.IDShortString())
		}
		if t.oldReplaceWithNew(old.WrappedTx, inp.VID) {
			if inp.VID.Reference("tippool 1") {
				old.UnReference("tippool 1")
				t.latestMilestones[seqIDIncoming] = milestoneData{WrappedTx: inp.VID, BranchID: inp.VID.BaselineBranch().ID}
				storedNew = true
			}
		} else {
			t.Tracef(TraceTag, "tippool: incoming milestone %s didn't replace existing %s", inp.VID.IDShortString, old.IDShortString)
		}
	} else {
		if inp.VID.Reference("tippool 2") {
			t.latestMilestones[seqIDIncoming] = milestoneData{WrappedTx: inp.VID, BranchID: inp.VID.BaselineBranch().ID}
			storedNew = true
		}
	}
	prevStr := "<none>"
	if prevExists {
		prevStr = old.IDShortString()
	}
	if storedNew {
		t.Tracef(TraceTag, "new milestone stored in sequencer tippool: %s (prev: %s)", inp.VID.IDShortString, prevStr)
	}
}

// oldReplaceWithNew compares timestamps, chooses the younger one.
// If timestamps equal, chooses the preferred one, older is preferred
func (t *SequencerTips) oldReplaceWithNew(old, new *vertex.WrappedTx) bool {
	t.Assertf(old != new, "old != new")
	tsOld := old.Timestamp()
	tsNew := new.Timestamp()
	switch {
	case tsOld.Before(tsNew):
		return true
	case tsOld.After(tsNew):
		return false
	}
	t.Assertf(tsNew == tsOld, "tsNew==tsOld")
	return vertex.IsPreferredMilestoneAgainstTheOther(new, old, false)
}

func (t *SequencerTips) GetLatestMilestone(seqID ledger.ChainID) *vertex.WrappedTx {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.latestMilestones[seqID].WrappedTx
}

func (t *SequencerTips) LatestMilestonesDescending(filter ...func(seqID ledger.ChainID, vid *vertex.WrappedTx) bool) []*vertex.WrappedTx {
	flt := func(_ ledger.ChainID, _ *vertex.WrappedTx) bool { return true }
	if len(filter) > 0 {
		flt = filter[0]
	}

	t.mutex.RLock()
	defer t.mutex.RUnlock()

	ret := make([]*vertex.WrappedTx, 0, len(t.latestMilestones))
	for seqID, ms := range t.latestMilestones {
		if flt(seqID, ms.WrappedTx) {
			ret = append(ret, ms.WrappedTx)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return vertex.IsPreferredMilestoneAgainstTheOther(ret[i], ret[j], false)
	})
	return ret
}

func (t *SequencerTips) NumSequencerTips() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return len(t.latestMilestones)
}

func (t *SequencerTips) TxInclusion(txid *ledger.TransactionID) map[ledger.ChainID]TxInclusion {
	ret := make(map[ledger.ChainID]TxInclusion)

	t.mutex.RLock()
	defer t.mutex.RUnlock()

	for seqID, msData := range t.latestMilestones {
		rr, hasIt := t.BranchHasTransaction(&msData.BranchID, txid)
		t.Assertf(rr != nil, "inconsistency: root record is nil for branch %s", msData.BranchID.StringShort)

		ret[seqID] = TxInclusion{
			RootData: RootData{
				BranchID:   msData.BranchID,
				RootRecord: *rr,
			},
			Included: hasIt,
		}
	}
	return ret
}

func (t *SequencerTips) TxInclusionJSONAble(txid *ledger.TransactionID) map[string]TxInclusionJSONAble {
	ret := make(map[string]TxInclusionJSONAble)
	for seqID, incl := range t.TxInclusion(txid) {
		ret[seqID.StringHex()] = TxInclusionJSONAble{
			RootDataJSONAble: RootDataJSONAble{
				BranchID:   incl.BranchID.StringHex(),
				RootRecord: *incl.RootRecord.JSONAble(),
			},
			Included: incl.Included,
		}
	}
	return ret
}

func RootDataFromJSONAble(r *RootDataJSONAble) (*RootData, error) {
	ret := &RootData{
		RootRecord: multistate.RootRecord{},
	}
	var err error
	ret.BranchID, err = ledger.TransactionIDFromHexString(r.BranchID)
	if err != nil {
		return nil, err
	}
	rr, err := multistate.RootRecordFromJSONAble(&r.RootRecord)
	if err != nil {
		return nil, err
	}
	ret.RootRecord = *rr

	return ret, nil
}
