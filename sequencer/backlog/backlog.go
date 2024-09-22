package backlog

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lunfardo314/proxima/core/attacher"
	"github.com/lunfardo314/proxima/core/vertex"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/set"
)

type (
	Environment interface {
		global.NodeGlobal
		attacher.Environment
		ListenToAccount(account ledger.Accountable, fun func(wOut vertex.WrappedOutput))
		SequencerID() ledger.ChainID
		SequencerName() string
		GetLatestMilestone(seqID ledger.ChainID) *vertex.WrappedTx
		LatestMilestonesDescending(filter ...func(seqID ledger.ChainID, vid *vertex.WrappedTx) bool) []*vertex.WrappedTx
		NumSequencerTips() int
		BacklogTTLSlots() int
		MustEnsureBranch(txid ledger.TransactionID) *vertex.WrappedTx
	}

	InputBacklog struct {
		Environment
		mutex                    sync.RWMutex
		outputs                  map[vertex.WrappedOutput]time.Time
		outputCount              int
		removedOutputsSinceReset int
		lastOutputArrived        time.Time
	}

	Stats struct {
		NumOtherSequencers       int
		NumOutputs               int
		OutputCount              int
		RemovedOutputsSinceReset int
	}
)

// TODO tag-along and delegation locks

const TraceTag = "backlog"

func New(env Environment) (*InputBacklog, error) {
	seqID := env.SequencerID()
	ret := &InputBacklog{
		Environment: env,
		outputs:     make(map[vertex.WrappedOutput]time.Time),
	}
	env.Tracef(TraceTag, "starting input backlog for the sequencer %s..", env.SequencerName)

	// start listening to chain-locked account
	env.ListenToAccount(seqID.AsChainLock(), func(wOut vertex.WrappedOutput) {
		env.Tracef(TraceTag, "[%s] output IN: %s", ret.SequencerName, wOut.IDShortString)
		env.TraceTx(&wOut.VID.ID, "[%s] backlog: output #%d IN", ret.SequencerName, wOut.Index)

		if !ret.checkAndReferenceCandidate(wOut) {
			// failed to reference -> ignore
			return
		}
		// referenced
		ret.mutex.Lock()
		defer ret.mutex.Unlock()

		if _, already := ret.outputs[wOut]; already {
			wOut.VID.UnReference()
			env.Tracef(TraceTag, "repeating output %s", wOut.IDShortString)
			env.TraceTx(&wOut.VID.ID, "[%s] output #%d is already in the backlog", ret.SequencerName, wOut.Index)
			return
		}
		nowis := time.Now()
		ret.outputs[wOut] = nowis
		ret.lastOutputArrived = nowis
		ret.outputCount++
		env.Tracef(TraceTag, "output stored in input backlog: %s (total: %d)", wOut.IDShortString, len(ret.outputs))
		env.TraceTx(&wOut.VID.ID, "[%s] output #%d stored in the backlog", ret.SequencerName, wOut.Index)
	})

	ttlInBacklog := time.Duration(env.BacklogTTLSlots()) * ledger.L().ID.SlotDuration()
	env.RepeatInBackground(env.SequencerName()+"_backlogPurge", time.Second, func() bool {
		if n := ret.purgeBacklog(ttlInBacklog); n > 0 {
			ret.Log().Infof("purged %d outputs from the backlog", n)
		}
		return true
	})

	return ret, nil
}

func (b *InputBacklog) ArrivedOutputsSince(t time.Time) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.lastOutputArrived.After(t)
}

// checkAndReferenceCandidate if returns false, it is unreferenced, otherwise referenced
func (b *InputBacklog) checkAndReferenceCandidate(wOut vertex.WrappedOutput) bool {
	if wOut.VID.IsBranchTransaction() {
		// outputs of branch transactions are filtered out
		// TODO probably ordinary outputs must not be allowed at ledger constraints level
		b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: is branch", b.SequencerName, wOut.Index)
		return false
	}
	if !wOut.VID.Reference() {
		b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: failed to reference", b.SequencerName, wOut.Index)
		return false
	}
	if wOut.VID.GetTxStatus() == vertex.Bad {
		wOut.VID.UnReference()
		b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: is BAD", b.SequencerName, wOut.Index)
		return false
	}
	o, err := wOut.VID.OutputAt(wOut.Index)
	if err != nil {
		b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: OutputAt failed for #%d: %v", b.SequencerName, wOut.Index, err)
		wOut.VID.UnReference()
		return false
	}
	if o != nil {
		if _, idx := o.ChainConstraint(); idx != 0xff {
			// filter out all chain constrained outputs
			// TODO must be revisited with delegated accounts (delegation-locked on the current sequencer)
			b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: #%d is chain-constrained", b.SequencerName, wOut.Index)
			wOut.VID.UnReference()
			return false
		}
	}
	// it is referenced
	b.TraceTx(&wOut.VID.ID, "[%s] backlog::checkAndReferenceCandidate: #%d success", b.SequencerName, wOut.Index)
	return true
}

// CandidatesToEndorseSorted returns list of transactions which can be endorsed from the given timestamp
func (b *InputBacklog) CandidatesToEndorseSorted(targetTs ledger.Time) []*vertex.WrappedTx {
	targetSlot := targetTs.Slot()
	ownSeqID := b.SequencerID()
	return b.LatestMilestonesDescending(func(seqID ledger.ChainID, vid *vertex.WrappedTx) bool {
		if vid.BaselineBranch() == nil {
			//b.Log().Warnf("InputBacklog: milestone %s has BaselineBranch == nil", vid.IDShortString())
			return false
		}
		return vid.Slot() == targetSlot && seqID != ownSeqID
	})
}

func (b *InputBacklog) GetOwnLatestMilestoneTx() *vertex.WrappedTx {
	return b.GetLatestMilestone(b.SequencerID())
}

func (b *InputBacklog) FilterAndSortOutputs(filter func(wOut vertex.WrappedOutput) bool) []vertex.WrappedOutput {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	ret := util.KeysFiltered(b.outputs, filter)
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Timestamp().Before(ret[j].Timestamp())
	})
	return ret
}

func (b *InputBacklog) NumOutputsInBuffer() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.outputs)
}

func (b *InputBacklog) getStatsAndReset() (ret Stats) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	ret = Stats{
		NumOtherSequencers:       b.NumSequencerTips(),
		NumOutputs:               len(b.outputs),
		OutputCount:              b.outputCount,
		RemovedOutputsSinceReset: b.removedOutputsSinceReset,
	}
	b.removedOutputsSinceReset = 0
	return
}

func (b *InputBacklog) numOutputs() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.outputs)
}

func (b *InputBacklog) purgeBacklog(ttl time.Duration) int {
	horizon := time.Now().Add(-ttl)

	b.mutex.Lock()
	defer b.mutex.Unlock()

	toDelete := make([]vertex.WrappedOutput, 0)
	for wOut, since := range b.outputs {
		if since.Before(horizon) {
			toDelete = append(toDelete, wOut)
		}
	}

	for _, wOut := range toDelete {
		wOut.VID.UnReference()
		delete(b.outputs, wOut)
		b.TraceTx(&wOut.VID.ID, "[%s] output #%d has been deleted from the backlog", b.SequencerName, wOut.Index)
	}
	return len(toDelete)
}

// LoadSequencerStartTips loads tip transactions relevant to the sequencer startup from persistent state to the memDAG
func (b *InputBacklog) LoadSequencerStartTips(seqID ledger.ChainID) error {
	var branchData *multistate.BranchData
	if b.IsBootstrapMode() {
		branchData = multistate.FindLatestReliableBranchWithSequencerID(b.StateStore(), b.SequencerID(), global.FractionHealthyBranch)
	} else {
		branchData = multistate.FindLatestReliableBranch(b.StateStore(), global.FractionHealthyBranch)
	}
	if branchData == nil {
		return fmt.Errorf("LoadSequencerStartTips: can't find latest reliable branch (LRB) with franction %s", global.FractionHealthyBranch.String())
	}
	loadedTxs := set.New[*vertex.WrappedTx]()
	nowSlot := ledger.TimeNow().Slot()
	b.Log().Infof("loading sequencer tips for %s from branch %s, %d slots back from (current slot is %d), bootstrap mode: %v",
		seqID.StringShort(), branchData.TxID().StringShort(), nowSlot-branchData.TxID().Slot(), nowSlot, b.IsBootstrapMode())

	rdr := multistate.MustNewSugaredReadableState(b.StateStore(), branchData.Root, 0)
	vidBranch := b.MustEnsureBranch(branchData.Stem.ID.TransactionID())
	b.PostEventNewGood(vidBranch)
	loadedTxs.Insert(vidBranch)

	// load sequencer output for the chain
	chainOut, stemOut, err := rdr.GetChainTips(&seqID)
	if err != nil {
		return fmt.Errorf("LoadSequencerStartTips: %w", err)
	}
	var wOut vertex.WrappedOutput
	if chainOut.ID.IsSequencerTransaction() {
		wOut, _, err = attacher.AttachSequencerOutputs(chainOut, stemOut, b, attacher.WithInvokedBy("LoadSequencerStartTips"))
	} else {
		wOut, err = attacher.AttachOutputWithID(chainOut, b, attacher.WithInvokedBy("LoadSequencerStartTips"))
	}
	if err != nil {
		return err
	}
	loadedTxs.Insert(wOut.VID)

	b.Log().Infof("loaded sequencer start output from branch %s\n%s",
		vidBranch.IDShortString(), chainOut.Lines("         ").String())

	// load pending tag-along outputs
	oids, err := rdr.GetIDsLockedInAccount(seqID.AsChainLock().AccountID())
	util.AssertNoError(err)
	for _, oid := range oids {
		o := rdr.MustGetOutputWithID(&oid)
		wOut, err = attacher.AttachOutputWithID(o, b, attacher.WithInvokedBy("LoadSequencerStartTips"))
		if err != nil {
			return err
		}
		b.Log().Infof("loaded tag-along input for sequencer %s: %s from branch %s", seqID.StringShort(), oid.StringShort(), vidBranch.IDShortString())
		loadedTxs.Insert(wOut.VID)
	}
	// post new tx event for each transaction
	for vid := range loadedTxs {
		b.PostEventNewTransaction(vid)
	}
	return nil
}
