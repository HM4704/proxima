package vertex

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"time"

	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"github.com/lunfardo314/proxima/util/set"
	"golang.org/x/exp/maps"
)

// ErrDeletedVertexAccessed exception is raised by PanicAccessDeleted handler of RUnwrap vertex so that could be caught if necessary
var (
	ErrDeletedVertexAccessed = errors.New("deleted vertex should not be accessed")
)

func (v _vertex) _time() time.Time {
	return v.whenWrapped
}

func (v _vertex) _outputAt(idx byte) (*ledger.Output, error) {
	return v.Tx.ProducedOutputAt(idx)
}

func (v _vertex) _hasOutputAt(idx byte) (bool, bool) {
	if int(idx) >= v.Tx.NumProducedOutputs() {
		return false, true
	}
	return true, false
}

func (v _virtualTx) _time() time.Time {
	return time.Time{}
}

func (v _virtualTx) _outputAt(idx byte) (*ledger.Output, error) {
	if o, available := v.OutputAt(idx); available {
		return o, nil
	}
	return nil, nil
}

func (v _virtualTx) _hasOutputAt(idx byte) (bool, bool) {
	_, hasIt := v.OutputAt(idx)
	return hasIt, false
}

func (v _deletedTx) _time() time.Time {
	return time.Time{}
}

func (v _deletedTx) _outputAt(_ byte) (*ledger.Output, error) {
	return nil, ErrDeletedVertexAccessed
}

func (v _deletedTx) _hasOutputAt(_ byte) (bool, bool) {
	return false, false
}

var nopFun = func(_ *WrappedTx) {}

func _newVID(g _genericWrapper, txid ledger.TransactionID) *WrappedTx {
	ret := &WrappedTx{
		ID:              txid,
		_genericWrapper: g,
		references:      1, // we always start with 1 reference, which is reference by the DAG itself. 0 references means it is deleted
	}
	ret.onPoke.Store(nopFun)
	return ret
}

func (vid *WrappedTx) _put(g _genericWrapper) {
	vid._genericWrapper = g
}

func (vid *WrappedTx) FlagsNoLock() Flags {
	return vid.flags
}

func (vid *WrappedTx) SetFlagsUpNoLock(f Flags) {
	vid.flags = vid.flags | f
}

func (vid *WrappedTx) FlagsUp(f Flags) bool {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid.flags&f == f
}

func (vid *WrappedTx) FlagsUpNoLock(f Flags) bool {
	return vid.flags&f == f
}

func (vid *WrappedTx) ConvertVirtualTxToVertexNoLock(v *Vertex) {
	util.Assertf(vid.ID == *v.Tx.ID(), "ConvertVirtualTxToVertexNoLock: txid-s do not match in: %s", vid.ID.StringShort)
	_, isVirtualTx := vid._genericWrapper.(_virtualTx)
	util.Assertf(isVirtualTx, "ConvertVirtualTxToVertexNoLock: virtual tx target expected %s", vid.ID.StringShort)
	vid._put(_vertex{Vertex: v})
}

func (vid *WrappedTx) ConvertBranchVertexToVirtualTx() {
	vid.Unwrap(UnwrapOptions{Vertex: func(_ *Vertex) {

	}})
}

func (vid *WrappedTx) MarkDeleted() {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	switch vid._genericWrapper.(type) {
	case _vertex:
		vid._put(_deletedTx{})
	case _virtualTx:
		vid._put(_deletedTx{})
	case _deletedTx:
		vid.PanicAccessDeleted()
	}
}

func (vid *WrappedTx) GetTxStatus() Status {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid.GetTxStatusNoLock()
}

func (vid *WrappedTx) GetTxStatusNoLock() Status {
	if !vid.flags.FlagsUp(FlagVertexDefined) {
		util.Assertf(vid.err == nil, "vid.err == nil")
		return Undefined
	}
	if vid.err != nil {
		return Bad
	}
	return Good
}

func (vid *WrappedTx) SetTxStatusGood() {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	util.Assertf(vid.GetTxStatusNoLock() == Undefined, "vid.GetTxStatusNoLock() == Undefined")
	vid.flags.SetFlagsUp(FlagVertexDefined)
}

func (vid *WrappedTx) SetTxStatusBad(reason error) {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	vid.SetTxStatusBadNoLock(reason)
}

func (vid *WrappedTx) SetTxStatusBadNoLock(reason error) {
	util.Assertf(vid.GetTxStatusNoLock() == Undefined, "vid.GetTxStatusNoLock() == Undefined")
	vid.flags.SetFlagsUp(FlagVertexDefined)
	vid.err = reason
}

func (vid *WrappedTx) SetTxStatusGoodNoLock() {
	util.Assertf(vid.err == nil, "vid.err == nil")
	vid.flags.SetFlagsUp(FlagVertexDefined)
}

func (vid *WrappedTx) GetError() error {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid.err
}

func (vid *WrappedTx) GetErrorNoLock() error {
	return vid.err
}

func (vid *WrappedTx) Reference() bool {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	if vid.references == 0 {
		return false
	}
	util.Assertf(vid.references < math.MaxUint16, "Reference: overflow in %s", vid.ID.StringShort)
	vid.references++
	return true
}

func (vid *WrappedTx) UnReference() {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	util.Assertf(vid.references > 0, "UnReference: vertex is deleted: %s", vid.ID.StringShort)
	vid.references--
}

// IsBadOrDeleted non-deterministic
func (vid *WrappedTx) IsBadOrDeleted() bool {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	if vid.GetTxStatusNoLock() == Bad {
		return true
	}
	_, isDeleted := vid._genericWrapper.(_deletedTx)
	return isDeleted
}

func (vid *WrappedTx) IsDeleted() bool {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid.references == 0
}

func (vid *WrappedTx) StatusString() string {
	r := vid.GetError()

	switch s := vid.GetTxStatus(); s {
	case Good, Undefined:
		return s.String()
	default:
		return fmt.Sprintf("%s('%v')", s.String(), r)
	}
}

func (vid *WrappedTx) OnPoke(fun func(vid *WrappedTx)) {
	if fun == nil {
		vid.onPoke.Store(nopFun)
	} else {
		vid.onPoke.Store(fun)
	}
}

func (vid *WrappedTx) PokeWith(withVID *WrappedTx) {
	vid.onPoke.Load().(func(_ *WrappedTx))(withVID)
}

func WrapTxID(txid ledger.TransactionID) *WrappedTx {
	return _newVID(_virtualTx{
		VirtualTransaction: newVirtualTx(),
	}, txid)
}

// TODO do we need those functions just accessing vid.ID?

func (vid *WrappedTx) ShortString() string {
	var mode, status, reason string
	flagsStr := ""
	vid.Unwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			mode = "vertex"
			flagsStr = fmt.Sprintf(", %08b", vid.flags)
			status = vid.GetTxStatusNoLock().String()
			if vid.err != nil {
				reason = fmt.Sprintf(" err: '%v'", vid.err)
			}
		},
		VirtualTx: func(v *VirtualTransaction) {
			mode = "virtualTx"
			status = vid.GetTxStatusNoLock().String()
			if vid.err != nil {
				reason = fmt.Sprintf(" err: '%v'", vid.err)
			}
		},
		Deleted: vid.PanicAccessDeleted,
	})
	return fmt.Sprintf("%22s %10s (%s%s) %s", vid.IDShortString(), mode, status, flagsStr, reason)
}

func (vid *WrappedTx) IDShortString() string {
	return vid.ID.StringShort()
}

func (vid *WrappedTx) IDVeryShort() string {
	return vid.ID.StringVeryShort()
}

func (vid *WrappedTx) IDShortStringExt() string {
	ret := vid.ID.StringShort()
	if !vid.IsSequencerMilestone() {
		return ret
	}
	chainID, ok := vid.SequencerIDIfAvailable()
	if !ok {
		return ret + "/$??"
	} else {
		return ret + "/" + chainID.StringShort()
	}
}

func (vid *WrappedTx) IsBranchTransaction() bool {
	return vid.ID.IsBranchTransaction()
}

func (vid *WrappedTx) IsSequencerMilestone() bool {
	return vid.ID.IsSequencerMilestone()
}

func (vid *WrappedTx) Timestamp() ledger.Time {
	return vid.ID.Timestamp()
}

func (vid *WrappedTx) Slot() ledger.Slot {
	return vid.ID.Slot()
}

func (vid *WrappedTx) OutputWithIDAt(idx byte) (ledger.OutputWithID, error) {
	ret, err := vid.OutputAt(idx)
	if err != nil || ret == nil {
		return ledger.OutputWithID{}, err
	}
	return ledger.OutputWithID{
		ID:     ledger.NewOutputID(&vid.ID, idx),
		Output: ret,
	}, nil
}

func (vid *WrappedTx) MustOutputWithIDAt(idx byte) (ret ledger.OutputWithID) {
	var err error
	ret, err = vid.OutputWithIDAt(idx)
	util.AssertNoError(err)
	return
}

// OutputAt return output at index, if available.
// err != nil indicates wrong index
// nil, nil means output not available, but no error (orphaned)
func (vid *WrappedTx) OutputAt(idx byte) (*ledger.Output, error) {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid._outputAt(idx)
}

func (vid *WrappedTx) MustOutputAt(idx byte) *ledger.Output {
	ret, err := vid.OutputAt(idx)
	util.AssertNoError(err)
	return ret
}

func (vid *WrappedTx) HasOutputAt(idx byte) (bool, bool) {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid._hasOutputAt(idx)
}

func (vid *WrappedTx) SequencerIDIfAvailable() (ledger.ChainID, bool) {
	var isAvailable bool
	var ret ledger.ChainID
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			isAvailable = v.Tx.IsSequencerMilestone()
			if isAvailable {
				ret = v.Tx.SequencerTransactionData().SequencerID
			}
		},
		VirtualTx: func(v *VirtualTransaction) {
			if v.sequencerOutputs != nil {
				seqOData, ok := v.outputs[v.sequencerOutputs[0]].SequencerOutputData()
				util.Assertf(ok, "sequencer output data unavailable for the output #%d", v.sequencerOutputs[0])
				ret = seqOData.ChainConstraint.ID
				if ret == ledger.NilChainID {
					oid := vid.OutputID(v.sequencerOutputs[0])
					ret = ledger.MakeOriginChainID(&oid)
				}
				isAvailable = true
			}
		},
	})
	return ret, isAvailable
}

func (vid *WrappedTx) MustSequencerIDAndStemID() (seqID ledger.ChainID, stemID ledger.OutputID) {
	util.Assertf(vid.IsBranchTransaction(), "vid.IsBranchTransaction()")
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			seqID = v.Tx.SequencerTransactionData().SequencerID
			stemID = vid.OutputID(v.Tx.SequencerTransactionData().StemOutputIndex)
		},
		VirtualTx: func(v *VirtualTransaction) {
			util.Assertf(v.sequencerOutputs != nil, "v.sequencerOutputs != nil")
			seqOData, ok := v.outputs[v.sequencerOutputs[0]].SequencerOutputData()
			util.Assertf(ok, "sequencer output data unavailable for the output #%d", v.sequencerOutputs[0])
			seqID = seqOData.ChainConstraint.ID
			if seqID == ledger.NilChainID {
				oid := vid.OutputID(v.sequencerOutputs[0])
				seqID = ledger.MakeOriginChainID(&oid)
			}
			stemID = vid.OutputID(v.sequencerOutputs[1])
		},
	})
	return
}

func (vid *WrappedTx) MustSequencerID() ledger.ChainID {
	ret, ok := vid.SequencerIDIfAvailable()
	util.Assertf(ok, "not a sequencer milestone")
	return ret
}

func (vid *WrappedTx) SequencerWrappedOutput() (ret WrappedOutput) {
	util.Assertf(vid.IsSequencerMilestone(), "vid.IsSequencerMilestone()")

	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			if seqData := v.Tx.SequencerTransactionData(); seqData != nil {
				ret = WrappedOutput{
					VID:   vid,
					Index: v.Tx.SequencerTransactionData().SequencerOutputIndex,
				}
			}
		},
		VirtualTx: func(v *VirtualTransaction) {
			if v.sequencerOutputs != nil {
				ret = WrappedOutput{
					VID:   vid,
					Index: v.sequencerOutputs[0],
				}
			}
		},
	})
	return
}

func (vid *WrappedTx) StemWrappedOutput() (ret WrappedOutput) {
	util.Assertf(vid.IsBranchTransaction(), "vid.IsBranchTransaction()")

	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			if seqData := v.Tx.SequencerTransactionData(); seqData != nil {
				ret = WrappedOutput{
					VID:   vid,
					Index: v.Tx.SequencerTransactionData().StemOutputIndex,
				}
			}
		},
		VirtualTx: func(v *VirtualTransaction) {
			if v.sequencerOutputs != nil {
				ret = WrappedOutput{
					VID:   vid,
					Index: v.sequencerOutputs[1],
				}
			}
		},
	})
	return
}

func (vid *WrappedTx) IsVertex() (ret bool) {
	vid.RUnwrap(UnwrapOptions{Vertex: func(_ *Vertex) {
		ret = true
	}})
	return
}

func (vid *WrappedTx) IsVirtualTx() (ret bool) {
	vid.RUnwrap(UnwrapOptions{VirtualTx: func(_ *VirtualTransaction) {
		ret = true
	}})
	return
}

func (vid *WrappedTx) OutputID(idx byte) (ret ledger.OutputID) {
	ret = ledger.NewOutputID(&vid.ID, idx)
	return
}

func (vid *WrappedTx) Unwrap(opt UnwrapOptions) {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	vid._unwrap(opt)
}

func (vid *WrappedTx) RUnwrap(opt UnwrapOptions) {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	vid._unwrap(opt)
}

func (vid *WrappedTx) _unwrap(opt UnwrapOptions) {

	switch v := vid._genericWrapper.(type) {
	case _vertex:
		if opt.Vertex != nil {
			opt.Vertex(v.Vertex)
		}
	case _virtualTx:
		if opt.VirtualTx != nil {
			opt.VirtualTx(v.VirtualTransaction)
		}
	case _deletedTx:
		if opt.Deleted != nil {
			opt.Deleted()
		}
	default:
		util.Assertf(false, "inconsistency: unsupported wrapped type")
	}
}

func (vid *WrappedTx) Time() time.Time {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid._genericWrapper._time()
}

func (vid *WrappedTx) Lines(prefix ...string) *lines.Lines {
	ret := lines.New(prefix...)
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			ret.Add("== vertex %s", vid.IDShortString())
			ret.Append(v.Lines(prefix...))
		},
		VirtualTx: func(v *VirtualTransaction) {
			ret.Add("== virtual tx %s", vid.IDShortString())
			idxs := util.SortKeys(v.outputs, func(k1, k2 byte) bool {
				return k1 < k2
			})
			for _, i := range idxs {
				ret.Add("    #%d :", i)
				ret.Append(v.outputs[i].Lines("     "))
			}
		},
		Deleted: func() {
			ret.Add("== orphaned vertex")
		},
	})
	return ret
}

func (vid *WrappedTx) NumInputs() int {
	ret := 0
	vid.RUnwrap(UnwrapOptions{Vertex: func(v *Vertex) {
		ret = v.Tx.NumInputs()
	}})
	return ret
}

func (vid *WrappedTx) NumProducedOutputs() int {
	ret := 0
	vid.RUnwrap(UnwrapOptions{Vertex: func(v *Vertex) {
		ret = v.Tx.NumProducedOutputs()
	}})
	return ret
}

func (vid *WrappedTx) ConvertToVirtualTx() {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	switch v := vid._genericWrapper.(type) {
	case _vertex:
		vid._put(_virtualTx{VirtualTransaction: v.convertToVirtualTx()})
	case _deletedTx:
		vid.PanicAccessDeleted()
	}
}

func (vid *WrappedTx) PanicAccessDeleted() {
	fmt.Printf(">>>>>>>>>>>>>>>>> PanicAccessDeleted:\n%s\n<<<<<<<<<<<<<<<<<<<<<<<\n", string(debug.Stack()))
	util.Panicf("%w: %s", ErrDeletedVertexAccessed, vid.ID.StringShort())
}

func (vid *WrappedTx) BaselineBranch() (baselineBranch *ledger.TransactionID) {
	if vid.ID.IsBranchTransaction() {
		return &vid.ID
	}
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			baselineBranch = v.BaselineBranch
		},
	})
	return
}

func (vid *WrappedTx) EnsureOutput(idx byte, o *ledger.Output) bool {
	ok := true
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			if idx >= byte(v.Tx.NumProducedOutputs()) {
				ok = false
				return
			}
			util.Assertf(bytes.Equal(o.Bytes(), v.Tx.MustProducedOutputAt(idx).Bytes()), "EnsureOutput: inconsistent output data")
		},
		VirtualTx: func(v *VirtualTransaction) {
			v.addOutput(idx, o)
		},
		Deleted: vid.PanicAccessDeleted,
	})
	return ok
}

func (vid *WrappedTx) AttachConsumer(outputIndex byte, consumer *WrappedTx, checkConflicts func(existingConsumers set.Set[*WrappedTx]) (conflict bool)) bool {
	vid.mutexDescendants.Lock()
	defer vid.mutexDescendants.Unlock()

	if vid.consumed == nil {
		vid.consumed = make(map[byte]set.Set[*WrappedTx])
	}
	outputConsumers := vid.consumed[outputIndex]
	if outputConsumers == nil {
		outputConsumers = set.New(consumer)
	} else {
		outputConsumers.Insert(consumer)
	}
	vid.consumed[outputIndex] = outputConsumers
	conflict := checkConflicts(outputConsumers)
	return !conflict
}

func (vid *WrappedTx) NotConsumedOutputIndices(allConsumers set.Set[*WrappedTx]) []byte {
	vid.mutexDescendants.Lock()
	defer vid.mutexDescendants.Unlock()

	nOutputs := 0
	vid.RUnwrap(UnwrapOptions{Vertex: func(v *Vertex) {
		nOutputs = v.Tx.NumProducedOutputs()
	}})

	ret := make([]byte, 0, nOutputs)

	for i := 0; i < nOutputs; i++ {
		if set.DoNotIntersect(vid.consumed[byte(i)], allConsumers) {
			ret = append(ret, byte(i))
		}
	}
	return ret
}

func (vid *WrappedTx) GetLedgerCoverage() *multistate.LedgerCoverage {
	vid.mutex.RLock()
	defer vid.mutex.RUnlock()

	return vid.coverage
}

func (vid *WrappedTx) SetLedgerCoverage(coverage multistate.LedgerCoverage) {
	vid.mutex.Lock()
	defer vid.mutex.Unlock()

	vid.coverage = &coverage
}

func (vid *WrappedTx) LedgerCoverageSum() uint64 {
	ret := vid.GetLedgerCoverage()
	return ret.Sum()
}

// LessByCoverageAndID compares sum of ledger coverages. If equal, compares IDs
// It means, earlier is less
func LessByCoverageAndID(vid1, vid2 *WrappedTx) bool {
	c1 := vid1.GetLedgerCoverage().Sum()
	c2 := vid2.GetLedgerCoverage().Sum()
	if c1 == c2 {
		return ledger.LessTxID(vid1.ID, vid2.ID)
	}
	return c1 < c2
}

// NumConsumers returns:
// - number of consumed outputs
// - number of conflict sets
func (vid *WrappedTx) NumConsumers() (int, int) {
	vid.mutexDescendants.RLock()
	defer vid.mutexDescendants.RUnlock()

	retConsumed := len(vid.consumed)
	retCS := 0
	for _, ds := range vid.consumed {
		if len(ds) > 1 {
			retCS++
		}
	}
	return retConsumed, retCS
}

func (vid *WrappedTx) ConsumersOf(outIdx byte) set.Set[*WrappedTx] {
	vid.mutexDescendants.RLock()
	defer vid.mutexDescendants.RUnlock()

	return vid.consumed[outIdx].Clone()
}

func (vid *WrappedTx) String() (ret string) {
	consumed, doubleSpent := vid.NumConsumers()
	reason := vid.GetError()
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			t := "vertex (" + vid.GetTxStatusNoLock().String() + ")"
			ret = fmt.Sprintf("%20s %s :: in: %d, out: %d, consumed: %d, conflicts: %d, Flags: %08b, err: '%v', cov: %s",
				t,
				vid.ID.StringShort(),
				v.Tx.NumInputs(),
				v.Tx.NumProducedOutputs(),
				consumed,
				doubleSpent,
				vid.flags,
				reason,
				vid.coverage.String(),
			)
		},
		VirtualTx: func(v *VirtualTransaction) {
			t := "virtualTx (" + vid.GetTxStatus().String() + ")"

			v.mutex.RLock()
			defer v.mutex.RUnlock()

			ret = fmt.Sprintf("%20s %s:: out: %d, consumed: %d, conflicts: %d, flags: %08b, err: %v",
				t,
				vid.ID.StringShort(),
				len(v.outputs),
				consumed,
				doubleSpent,
				vid.flags,
				reason,
			)
		},
		Deleted: vid.PanicAccessDeleted,
	})
	return
}

func (vid *WrappedTx) WrappedInputs() (ret []WrappedOutput) {
	vid.Unwrap(UnwrapOptions{Vertex: func(v *Vertex) {
		ret = make([]WrappedOutput, v.Tx.NumInputs())
		v.ForEachInputDependency(func(i byte, inp *WrappedTx) bool {
			inpID := v.Tx.MustInputAt(i)
			ret[i] = WrappedOutput{
				VID:   inp,
				Index: inpID.Index(),
			}
			return true
		})
	}})
	return ret
}

func (vid *WrappedTx) SequencerPredecessor() (ret *WrappedTx) {
	vid.Unwrap(UnwrapOptions{Vertex: func(v *Vertex) {
		if seqData := v.Tx.SequencerTransactionData(); seqData != nil {
			ret = v.Inputs[seqData.SequencerOutputData.ChainConstraint.PredecessorInputIndex]
		}
	}})
	return
}

func (vid *WrappedTx) LinesTx(prefix ...string) *lines.Lines {
	ret := lines.New()
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			ret.Append(v.Tx.LinesShort(prefix...))
		},
		VirtualTx: func(v *VirtualTransaction) {
			ret.Add("a virtual tx %s", vid.IDShortString())
		},
		Deleted: func() {
			ret.Add("deleted tx %s", vid.IDShortString())
		},
	})
	return ret
}

func VerticesLines(vertices []*WrappedTx, prefix ...string) *lines.Lines {
	ret := lines.New(prefix...)
	for _, vid := range vertices {
		ret.Add(vid.String())
	}
	return ret
}

func VIDSetIDString(set set.Set[*WrappedTx], prefix ...string) string {
	ret := lines.New(prefix...)
	for _, vid := range maps.Keys(set) {
		ret.Add(vid.IDShortString())
	}
	return ret.Join(", ")
}

func VerticesShortLines(vertices []*WrappedTx, prefix ...string) *lines.Lines {
	ret := lines.New(prefix...)
	for _, vid := range vertices {
		ret.Add(vid.IDShortStringExt())
	}
	return ret
}

func VerticesIDLines(vertices []*WrappedTx, prefix ...string) *lines.Lines {
	ret := lines.New(prefix...)
	for _, vid := range vertices {
		ret.Add(vid.IDShortString())
	}
	return ret
}

type _unwrapOptionsTraverse struct {
	UnwrapOptionsForTraverse
	visited set.Set[*WrappedTx]
}

// TraversePastConeDepthFirst performs depth-first traverse of the DAG. Visiting once each node
// and calling vertex-type specific function if provided on each.
// If function returns false, the traverse is cancelled globally.
// The traverse stops at terminal dag. The vertex is terminal if it either is not-full vertex
// i.e. (booked, orphaned, deleted) or it belongs to 'visited' set
// If 'visited' set is provided at call, it is mutable. In the end it contains all initial dag plus
// all dag visited during the traverse
func (vid *WrappedTx) TraversePastConeDepthFirst(opt UnwrapOptionsForTraverse, visited ...set.Set[*WrappedTx]) {
	var visitedSet set.Set[*WrappedTx]
	if len(visited) > 0 {
		visitedSet = visited[0]
	} else {
		visitedSet = set.New[*WrappedTx]()
	}
	vid._traversePastCone(&_unwrapOptionsTraverse{
		UnwrapOptionsForTraverse: opt,
		visited:                  visitedSet,
	})
}

func (vid *WrappedTx) _traversePastCone(opt *_unwrapOptionsTraverse) bool {
	if opt.visited.Contains(vid) {
		return true
	}
	opt.visited.Insert(vid)

	ret := true
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			v.ForEachInputDependency(func(i byte, inp *WrappedTx) bool {
				if inp == nil {
					return true
				}
				//util.Assertf(inp != nil, "_traversePastCone: input %d is nil (not solidified) in %s",
				//	i, func() any { return v.Tx.IDShortString() })
				ret = inp._traversePastCone(opt)
				return ret
			})
			if ret {
				v.ForEachEndorsement(func(i byte, inpEnd *WrappedTx) bool {
					if inpEnd == nil {
						return true
					}
					//util.Assertf(inpEnd != nil, "_traversePastCone: endorsement %d is nil (not solidified) in %s",
					//	i, func() any { return v.Tx.IDShortString() })
					ret = inpEnd._traversePastCone(opt)
					return ret
				})
			}
			if ret && opt.Vertex != nil {
				ret = opt.Vertex(vid, v)
			}
		},
		VirtualTx: func(v *VirtualTransaction) {
			if opt.VirtualTx != nil {
				ret = opt.VirtualTx(vid, v)
			}
		},
		Deleted: func() {
			if opt.Orphaned != nil {
				ret = opt.Orphaned(vid)
			}
		},
	})
	return ret
}

func (vid *WrappedTx) InflationAmount() (ret uint64) {
	vid.RUnwrap(UnwrapOptions{
		Vertex: func(v *Vertex) {
			ret = v.Tx.InflationAmount()
		},
		Deleted: vid.PanicAccessDeleted,
	})
	return
}

// IsPreferredMilestoneAgainstTheOther betterMilestone returns if vid1 is strongly better than vid2
func IsPreferredMilestoneAgainstTheOther(vid1, vid2 *WrappedTx) bool {
	util.Assertf(vid1.IsSequencerMilestone() && vid2.IsSequencerMilestone(), "vid1.IsSequencerMilestone() && vid2.IsSequencerMilestone()")

	if vid1 == vid2 {
		return false
	}
	if vid2 == nil {
		return true
	}
	return IsPreferredBase(vid1.LedgerCoverageSum(), vid2.LedgerCoverageSum(), &vid1.ID, &vid2.ID)
}

func IsPreferredBase(covSum1, covSum2 uint64, txid1, txid2 *ledger.TransactionID) bool {
	switch {
	case covSum1 > covSum2:
		// main preference is by ledger coverage
		return true
	case covSum1 == covSum2:
		if txid1 == nil || txid2 == nil {
			return false
		}
		if txid1.Timestamp() == txid2.Timestamp() {
			// prefer with bigger hash
			return bytes.Compare(txid1[:], txid2[:]) > 0
		}
		// prefer younger
		return txid2.Timestamp().Before(txid1.Timestamp())
	}
	return false

}
