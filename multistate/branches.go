package multistate

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lazybytes"
	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
)

// two additional partitions of the k/v store
const (
	// rootRecordDBPartition
	rootRecordDBPartition = immutable.PartitionOther
	latestSlotDBPartition = rootRecordDBPartition + 1
)

func writeRootRecord(w common.KVWriter, branchTxID ledger.TransactionID, rootData RootRecord) {
	key := common.ConcatBytes([]byte{rootRecordDBPartition}, branchTxID[:])
	w.Set(key, rootData.Bytes())
}

func writeLatestSlot(w common.KVWriter, slot ledger.Slot) {
	w.Set([]byte{latestSlotDBPartition}, slot.Bytes())
}

// FetchLatestSlot fetches latest recorded slot
func FetchLatestSlot(store common.KVReader) ledger.Slot {
	bin := store.Get([]byte{latestSlotDBPartition})
	if len(bin) == 0 {
		return 0
	}
	ret, err := ledger.SlotFromBytes(bin)
	common.AssertNoError(err)
	return ret
}

const numberOfElementsInRootRecord = 6

func (r *RootRecord) Bytes() []byte {
	util.Assertf(r.LedgerCoverage > 0, "r.Coverage.LatestDelta() > 0")
	arr := lazybytes.EmptyArray(numberOfElementsInRootRecord)
	arr.Push(r.SequencerID.Bytes())
	arr.Push(r.Root.Bytes())

	var coverage [8]byte
	binary.BigEndian.PutUint64(coverage[:], r.LedgerCoverage)
	arr.Push(coverage[:])

	var slotInflationBin, supplyBin [8]byte
	binary.BigEndian.PutUint64(slotInflationBin[:], r.SlotInflation)

	arr.Push(slotInflationBin[:])
	binary.BigEndian.PutUint64(supplyBin[:], r.Supply)

	arr.Push(supplyBin[:])
	var nTxBin [4]byte
	binary.BigEndian.PutUint32(nTxBin[:], r.NumTransactions)

	arr.Push(nTxBin[:])
	util.Assertf(arr.NumElements() == numberOfElementsInRootRecord, "arr.NumElements() == 6")
	return arr.Bytes()
}

func (r *RootRecord) String() string {
	return fmt.Sprintf("root record %s, %s, %s, %d",
		r.SequencerID.StringShort(), util.GoTh(r.LedgerCoverage), r.Root.String(), r.NumTransactions)
}

func RootRecordFromBytes(data []byte) (RootRecord, error) {
	arr, err := lazybytes.ParseArrayFromBytesReadOnly(data, numberOfElementsInRootRecord)
	if err != nil {
		return RootRecord{}, err
	}
	chainID, err := ledger.ChainIDFromBytes(arr.At(0))
	if err != nil {
		return RootRecord{}, err
	}
	root, err := common.VectorCommitmentFromBytes(ledger.CommitmentModel, arr.At(1))
	if err != nil {
		return RootRecord{}, err
	}
	if len(arr.At(2)) != 8 || len(arr.At(3)) != 8 || len(arr.At(4)) != 8 || len(arr.At(5)) != 4 {
		return RootRecord{}, fmt.Errorf("wrong data length")
	}
	return RootRecord{
		Root:            root,
		SequencerID:     chainID,
		LedgerCoverage:  binary.BigEndian.Uint64(arr.At(2)),
		SlotInflation:   binary.BigEndian.Uint64(arr.At(3)),
		Supply:          binary.BigEndian.Uint64(arr.At(4)),
		NumTransactions: binary.BigEndian.Uint32(arr.At(5)),
	}, nil
}

func ValidInclusionThresholdFraction(numerator, denominator int) bool {
	return numerator > 0 && denominator > 0 && numerator < denominator && denominator >= 2
}

func AbsoluteStrongFinalityCoverageThreshold(supply uint64, numerator, denominator int) uint64 {
	// 2 *supply * theta
	return ((supply / uint64(denominator)) * uint64(numerator)) << 1 // this order to avoid overflow
}

// IsCoverageAboveThreshold the root is dominating if coverage last delta is more than numerator/denominator of the double supply
func (r *RootRecord) IsCoverageAboveThreshold(numerator, denominator int) bool {
	util.Assertf(ValidInclusionThresholdFraction(numerator, denominator), "IsCoverageAboveThreshold: fraction is wrong")
	return r.LedgerCoverage > AbsoluteStrongFinalityCoverageThreshold(r.Supply, numerator, denominator)
}

// TxID transaction ID of the branch, as taken from the stem output ID
func (br *BranchData) TxID() *ledger.TransactionID {
	ret := br.Stem.ID.TransactionID()
	return &ret
}

func iterateAllRootRecords(store common.Traversable, fun func(branchTxID ledger.TransactionID, rootData RootRecord) bool) {
	store.Iterator([]byte{rootRecordDBPartition}).Iterate(func(k, data []byte) bool {
		txid, err := ledger.TransactionIDFromBytes(k[1:])
		util.AssertNoError(err)

		rootData, err := RootRecordFromBytes(data)
		util.AssertNoError(err)

		return fun(txid, rootData)
	})
}

func iterateRootRecordsOfParticularSlots(store common.Traversable, fun func(branchTxID ledger.TransactionID, rootData RootRecord) bool, slots []ledger.Slot) {
	prefix := [5]byte{rootRecordDBPartition, 0, 0, 0, 0}
	for _, s := range slots {
		slotPrefix := ledger.NewTransactionIDPrefix(s, true)
		copy(prefix[1:], slotPrefix[:])

		store.Iterator(prefix[:]).Iterate(func(k, data []byte) bool {
			txid, err := ledger.TransactionIDFromBytes(k[1:])
			util.AssertNoError(err)

			rootData, err := RootRecordFromBytes(data)
			util.AssertNoError(err)

			return fun(txid, rootData)
		})
	}
}

// IterateRootRecords iterates root records in the store:
// - if len(optSlot) > 0, it iterates specific slots
// - if len(optSlot) == 0, it iterates all records in the store
func IterateRootRecords(store common.Traversable, fun func(branchTxID ledger.TransactionID, rootData RootRecord) bool, optSlot ...ledger.Slot) {
	if len(optSlot) == 0 {
		iterateAllRootRecords(store, fun)
	}
	iterateRootRecordsOfParticularSlots(store, fun, optSlot)
}

// FetchRootRecord returns root data, stem output index and existence flag
// Exactly one root record must exist for the branch transaction
func FetchRootRecord(store common.KVReader, branchTxID ledger.TransactionID) (ret RootRecord, found bool) {
	key := common.Concat(rootRecordDBPartition, branchTxID[:])
	data := store.Get(key)
	if len(data) == 0 {
		return
	}
	ret, err := RootRecordFromBytes(data)
	util.AssertNoError(err)
	found = true
	return
}

// FetchAnyLatestRootRecord return first root record for the latest slot
func FetchAnyLatestRootRecord(store global.StateStoreReader) RootRecord {
	recs := FetchRootRecords(store, FetchLatestSlot(store))
	util.Assertf(len(recs) > 0, "len(recs)>0")
	return recs[0]
}

// FetchRootRecordsNSlotsBack load root records from N lates slots, present in the store
func FetchRootRecordsNSlotsBack(store global.StateStoreReader, nBack int) []RootRecord {
	if nBack <= 0 {
		return nil
	}
	ret := make([]RootRecord, 0)
	slotCount := 0
	for s := FetchLatestSlot(store); ; s-- {
		recs := FetchRootRecords(store, s)
		if len(recs) > 0 {
			ret = append(ret, recs...)
			slotCount++
		}
		if slotCount >= nBack || s == 0 {
			return ret
		}
	}
}

// FetchAllRootRecords returns all root records in the DB
func FetchAllRootRecords(store common.Traversable) []RootRecord {
	ret := make([]RootRecord, 0)
	IterateRootRecords(store, func(_ ledger.TransactionID, rootData RootRecord) bool {
		ret = append(ret, rootData)
		return true
	})
	return ret
}

// FetchRootRecords returns root records for particular slots in the DB
func FetchRootRecords(store common.Traversable, slots ...ledger.Slot) []RootRecord {
	if len(slots) == 0 {
		return nil
	}
	ret := make([]RootRecord, 0)
	IterateRootRecords(store, func(_ ledger.TransactionID, rootData RootRecord) bool {
		ret = append(ret, rootData)
		return true
	}, slots...)

	return ret
}

// FetchBranchData returns branch data by the branch transaction ID
func FetchBranchData(store common.KVReader, branchTxID ledger.TransactionID) (BranchData, bool) {
	if rd, found := FetchRootRecord(store, branchTxID); found {
		return FetchBranchDataByRoot(store, rd), true
	}
	return BranchData{}, false
}

// FetchBranchDataByRoot returns existing branch data by root record. The root record usually returned by FetchRootRecord
func FetchBranchDataByRoot(store common.KVReader, rootData RootRecord) BranchData {
	rdr, err := NewSugaredReadableState(store, rootData.Root, 0)
	util.AssertNoError(err)

	seqOut, err := rdr.GetChainOutput(&rootData.SequencerID)
	util.AssertNoError(err)

	return BranchData{
		RootRecord:      rootData,
		Stem:            rdr.GetStemOutput(),
		SequencerOutput: seqOut,
	}
}

// FetchBranchDataMulti returns branch records for particular root records
func FetchBranchDataMulti(store common.KVReader, rootData ...RootRecord) []*BranchData {
	ret := make([]*BranchData, len(rootData))
	for i, rd := range rootData {
		bd := FetchBranchDataByRoot(store, rd)
		ret[i] = &bd
	}
	return ret
}

// FetchLatestBranches branches of the latest slot sorted by coverage descending
func FetchLatestBranches(store global.StateStoreReader) []*BranchData {
	return FetchBranchDataMulti(store, FetchLatestRootRecords(store)...)
}

// FetchLatestRootRecords sorted descending by coverage
func FetchLatestRootRecords(store global.StateStoreReader) []RootRecord {
	ret := FetchRootRecords(store, FetchLatestSlot(store))
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].LedgerCoverage > ret[j].LedgerCoverage
	})
	return ret
}

// FetchLatestBranchTransactionIDs sorted descending by coverage
func FetchLatestBranchTransactionIDs(store global.StateStoreReader) []ledger.TransactionID {
	bd := FetchLatestBranches(store)
	ret := make([]ledger.TransactionID, len(bd))

	for i := range ret {
		ret[i] = bd[i].Stem.ID.TransactionID()
	}
	return ret
}

// FetchHeaviestBranchChainNSlotsBack descending by epoch
func FetchHeaviestBranchChainNSlotsBack(store global.StateStoreReader, nBack int) []*BranchData {
	rootData := make(map[ledger.TransactionID]RootRecord)
	latestSlot := FetchLatestSlot(store)

	if nBack < 0 {
		IterateRootRecords(store, func(branchTxID ledger.TransactionID, rd RootRecord) bool {
			rootData[branchTxID] = rd
			return true
		})
	} else {
		IterateRootRecords(store, func(branchTxID ledger.TransactionID, rd RootRecord) bool {
			rootData[branchTxID] = rd
			return true
		}, util.MakeRange(latestSlot-ledger.Slot(nBack), latestSlot)...)
	}

	sortedTxIDs := util.SortKeys(rootData, func(k1, k2 ledger.TransactionID) bool {
		// descending by epoch
		return k1.Slot() > k2.Slot()
	})

	latestBD := FetchLatestBranches(store)
	var lastInTheChain *BranchData

	for _, bd := range latestBD {
		if lastInTheChain == nil || bd.LedgerCoverage > lastInTheChain.LedgerCoverage {
			lastInTheChain = bd
		}
	}

	ret := append(make([]*BranchData, 0), lastInTheChain)

	for _, txid := range sortedTxIDs {
		rd := rootData[txid]
		bd := FetchBranchDataByRoot(store, rd)

		if bd.SequencerOutput.ID.Slot() == lastInTheChain.Stem.ID.Slot() {
			continue
		}
		util.Assertf(bd.SequencerOutput.ID.Slot() < lastInTheChain.Stem.ID.Slot(), "bd.SequencerOutput.ID.Slot() < lastInTheChain.Slot()")

		stemLock, ok := lastInTheChain.Stem.Output.StemLock()
		util.Assertf(ok, "stem output expected")

		if bd.Stem.ID != stemLock.PredecessorOutputID {
			continue
		}
		lastInTheChain = &bd
		ret = append(ret, lastInTheChain)
	}
	return ret
}

// BranchIsDescendantOf returns true if predecessor txid is known in the descendents state
func BranchIsDescendantOf(descendant, predecessor *ledger.TransactionID, getStore func() common.KVReader) bool {
	util.Assertf(descendant.IsBranchTransaction(), "must be a branch ts")

	if ledger.EqualTransactionIDs(descendant, predecessor) {
		return true
	}
	if descendant.Timestamp().Before(predecessor.Timestamp()) {
		return false
	}
	store := getStore()
	rr, found := FetchRootRecord(store, *descendant)
	if !found {
		return false
	}
	rdr, err := NewReadable(store, rr.Root)
	if err != nil {
		return false
	}

	return rdr.KnowsCommittedTransaction(predecessor)
}

// MustSequencerOutputOfBranch fetches and returns sequencer output of the branch. Panics if fails for any reason
func MustSequencerOutputOfBranch(store common.KVReader, txid ledger.TransactionID) *ledger.OutputWithChainID {
	util.Assertf(txid.IsBranchTransaction(), "txid.IsBranchTransaction()")
	bd, ok := FetchBranchData(store, txid)
	util.Assertf(ok, "SequencerOutputOfBranch: can't load branch data for %s", txid.StringShort)
	cc, idx := bd.SequencerOutput.Output.ChainConstraint()
	util.Assertf(idx != 0xff, "can't find chain constraint in %s", txid.StringShort)

	return &ledger.OutputWithChainID{
		OutputWithID:               *bd.SequencerOutput,
		ChainID:                    cc.ID,
		PredecessorConstraintIndex: cc.PredecessorConstraintIndex,
	}
}
