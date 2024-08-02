package multistate

import (
	"encoding/hex"

	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/unitrie/common"
)

func (r *RootRecord) JSONAble() *RootRecordJSONAble {
	return &RootRecordJSONAble{
		Root:           r.Root.String(),
		SequencerID:    r.SequencerID.StringHex(),
		LedgerCoverage: r.LedgerCoverage,
		SlotInflation:  r.SlotInflation,
		Supply:         r.Supply,
	}
}

func (r *RootRecordJSONAble) Parse() (*RootRecord, error) {
	ret := &RootRecord{
		SlotInflation: r.SlotInflation,
		Supply:        r.Supply,
	}
	var err error
	rootBin, err := hex.DecodeString(r.Root)
	if err != nil {
		return nil, err
	}
	ret.Root, err = common.VectorCommitmentFromBytes(ledger.CommitmentModel, rootBin)
	if err != nil {
		return nil, err
	}
	ret.SequencerID, err = ledger.ChainIDFromHexString(r.SequencerID)
	if err != nil {
		return nil, err
	}
	ret.LedgerCoverage = r.LedgerCoverage
	return ret, nil
}

func (r *BranchData) JSONAble() *BranchDataJSONAble {
	rr := r.RootRecord.JSONAble()

	return &BranchDataJSONAble{
		RootRecordJSONAble: *rr,
		Stem: &ledger.OutputWithIDJSONAble{
			ID: r.Stem.ID.StringHex(),
		},
		SequencerOutput: &ledger.OutputWithIDJSONAble{
			ID: r.SequencerOutput.ID.StringHex(),
		},
	}
}

func (r *BranchDataJSONAble) Parse() (*BranchData, error) {
	rr, err := r.RootRecordJSONAble.Parse()
	if err != nil {
		return nil, err
	}
	StemID, err := ledger.OutputIDFromHexString(r.Stem.ID)
	if err != nil {
		return nil, err
	}
	SequencerOutputID, err := ledger.OutputIDFromHexString(r.SequencerOutput.ID)
	if err != nil {
		return nil, err
	}
	ret := &BranchData{
		RootRecord: *rr,
		Stem: &ledger.OutputWithID{
			ID: StemID,
		},
		SequencerOutput: &ledger.OutputWithID{
			ID: SequencerOutputID,
		},
	}

	return ret, nil
}
