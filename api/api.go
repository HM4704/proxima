package api

import (
	"github.com/lunfardo314/proxima/core/vertex"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
)

const (
	PathGetLedgerID             = "/get_ledger_id"
	PathGetAccountOutputs       = "/get_account_outputs"
	PathGetChainOutput          = "/get_chain_output"
	PathGetOutput               = "/get_output"
	PathQueryTxStatus           = "/query_tx_status"
	PathQueryInclusionScore     = "/query_inclusion_score"
	PathSubmitTransaction       = "/submit_tx"
	PathGetSyncInfo             = "/sync_info"
	PathGetNodeInfo             = "/node_info"
	PathGetPeersInfo            = "/peers_info"
	PathGetSequencerStats       = "/sequ_stats"
	PathGetLatestReliableBranch = "/get_latest_reliable_branch"
)

type (
	Error struct {
		// empty string when no error
		Error string `json:"error,omitempty"`
	}

	LedgerID struct {
		Error
		// hex-encoded ledger id bytes
		LedgerIDBytes string `json:"ledger_id_bytes,omitempty"`
	}

	// OutputList is returned by 'get_account_outputs'
	OutputList struct {
		Error
		// key is hex-encoded outputID bytes
		// value is hex-encoded raw output data
		Outputs map[string]string `json:"outputs,omitempty"`
	}

	// ChainOutput is returned by 'get_chain_output'
	ChainOutput struct {
		Error
		// hex-encoded outputID
		OutputID string `json:"output_id,omitempty"`
		// hex-encoded output data
		OutputData string `json:"output_data,omitempty"`
	}

	// OutputData is returned by 'get_output'
	OutputData struct {
		Error
		// hex-encoded output data
		OutputData string `json:"output_data,omitempty"`
		//Inclusion  []InclusionDataEncoded `json:"inclusion,omitempty"`
	}

	QueryTxStatus struct {
		Error
		TxIDStatus vertex.TxIDStatusJSONAble       `json:"txid_status"`
		Inclusion  *multistate.TxInclusionJSONAble `json:"inclusion,omitempty"`
	}

	TxInclusionScore struct {
		ThresholdNumerator   int `json:"threshold_numerator"`
		ThresholdDenominator int `json:"threshold_denominator"`
		LatestSlot           int `json:"latest_slot"`
		EarliestSlot         int `json:"earliest_slot"`
		StrongScore          int `json:"strong_score"`
		WeakScore            int `json:"weak_score"`
	}

	QueryTxInclusionScore struct {
		Error
		TxInclusionScore
	}

	SyncInfo struct {
		Error
		Synced       bool                         `json:"synced"`
		InSyncWindow bool                         `json:"in_sync_window,omitempty"`
		PerSequencer map[string]SequencerSyncInfo `json:"per_sequencer,omitempty"`
	}

	SequencerSyncInfo struct {
		Synced              bool   `json:"synced"`
		LatestHealthySlot   uint32 `json:"latest_healthy_slot"`
		LatestCommittedSlot uint32 `json:"latest_committed_slot"`
		LedgerCoverage      uint64 `json:"ledger_coverage"`
	}

	PeersInfo struct {
		Error
		Peers []PeerInfo `json:"peers,omitempty"`
	}

	PeerInfo struct {
		// The libp2p identifier of the peer.
		ID string `json:"id"`
		// The libp2p multi addresses of the peer.
		MultiAddresses []string `json:"multiAddresses,omitempty"`
	}

	// LatestReliableBranch returned by get_latest_reliable_branch
	LatestReliableBranch struct {
		Error
		RootData multistate.RootRecordJSONAble `json:"root_record,omitempty"`
		BranchID ledger.TransactionID          `json:"branch_id,omitempty"`
	}

	SequencerData struct {
		SequencerID    ledger.ChainID
		LedgerCoverage uint64
		SlotInflation  uint64
		Supply         uint64
		AmountOnChain  uint64
	}

	SequencerStatistic struct {
		Name string `json:"name"`
		//Active       uint32 `json:"active"`
		Wins uint32 `json:"wins"`
		//SumInflation   uint64 `json:"sum_inflation"`
		OnChainBalance uint64 `json:"on_chain_balance"`
	}

	SequencerStatistics struct {
		Error
		SequStat map[string]*SequencerStatistic `json:"sequ_stat,omitempty"`
	}
)

const ErrGetOutputNotFound = "output not found"

func CalcTxInclusionScore(inclusion *multistate.TxInclusion, thresholdNumerator, thresholdDenominator int) TxInclusionScore {
	ret := TxInclusionScore{
		ThresholdNumerator:   thresholdNumerator,
		ThresholdDenominator: thresholdDenominator,
		LatestSlot:           int(inclusion.LatestSlot),
		EarliestSlot:         int(inclusion.EarliestSlot),
		StrongScore:          0,
		WeakScore:            0,
	}
	if len(inclusion.Inclusion) == 0 {
		return ret
	}
	var includedInBranches, numDominatingBranches, numIncludedInDominating int
	for i := range inclusion.Inclusion {
		if inclusion.Inclusion[i].Included {
			includedInBranches++
		}
		if inclusion.Inclusion[i].RootRecord.IsCoverageAboveThreshold(thresholdNumerator, thresholdDenominator) {
			numDominatingBranches++
			if inclusion.Inclusion[i].Included {
				numIncludedInDominating++
			}
		}
	}
	ret.WeakScore = (includedInBranches * 100) / len(inclusion.Inclusion)
	if numDominatingBranches > 0 {
		ret.StrongScore = (numIncludedInDominating * 100) / numDominatingBranches
	}
	return ret
}

func GetSequencerStatistics(stateStore global.StateStoreReader, nSlotsBack int) *SequencerStatistics {

	sequStat := &SequencerStatistics{
		SequStat: make(map[string]*SequencerStatistic),
	}

	//actSlot, _ := multistate.FindLatestHealthySlot(stateStore, global.FractionHealthyBranch)

	mainBranches := multistate.FetchHeaviestBranchChainNSlotsBack(stateStore, -1)
	for _, bd := range mainBranches {
		sd, exists := sequStat.SequStat[bd.SequencerID.String()]
		if !exists {
			// Initialize the struct if it doesn't exist in the map
			sd = &SequencerStatistic{}
			sequStat.SequStat[bd.SequencerID.String()] = sd
		}
		sd.Wins++
		if sd.OnChainBalance == 0 {
			sd.OnChainBalance = bd.SequencerOutput.Output.Amount()
		}
		if sd.Name == "" {
			if md := ledger.ParseMilestoneData(bd.SequencerOutput.Output); md != nil {
				sd.Name = md.Name
			}
		}
	}

	return sequStat
}
