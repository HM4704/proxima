package node_cmd

import (
	"strconv"

	"github.com/lunfardo314/proxima/proxi/glb"
	"github.com/spf13/cobra"
)

func initBranchesCmd() *cobra.Command {
	branchesCmd := &cobra.Command{
		Use:   "branches",
		Short: `gets the latests branches`,
		Args:  cobra.ExactArgs(1),
		Run:   runBranchesCmd,
	}
	branchesCmd.InitDefaultHelpCmd()
	return branchesCmd
}

func runBranchesCmd(_ *cobra.Command, args []string) {
	glb.InitLedgerFromNode()

	const defaultLastNSlots = 5

	lastNSlots := defaultLastNSlots
	var err error
	if len(args) > 0 {
		lastNSlots, err = strconv.Atoi(args[0])
		glb.AssertNoError(err)
		if lastNSlots < 1 {
			lastNSlots = defaultLastNSlots
		}
	}
	glb.Infof("displaying branch info of last %d slots back", lastNSlots)
	// rootRecords, err := glb.GetClient().GetRootRecordsNSlotsBack(lastNSlots)
	// for i, rr := range rootRecords {
	// 	glb.Infof("%3d: %18s   ledger cov.: %d, seqID: %s, inflation: %d, supply:%d",
	// 		i,
	// 		rr.Root.String(),
	// 		rr.LedgerCoverage,
	// 		rr.SequencerID.String(),
	// 		rr.SlotInflation,
	// 		rr.Supply,
	// 	)
	// }

	branchData, err := glb.GetClient().GetBranchDataMulti(lastNSlots)
	glb.AssertNoError(err)

	for i, bd := range branchData {
		txid := bd.Stem.ID.TransactionID()
		glb.Infof("%3d: %18s   numTx: %d, seqID: %s, root: %s",
			i,
			txid.StringShort(),
			bd.NumTransactions,
			bd.SequencerID.String(),
			bd.Root.String(),
		)
	}
}
