package genesis

import (
	"github.com/lunfardo314/proxima/core"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/sequencer"
	"github.com/lunfardo314/proxima/util"
)

// MakeDistributionTransaction
// - inits ledger state, returns it root and bootstrap sequencer ID
// - makes and returns origin distribution transaction
// Deprecated:
func MakeDistributionTransaction(par OriginDistributionParams) []byte {
	util.Assertf(len(par.GenesisDistribution) < 253, "too many addresses in the genesis distribution")

	// create genesis state with stem and genesis outputs in it
	distributeTotal := uint64(0)
	for i := range par.GenesisDistribution {
		distributeTotal += par.GenesisDistribution[i].Balance
		util.Assertf(distributeTotal+MinimumBalanceOnBoostrapSequencer <= par.InitialSupply,
			"distributeTotal(%d) + MinimumBalanceOnBoostrapSequencer(%d) < parState.InitialSupply(%d)",
			distributeTotal, MinimumBalanceOnBoostrapSequencer, par.InitialSupply)
	}
	genesisDistributionOutputs := make([]*core.Output, len(par.GenesisDistribution))
	for i := range par.GenesisDistribution {
		genesisDistributionOutputs[i] = core.NewOutput(func(o *core.Output) {
			o.WithAmount(par.GenesisDistribution[i].Balance).
				WithLock(par.GenesisDistribution[i].Lock)
		})
	}

	genesisReader, err := multistate.NewReadable(par.StateStore, par.GenesisStateRoot)
	util.AssertNoError(err)
	sugaredGenesisReader := multistate.MakeSugared(genesisReader)
	genesisIn, err := sugaredGenesisReader.GetChainOutput(&par.BootstrapSequencerID)
	util.AssertNoError(err)
	genesisStemIn := sugaredGenesisReader.GetStemOutput()

	// create origin branch transaction at the next slot after genesis time slot
	txBytes, err := sequencer.MakeSequencerTransaction(sequencer.MakeSequencerTransactionParams{
		ChainInput: &core.OutputWithChainID{
			OutputWithID: *genesisIn,
			ChainID:      par.BootstrapSequencerID,
		},
		StemInput:         genesisStemIn,
		Timestamp:         core.MustNewLogicalTime(genesisStemIn.Timestamp().TimeSlot()+1, 0),
		MinimumFee:        0,
		AdditionalInputs:  nil,
		AdditionalOutputs: genesisDistributionOutputs,
		Endorsements:      nil,
		PrivateKey:        par.GenesisControllerPrivateKey,
		TotalSupply:       par.InitialSupply,
	})
	util.AssertNoError(err)
	return txBytes
}