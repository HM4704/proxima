package workflow

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lunfardo314/proxima/core"
	"github.com/lunfardo314/proxima/genesis"
	state "github.com/lunfardo314/proxima/state"
	"github.com/lunfardo314/proxima/txbuilder"
	utxo_tangle "github.com/lunfardo314/proxima/utangle"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/countdown"
	"github.com/lunfardo314/proxima/util/testutil"
	"github.com/lunfardo314/proxima/util/testutil/inittest"
	"github.com/lunfardo314/proxima/workflow"
	"github.com/lunfardo314/unitrie/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/blake2b"
)

type workflowTestData struct {
	initLedgerStatePar      genesis.IdentityData
	distributionPrivateKeys []ed25519.PrivateKey
	distributionAddrs       []core.AddressED25519
	faucetOutputs           []*core.OutputWithID
	ut                      *utxo_tangle.UTXOTangle
	bootstrapChainID        core.ChainID
	distributionTxID        core.TransactionID
	w                       *workflow.Workflow
}

const initDistributedBalance = 10_000_000

func initWorkflowTest(t *testing.T, nDistribution int, nowis core.LogicalTime, debugConfig ...workflow.DebugConfig) *workflowTestData {
	core.SetTimeTickDuration(10 * time.Millisecond)
	t.Logf("nowis timestamp: %s", nowis.String())
	genesisPrivKey := testutil.GetTestingPrivateKey()
	par := *genesis.DefaultIdentityData(genesisPrivKey)
	distrib, privKeys, addrs := inittest.GenesisParamsWithPreDistribution(nDistribution, initDistributedBalance, nowis.TimeSlot())
	ret := &workflowTestData{
		initLedgerStatePar:      par,
		distributionPrivateKeys: privKeys,
		distributionAddrs:       addrs,
		faucetOutputs:           make([]*core.OutputWithID, nDistribution),
	}

	ret.ut, ret.bootstrapChainID, ret.distributionTxID =
		utxo_tangle.CreateGenesisUTXOTangleWithDistribution(par, genesisPrivKey, distrib, common.NewInMemoryKVStore(), common.NewInMemoryKVStore())

	for i := range ret.faucetOutputs {
		outs, err := ret.ut.HeaviestStateForLatestTimeSlot().GetOutputsForAccount(ret.distributionAddrs[i].AccountID())
		require.NoError(t, err)
		require.EqualValues(t, 1, len(outs))
		ret.faucetOutputs[i] = outs[0]

	}
	ret.w = workflow.New(ret.ut, debugConfig...)
	return ret
}

func (wd *workflowTestData) makeTxFromFaucet(amount uint64, target core.AddressED25519, idx ...int) ([]byte, error) {
	idxFaucet := 0
	if len(idx) > 0 {
		idxFaucet = idx[0]
	}
	td := txbuilder.NewTransferData(wd.distributionPrivateKeys[idxFaucet], wd.distributionAddrs[idxFaucet], wd.faucetOutputs[idxFaucet].Timestamp()).
		WithAmount(amount).
		WithTargetLock(target).
		MustWithInputs(wd.faucetOutputs[idxFaucet])

	_, err := core.LogicalTimeFromBytes(td.Timestamp[:])
	util.AssertNoError(err)

	txBytes, remainder, err := txbuilder.MakeSimpleTransferTransactionWithRemainder(td)
	if err != nil {
		return nil, err
	}
	wd.faucetOutputs[idxFaucet] = remainder
	return txBytes, nil
}

func (wd *workflowTestData) setNewVertexCounter(waitCounter *countdown.Countdown) {
	wd.w.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
		waitCounter.Tick()
	})
}

func TestWorkflowBasic(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), workflow.DebugConfig{"all": zapcore.DebugLevel})
		wd.w.SetLogTransactions(true)
		wd.w.Start()
		time.Sleep(10 * time.Millisecond)
		wd.w.Stop()
		time.Sleep(10 * time.Millisecond)
	})
	t.Run("2", func(t *testing.T) {
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), workflow.DebugConfig{"all": zapcore.DebugLevel})
		wd.w.SetLogTransactions(true)
		wd.w.Start()
		err := wd.w.TransactionIn(nil)
		require.Error(t, err)
		err = wd.w.TransactionIn([]byte("abc"))
		require.Error(t, err)
		util.RequirePanicOrErrorWith(t, func() error {
			return wd.w.TransactionIn([]byte("0000000000"))
		}, "basic parse failed")
		time.Sleep(1000 * time.Millisecond)
		wd.w.Stop()
		time.Sleep(10 * time.Millisecond)
	})
}

func TestWorkflow(t *testing.T) {
	t.Run("1 sync", func(t *testing.T) {
		const numRuns = 200

		dbg := workflow.DebugConfig{
			//PreValidateConsumerName: zapcore.DebugLevel,
		}
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), dbg)
		wd.w.SetLogTransactions(true)

		t.Logf("timestamp now: %s", core.LogicalTimeNow().String())
		t.Logf("distribution timestamp: %s", wd.distributionTxID.Timestamp().String())
		t.Logf("origin slot: %d", wd.initLedgerStatePar.GenesisTimeSlot)

		estimatedTimeout := (time.Duration(numRuns) * core.TransactionTimePaceDuration()) + (5 * time.Second)
		waitCounter := countdown.New(numRuns, estimatedTimeout)
		cnt := 0
		err := wd.w.OnEvent(workflow.EventNewVertex, func(v *workflow.NewVertexEventData) {
			waitCounter.Tick()
			cnt++
		})
		require.NoError(t, err)

		wd.w.Start()

		for i := 0; i < numRuns; i++ {
			txBytes, err := wd.makeTxFromFaucet(100+uint64(i), wd.distributionAddrs[0])
			require.NoError(t, err)

			_, err = wd.w.TransactionInWaitAppendSyncTx(txBytes)
			require.NoError(t, err)
		}
		err = waitCounter.Wait()
		require.NoError(t, err)

		wd.w.Stop()
		t.Logf("UTXO tangle:\n%s", wd.ut.Info())
		require.EqualValues(t, numRuns, cnt)
	})
	t.Run("1 async", func(t *testing.T) {
		const numRuns = 200

		dbg := workflow.DebugConfig{
			//PreValidateConsumerName: zapcore.DebugLevel,
		}
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), dbg)
		wd.w.SetLogTransactions(true)

		t.Logf("timestamp now: %s", core.LogicalTimeNow().String())
		t.Logf("distribution timestamp: %s", wd.distributionTxID.Timestamp().String())
		t.Logf("origin slot: %d", wd.initLedgerStatePar.GenesisTimeSlot)

		estimatedTimeout := (time.Duration(numRuns) * core.TransactionTimePaceDuration()) + (5 * time.Second)
		waitCounter := countdown.New(numRuns, estimatedTimeout)
		cnt := 0
		err := wd.w.OnEvent(workflow.EventNewVertex, func(v *workflow.NewVertexEventData) {
			waitCounter.Tick()
			cnt++
		})
		require.NoError(t, err)

		wd.w.Start()

		for i := 0; i < numRuns; i++ {
			txBytes, err := wd.makeTxFromFaucet(100+uint64(i), wd.distributionAddrs[0])
			require.NoError(t, err)

			err = wd.w.TransactionIn(txBytes)
			require.NoError(t, err)
		}
		err = waitCounter.Wait()
		require.NoError(t, err)

		wd.w.Stop()
		t.Logf("UTXO tangle:\n%s", wd.ut.Info())
		require.EqualValues(t, numRuns, cnt)
	})
	t.Run("duplicates", func(t *testing.T) {
		const (
			numTx   = 10
			numRuns = 10
		)

		wd := initWorkflowTest(t, 1, core.LogicalTimeNow()) //, DebugConfig{PrimaryInputConsumerName: zapcore.DebugLevel})
		wd.w.SetLogTransactions(true)

		var err error
		txBytes := make([][]byte, numTx)
		for i := range txBytes {
			txBytes[i], err = wd.makeTxFromFaucet(100+uint64(i), wd.distributionAddrs[0])
			require.NoError(t, err)
		}

		waitCounterAdd := countdown.NewNamed("addTx", numTx, 5*time.Second)
		waitCounterDuplicate := countdown.NewNamed("duplicates", numTx*(numRuns-1), 5*time.Second)

		err = wd.w.OnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			waitCounterAdd.Tick()
		})
		require.NoError(t, err)
		err = wd.w.OnEvent(workflow.EventCodeDuplicateTx, func(_ *core.TransactionID) {
			waitCounterDuplicate.Tick()
		})
		require.NoError(t, err)
		wd.w.Start()

		for i := 0; i < numRuns; i++ {
			for j := range txBytes {
				err = wd.w.TransactionInWithLog(txBytes[j], fmt.Sprintf("%d", j))
				require.NoError(t, err)
			}
		}
		err = waitCounterAdd.Wait()
		require.NoError(t, err)
		err = waitCounterDuplicate.Wait()
		require.NoError(t, err)

		wd.w.Stop()
		t.Logf("%s", wd.w.UTXOTangle().Info())
		t.Logf("%s", wd.w.DumpTransactionLog())

	})
	t.Run("listen", func(t *testing.T) {
		const numRuns = 200

		dbg := workflow.DebugConfig{
			//PreValidateConsumerName: zapcore.DebugLevel,
		}
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), dbg)
		wd.w.SetLogTransactions(true)

		t.Logf("timestamp now: %s", core.LogicalTimeNow().String())
		t.Logf("distribution timestamp: %s", wd.distributionTxID.Timestamp().String())
		t.Logf("origin slot: %d", wd.initLedgerStatePar.GenesisTimeSlot)

		estimatedTimeout := (time.Duration(numRuns) * core.TransactionTimePaceDuration()) + (6 * time.Second)
		waitCounter := countdown.New(numRuns, estimatedTimeout)
		cnt := 0
		err := wd.w.OnEvent(workflow.EventNewVertex, func(v *workflow.NewVertexEventData) {
			waitCounter.Tick()
			cnt++
		})
		require.NoError(t, err)

		listenerCounter := 0
		err = wd.w.Events().ListenAccount(wd.distributionAddrs[0], func(_ utxo_tangle.WrappedOutput) {
			listenerCounter++
		})
		require.NoError(t, err)

		wd.w.Start()

		for i := 0; i < numRuns; i++ {
			txBytes, err := wd.makeTxFromFaucet(100+uint64(i), wd.distributionAddrs[0])
			require.NoError(t, err)

			err = wd.w.TransactionIn(txBytes)
			require.NoError(t, err)
		}
		err = waitCounter.Wait()
		require.NoError(t, err)
		require.EqualValues(t, 2*numRuns, listenerCounter)

		wd.w.Stop()
		t.Logf("UTXO tangle:\n%s", wd.ut.Info())
	})

}

func TestSolidifier(t *testing.T) {
	t.Run("one tx", func(t *testing.T) {
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), workflow.AllDebugLevel)
		wd.w.SetLogTransactions(true)
		cd := countdown.New(1, 3*time.Second)
		wd.setNewVertexCounter(cd)

		txBytes, err := wd.makeTxFromFaucet(10_000, wd.distributionAddrs[0])
		require.NoError(t, err)

		wd.w.Start()
		err = wd.w.TransactionIn(txBytes)
		require.NoError(t, err)

		err = cd.Wait()
		require.NoError(t, err)
		wd.w.Stop()

		t.Logf(wd.w.CounterInfo())
		err = wd.w.CheckDebugCounters(map[string]int{"[addtx].ok": 1})
		require.NoError(t, err)
	})
	t.Run("several tx usual seq", func(t *testing.T) {
		const howMany = 100
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), workflow.AllInfoLevel)
		wd.w.SetLogTransactions(true)
		cd := countdown.New(howMany, 10*time.Second)
		wd.setNewVertexCounter(cd)

		var err error

		txBytes := make([][]byte, howMany)
		for i := range txBytes {
			txBytes[i], err = wd.makeTxFromFaucet(10_000, wd.distributionAddrs[0])
			require.NoError(t, err)
		}
		wd.w.Start()
		for i := range txBytes {
			err = wd.w.TransactionIn(txBytes[i])
			require.NoError(t, err)
		}
		err = cd.Wait()
		require.NoError(t, err)
		wd.w.Stop()

		t.Logf(wd.w.CounterInfo())
		err = wd.w.CheckDebugCounters(map[string]int{"[addtx].ok": howMany})
	})
	t.Run("several tx reverse seq", func(t *testing.T) {
		const howMany = 10
		dbg := workflow.DebugConfig{
			//PrimaryInputConsumerName: zapcore.DebugLevel,
			//PreValidateConsumerName:  zapcore.DebugLevel,
		}
		wd := initWorkflowTest(t, 1, core.LogicalTimeNow(), dbg)
		wd.w.SetLogTransactions(true)
		cd := countdown.New(howMany, 10*time.Second)
		wd.setNewVertexCounter(cd)

		var err error

		txBytes := make([][]byte, howMany)
		for i := range txBytes {
			txBytes[i], err = wd.makeTxFromFaucet(10_000, wd.distributionAddrs[0])
			require.NoError(t, err)
		}
		wd.w.Start()
		for i := len(txBytes) - 1; i >= 0; i-- {
			err = wd.w.TransactionIn(txBytes[i])
			require.NoError(t, err)
		}
		err = cd.Wait()
		require.NoError(t, err)
		wd.w.Stop()

		t.Logf(wd.w.CounterInfo())
		err = wd.w.CheckDebugCounters(map[string]int{"[addtx].ok": howMany})
	})
	t.Run("several tx reverse seq no waiting room", func(t *testing.T) {
		const howMany = 100
		dbg := workflow.DebugConfig{
			//PrimaryInputConsumerName: zapcore.DebugLevel,
			//PreValidateConsumerName:  zapcore.DebugLevel,
			//SolidifyConsumerName: zapcore.DebugLevel,
		}
		// create all tx in the past, so that won't wait in the waiting room
		// all are sent to solidifier in the reverse order
		nowis := time.Now().Add(-10 * time.Second)
		wd := initWorkflowTest(t, 1, core.LogicalTimeFromTime(nowis), dbg)
		wd.w.SetLogTransactions(true)
		cd := countdown.New(howMany, 10*time.Second)
		wd.setNewVertexCounter(cd)

		var err error

		txBytes := make([][]byte, howMany)
		for i := range txBytes {
			txBytes[i], err = wd.makeTxFromFaucet(10_000, wd.distributionAddrs[0])
			require.NoError(t, err)
		}
		wd.w.Start()
		for i := len(txBytes) - 1; i >= 0; i-- {
			err = wd.w.TransactionIn(txBytes[i])
			require.NoError(t, err)
		}
		err = cd.Wait()
		require.NoError(t, err)
		wd.w.Stop()

		t.Logf(wd.w.CounterInfo())
		err = wd.w.CheckDebugCounters(map[string]int{"[addtx].ok": howMany})
	})
	t.Run("parallel rnd seqs no waiting room", func(t *testing.T) {
		const (
			howMany    = 50
			nAddresses = 5
		)
		dbg := workflow.DebugConfig{
			//PrimaryInputConsumerName: zapcore.DebugLevel,
			//PreValidateConsumerName:  zapcore.DebugLevel,
			//SolidifyConsumerName: zapcore.DebugLevel,
		}
		// create all tx in the past, so that won't wait in the waiting room
		// all are sent to solidifier in the reverse order
		nowis := time.Now().Add(-10 * time.Second)
		wd := initWorkflowTest(t, nAddresses, core.LogicalTimeFromTime(nowis), dbg)
		wd.w.SetLogTransactions(true)
		cd := countdown.New(howMany*nAddresses, 10*time.Second)
		wd.setNewVertexCounter(cd)

		var err error

		txSequences := make([][][]byte, nAddresses)
		for iSeq := range txSequences {
			txSequences[iSeq] = make([][]byte, howMany)
			for i := range txSequences[iSeq] {
				txSequences[iSeq][i], err = wd.makeTxFromFaucet(10_000, wd.distributionAddrs[iSeq])
				require.NoError(t, err)
			}
			sort.Slice(txSequences[iSeq], func(i, j int) bool {
				hi := blake2b.Sum256(txSequences[iSeq][i])
				hj := blake2b.Sum256(txSequences[iSeq][j])
				return bytes.Compare(hi[:], hj[:]) < 0
			})
		}
		wd.w.Start()
		for iSeq := range txSequences {
			for i := len(txSequences[iSeq]) - 1; i >= 0; i-- {
				err = wd.w.TransactionIn(txSequences[iSeq][i])
				require.NoError(t, err)
			}
		}
		err = cd.Wait()
		require.NoError(t, err)
		wd.w.Stop()

		t.Logf(wd.w.CounterInfo())
		err = wd.w.CheckDebugCounters(map[string]int{"[addtx].ok": howMany})
		t.Logf("UTXO UTXOTangle:\n%s", wd.ut.Info())
	})
}

type multiChainTestData struct {
	t                  *testing.T
	ts                 core.LogicalTime
	ut                 *utxo_tangle.UTXOTangle
	bootstrapChainID   core.ChainID
	privKey            ed25519.PrivateKey
	addr               core.AddressED25519
	faucetPrivKey      ed25519.PrivateKey
	faucetAddr         core.AddressED25519
	faucetOrigin       *core.OutputWithID
	sPar               genesis.IdentityData
	tPar               txbuilder.OriginDistributionParams
	originBranchTxid   core.TransactionID
	txBytesChainOrigin []byte
	txBytes            [][]byte // with chain origins
	chainOrigins       []*core.OutputWithChainID
	total              uint64
	pkController       []ed25519.PrivateKey
}

const onChainAmount = 10_000

func initMultiChainTest(t *testing.T, nChains int, printTx bool, timeSlot ...core.TimeSlot) *multiChainTestData {
	core.SetTimeTickDuration(10 * time.Millisecond)
	t.Logf("initMultiChainTest: now is: %s, %v", core.LogicalTimeNow().String(), time.Now())
	t.Logf("time tick duration is %v", core.TimeTickDuration())

	if len(timeSlot) > 0 {
		t.Logf("initMultiChainTest: timeSlot now is assumed: %d, %v", timeSlot[0], core.MustNewLogicalTime(timeSlot[0], 0).Time())
	}
	ret := &multiChainTestData{t: t}
	var privKeys []ed25519.PrivateKey
	var addrs []core.AddressED25519

	genesisPrivKey := testutil.GetTestingPrivateKey()
	ret.sPar = *genesis.DefaultIdentityData(genesisPrivKey)
	distrib, privKeys, addrs := inittest.GenesisParamsWithPreDistribution(2, onChainAmount*uint64(nChains), timeSlot...)
	ret.privKey = privKeys[0]
	ret.addr = addrs[0]
	ret.faucetPrivKey = privKeys[1]
	ret.faucetAddr = addrs[1]

	ret.pkController = make([]ed25519.PrivateKey, nChains)
	for i := range ret.pkController {
		ret.pkController[i] = ret.privKey
	}

	ret.ut, ret.bootstrapChainID, ret.originBranchTxid = utxo_tangle.CreateGenesisUTXOTangleWithDistribution(ret.sPar, genesisPrivKey, distrib, common.NewInMemoryKVStore(), common.NewInMemoryKVStore())
	require.True(t, ret.ut != nil)
	stateReader := ret.ut.HeaviestStateForLatestTimeSlot()

	t.Logf("state identity:\n%s", genesis.MustIdentityDataFromBytes(stateReader.IdentityBytes()).String())
	t.Logf("origin branch txid: %s", ret.originBranchTxid.Short())
	t.Logf("%s", ret.ut.Info())

	ret.faucetOrigin = &core.OutputWithID{
		ID:     core.NewOutputID(&ret.originBranchTxid, 0),
		Output: nil,
	}
	bal, _ := state.BalanceOnLock(stateReader, ret.addr)
	require.EqualValues(t, onChainAmount*int(nChains), int(bal))
	bal, _ = state.BalanceOnLock(stateReader, ret.faucetAddr)
	require.EqualValues(t, onChainAmount*int(nChains), int(bal))

	oDatas, err := stateReader.GetUTXOsLockedInAccount(ret.addr.AccountID())
	require.NoError(t, err)
	require.EqualValues(t, 1, len(oDatas))

	firstOut, err := oDatas[0].Parse()
	require.NoError(t, err)
	require.EqualValues(t, onChainAmount*uint64(nChains), firstOut.Output.Amount())

	faucetDatas, err := stateReader.GetUTXOsLockedInAccount(ret.faucetAddr.AccountID())
	require.NoError(t, err)
	require.EqualValues(t, 1, len(oDatas))

	ret.faucetOrigin, err = faucetDatas[0].Parse()
	require.NoError(t, err)
	require.EqualValues(t, onChainAmount*uint64(nChains), ret.faucetOrigin.Output.Amount())

	// Create transaction with nChains new chain origins.
	// It is not a sequencer tx with many chain origins
	txb := txbuilder.NewTransactionBuilder()
	_, err = txb.ConsumeOutput(firstOut.Output, firstOut.ID)
	require.NoError(t, err)
	txb.PutSignatureUnlock(0)

	ret.ts = firstOut.Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks)

	ret.chainOrigins = make([]*core.OutputWithChainID, nChains)
	for range ret.chainOrigins {
		o := core.NewOutput(func(o *core.Output) {
			o.WithAmount(onChainAmount).WithLock(ret.addr)
			_, err := o.PushConstraint(core.NewChainOrigin().Bytes())
			require.NoError(t, err)
		})
		_, err = txb.ProduceOutput(o)
		require.NoError(t, err)
	}

	txb.Transaction.Timestamp = ret.ts
	txb.Transaction.InputCommitment = txb.InputCommitment()
	txb.SignED25519(ret.privKey)

	ret.txBytesChainOrigin = txb.Transaction.Bytes()

	tx, err := state.TransactionFromBytesAllChecks(ret.txBytesChainOrigin)
	require.NoError(t, err)

	if printTx {
		t.Logf("chain origin tx: %s", tx.ToString(stateReader.GetUTXO))
	}

	tx.ForEachProducedOutput(func(idx byte, o *core.Output, oid *core.OutputID) bool {
		out := core.OutputWithID{
			ID:     *oid,
			Output: o,
		}
		if int(idx) != nChains {
			chainID, ok := out.ExtractChainID()
			require.True(t, ok)
			ret.chainOrigins[idx] = &core.OutputWithChainID{
				OutputWithID: out,
				ChainID:      chainID,
			}
		}
		return true
	})

	if printTx {
		cstr := make([]string, 0)
		for _, o := range ret.chainOrigins {
			cstr = append(cstr, o.ChainID.Short())
		}
		t.Logf("Chain IDs:\n%s\n", strings.Join(cstr, "\n"))
	}

	_, _, err = ret.ut.AppendVertexFromTransactionBytesDebug(ret.txBytesChainOrigin)
	require.NoError(t, err)
	return ret
}

func (r *multiChainTestData) createSequencerChain1(chainIdx int, pace int, printtx bool, exitFun func(i int, tx *state.Transaction) bool) [][]byte {
	require.True(r.t, pace >= core.TransactionTimePaceInTicks*2)

	ret := make([][]byte, 0)
	outConsumeChain := r.chainOrigins[chainIdx]
	r.t.Logf("chain #%d, ID: %s, origin: %s", chainIdx, outConsumeChain.ChainID.Short(), outConsumeChain.ID.Short())
	chainID := outConsumeChain.ChainID

	par := txbuilder.MakeSequencerTransactionParams{
		ChainInput:        outConsumeChain,
		StemInput:         nil,
		Timestamp:         outConsumeChain.Timestamp(),
		MinimumFee:        0,
		AdditionalInputs:  nil,
		AdditionalOutputs: nil,
		Endorsements:      nil,
		PrivateKey:        r.privKey,
		TotalSupply:       0,
	}

	lastStem := r.ut.HeaviestStemOutput()
	//r.t.Logf("lastStem #0 = %s, ts: %s", lastStem.ID.Short(), par.LogicalTime.String())
	lastBranchID := r.originBranchTxid

	var tx *state.Transaction
	for i := 0; !exitFun(i, tx); i++ {
		prevTs := par.Timestamp
		toNext := par.Timestamp.TimesTicksToNextSlotBoundary()
		if toNext == 0 || toNext > pace {
			par.Timestamp = par.Timestamp.AddTimeTicks(pace)
		} else {
			par.Timestamp = par.Timestamp.NextTimeSlotBoundary()
		}
		curTs := par.Timestamp
		//r.t.Logf("       %s -> %s", prevTs.String(), curTs.String())

		par.StemInput = nil
		if par.Timestamp.TimeTick() == 0 {
			par.StemInput = lastStem
		}

		par.Endorsements = nil
		if !par.ChainInput.ID.SequencerFlagON() {
			par.Endorsements = []*core.TransactionID{&lastBranchID}
		}

		txBytes, err := txbuilder.MakeSequencerTransaction(par)
		require.NoError(r.t, err)
		ret = append(ret, txBytes)
		require.NoError(r.t, err)

		tx, err = state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)

		if printtx {
			ce := ""
			if prevTs.TimeSlot() != curTs.TimeSlot() {
				ce = "(cross-slot)"
			}
			r.t.Logf("tx %d : %s    %s", i, tx.IDShort(), ce)
		}

		require.True(r.t, tx.IsSequencerMilestone())
		if par.StemInput != nil {
			require.True(r.t, tx.IsBranchTransaction())
		}

		o := tx.FindChainOutput(chainID)
		require.True(r.t, o != nil)

		par.ChainInput.OutputWithID = *o.Clone()
		if par.StemInput != nil {
			lastStem = tx.FindStemProducedOutput()
			require.True(r.t, lastStem != nil)
			//r.t.Logf("lastStem #%d = %s", i, lastStem.ID.Short())
		}
	}
	return ret
}

func (r *multiChainTestData) createSequencerChains1(pace int, howLong int) [][]byte {
	require.True(r.t, pace >= core.TransactionTimePaceInTicks*2)
	nChains := len(r.chainOrigins)
	require.True(r.t, nChains >= 2)

	ret := make([][]byte, 0)
	sequences := make([][]*state.Transaction, nChains)
	counter := 0
	for range sequences {
		// sequencer tx
		txBytes, err := txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput:   r.chainOrigins[counter],
			Timestamp:    r.chainOrigins[counter].Timestamp().AddTimeTicks(pace),
			Endorsements: []*core.TransactionID{&r.originBranchTxid},
			PrivateKey:   r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[counter] = []*state.Transaction{tx}
		ret = append(ret, txBytes)
		r.t.Logf("chain #%d, ID: %s, origin: %s, seq start: %s",
			counter, r.chainOrigins[counter].ChainID.Short(), r.chainOrigins[counter].ID.Short(), tx.IDShort())
		counter++
	}

	lastInChain := func(chainIdx int) *state.Transaction {
		return sequences[chainIdx][len(sequences[chainIdx])-1]
	}

	lastStemOutput := r.ut.HeaviestStemOutput()

	var curChainIdx, nextChainIdx int
	var txBytes []byte
	var err error

	for i := counter; i < howLong; i++ {
		nextChainIdx = (curChainIdx + 1) % nChains
		ts := core.MaxLogicalTime(
			lastInChain(nextChainIdx).Timestamp().AddTimeTicks(pace),
			lastInChain(curChainIdx).Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks),
		)
		chainIn := lastInChain(nextChainIdx).MustProducedOutputWithIDAt(0)

		if ts.TimesTicksToNextSlotBoundary() < 2*pace {
			ts = ts.NextTimeSlotBoundary()
		}
		var endorse []*core.TransactionID
		var stemOut *core.OutputWithID

		if ts.TimeTick() == 0 {
			// create branch tx
			stemOut = lastStemOutput
		} else {
			// endorse previous sequencer tx
			endorse = []*core.TransactionID{lastInChain(curChainIdx).ID()}
		}
		txBytes, err = txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput: &core.OutputWithChainID{
				OutputWithID: *chainIn,
				ChainID:      r.chainOrigins[nextChainIdx].ChainID,
			},
			StemInput:    stemOut,
			Endorsements: endorse,
			Timestamp:    ts,
			PrivateKey:   r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[nextChainIdx] = append(sequences[nextChainIdx], tx)
		ret = append(ret, txBytes)
		if stemOut != nil {
			lastStemOutput = tx.FindStemProducedOutput()
		}

		if stemOut == nil {
			r.t.Logf("%d : chain #%d, txid: %s, endorse(%d): %s, timestamp: %s",
				i, nextChainIdx, tx.IDShort(), curChainIdx, endorse[0].Short(), tx.Timestamp().String())
		} else {
			r.t.Logf("%d : chain #%d, txid: %s, timestamp: %s <- branch tx",
				i, nextChainIdx, tx.IDShort(), tx.Timestamp().String())
		}
		curChainIdx = nextChainIdx
	}
	return ret
}

// n parallel sequencer chains. Each sequencer transaction endorses 1 or 2 previous if possible
func (r *multiChainTestData) createSequencerChains2(pace int, howLong int) [][]byte {
	require.True(r.t, pace >= core.TransactionTimePaceInTicks*2)
	nChains := len(r.chainOrigins)
	require.True(r.t, nChains >= 2)

	ret := make([][]byte, 0)
	sequences := make([][]*state.Transaction, nChains)
	counter := 0
	for range sequences {
		txBytes, err := txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput:   r.chainOrigins[counter],
			Timestamp:    r.chainOrigins[counter].Timestamp().AddTimeTicks(pace),
			Endorsements: []*core.TransactionID{&r.originBranchTxid},
			PrivateKey:   r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[counter] = []*state.Transaction{tx}
		ret = append(ret, txBytes)
		r.t.Logf("chain #%d, ID: %s, origin: %s, seq start: %s",
			counter, r.chainOrigins[counter].ChainID.Short(), r.chainOrigins[counter].ID.Short(), tx.IDShort())
		counter++
	}

	lastInChain := func(chainIdx int) *state.Transaction {
		return sequences[chainIdx][len(sequences[chainIdx])-1]
	}

	lastStemOutput := r.ut.HeaviestStemOutput()

	var curChainIdx, nextChainIdx int
	var txBytes []byte
	var err error

	for i := counter; i < howLong; i++ {
		nextChainIdx = (curChainIdx + 1) % nChains
		ts := core.MaxLogicalTime(
			lastInChain(nextChainIdx).Timestamp().AddTimeTicks(pace),
			lastInChain(curChainIdx).Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks),
		)
		chainIn := lastInChain(nextChainIdx).MustProducedOutputWithIDAt(0)

		if ts.TimesTicksToNextSlotBoundary() < 2*pace {
			ts = ts.NextTimeSlotBoundary()
		}
		endorse := make([]*core.TransactionID, 0)
		var stemOut *core.OutputWithID

		if ts.TimeTick() == 0 {
			// create branch tx
			stemOut = lastStemOutput
		} else {
			// endorse previous sequencer tx
			const B = 4
			endorse = endorse[:0]
			endorsedIdx := curChainIdx
			maxEndorsements := B
			if maxEndorsements > nChains {
				maxEndorsements = nChains
			}
			for k := 0; k < maxEndorsements; k++ {
				endorse = append(endorse, lastInChain(endorsedIdx).ID())
				if endorsedIdx == 0 {
					endorsedIdx = nChains - 1
				} else {
					endorsedIdx--
				}
				if lastInChain(endorsedIdx).TimeSlot() != ts.TimeSlot() {
					break
				}
			}
		}
		txBytes, err = txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput: &core.OutputWithChainID{
				OutputWithID: *chainIn,
				ChainID:      r.chainOrigins[nextChainIdx].ChainID,
			},
			StemInput:    stemOut,
			Endorsements: endorse,
			Timestamp:    ts,
			PrivateKey:   r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[nextChainIdx] = append(sequences[nextChainIdx], tx)
		ret = append(ret, txBytes)
		if stemOut != nil {
			lastStemOutput = tx.FindStemProducedOutput()
		}

		if stemOut == nil {
			lst := make([]string, 0)
			for _, txid := range endorse {
				lst = append(lst, txid.Short())
			}
			r.t.Logf("%d : chain #%d, txid: %s, ts: %s, endorse: (%s)",
				i, nextChainIdx, tx.IDShort(), tx.Timestamp().String(), strings.Join(lst, ","))
		} else {
			r.t.Logf("%d : chain #%d, txid: %s, ts: %s <- branch tx",
				i, nextChainIdx, tx.IDShort(), tx.Timestamp().String())
		}
		curChainIdx = nextChainIdx
	}
	return ret
}

// n parallel sequencer chains. Each sequencer transaction endorses 1 or 2 previous if possible
// adding faucet transactions in between
func (r *multiChainTestData) createSequencerChains3(pace int, howLong int, printTx bool) [][]byte {
	require.True(r.t, pace >= core.TransactionTimePaceInTicks*2)
	nChains := len(r.chainOrigins)
	require.True(r.t, nChains >= 2)

	ret := make([][]byte, 0)
	sequences := make([][]*state.Transaction, nChains)
	counter := 0
	for range sequences {
		txBytes, err := txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput:   r.chainOrigins[counter],
			Timestamp:    r.chainOrigins[counter].Timestamp().AddTimeTicks(pace),
			Endorsements: []*core.TransactionID{&r.originBranchTxid},
			PrivateKey:   r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[counter] = []*state.Transaction{tx}
		ret = append(ret, txBytes)
		if printTx {
			r.t.Logf("chain #%d, ID: %s, origin: %s, seq start: %s",
				counter, r.chainOrigins[counter].ChainID.Short(), r.chainOrigins[counter].ID.Short(), tx.IDShort())
		}
		counter++
	}

	faucetOutput := r.faucetOrigin

	lastInChain := func(chainIdx int) *state.Transaction {
		return sequences[chainIdx][len(sequences[chainIdx])-1]
	}

	lastStemOutput := r.ut.HeaviestStemOutput()

	var curChainIdx, nextChainIdx int
	var txBytes []byte
	var tx *state.Transaction
	var err error

	for i := counter; i < howLong; i++ {
		nextChainIdx = (curChainIdx + 1) % nChains
		// create faucet tx
		td := txbuilder.NewTransferData(r.faucetPrivKey, r.faucetAddr, faucetOutput.Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks))
		td.WithTargetLock(core.ChainLockFromChainID(r.chainOrigins[nextChainIdx].ChainID)).
			WithAmount(100).
			MustWithInputs(faucetOutput)
		txBytes, err = txbuilder.MakeTransferTransaction(td)
		require.NoError(r.t, err)
		tx, err = state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		faucetOutput = tx.MustProducedOutputWithIDAt(0)
		feeOutput := tx.MustProducedOutputWithIDAt(1)
		ret = append(ret, txBytes)
		if printTx {
			r.t.Logf("faucet tx %s: amount left on faucet: %d", tx.IDShort(), faucetOutput.Output.Amount())
		}

		ts := core.MaxLogicalTime(
			lastInChain(nextChainIdx).Timestamp().AddTimeTicks(pace),
			lastInChain(curChainIdx).Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks),
			tx.Timestamp().AddTimeTicks(core.TransactionTimePaceInTicks),
		)
		chainIn := lastInChain(nextChainIdx).MustProducedOutputWithIDAt(0)

		if ts.TimesTicksToNextSlotBoundary() < 2*pace {
			ts = ts.NextTimeSlotBoundary()
		}
		endorse := make([]*core.TransactionID, 0)
		var stemOut *core.OutputWithID

		if ts.TimeTick() == 0 {
			// create branch tx
			stemOut = lastStemOutput
		} else {
			// endorse previous sequencer tx
			const B = 4
			endorse = endorse[:0]
			endorsedIdx := curChainIdx
			maxEndorsements := B
			if maxEndorsements > nChains {
				maxEndorsements = nChains
			}
			for k := 0; k < maxEndorsements; k++ {
				endorse = append(endorse, lastInChain(endorsedIdx).ID())
				if endorsedIdx == 0 {
					endorsedIdx = nChains - 1
				} else {
					endorsedIdx--
				}
				if lastInChain(endorsedIdx).TimeSlot() != ts.TimeSlot() {
					break
				}
			}
		}
		txBytes, err = txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput: &core.OutputWithChainID{
				OutputWithID: *chainIn,
				ChainID:      r.chainOrigins[nextChainIdx].ChainID,
			},
			StemInput:        stemOut,
			AdditionalInputs: []*core.OutputWithID{feeOutput},
			Endorsements:     endorse,
			Timestamp:        ts,
			PrivateKey:       r.privKey,
		})
		require.NoError(r.t, err)
		tx, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(r.t, err)
		sequences[nextChainIdx] = append(sequences[nextChainIdx], tx)
		ret = append(ret, txBytes)
		if stemOut != nil {
			lastStemOutput = tx.FindStemProducedOutput()
		}

		if printTx {
			total := lastInChain(nextChainIdx).MustProducedOutputWithIDAt(0).Output.Amount()
			if stemOut == nil {
				lst := make([]string, 0)
				for _, txid := range endorse {
					lst = append(lst, txid.Short())
				}
				r.t.Logf("%d : chain #%d, txid: %s, ts: %s, total: %d, endorse: (%s)",
					i, nextChainIdx, tx.IDShort(), tx.Timestamp().String(), total, strings.Join(lst, ","))
			} else {
				r.t.Logf("%d : chain #%d, txid: %s, ts: %s, total: %d <- branch tx",
					i, nextChainIdx, tx.IDShort(), tx.Timestamp().String(), total)
			}
		}
		curChainIdx = nextChainIdx
	}
	return ret
}

func TestMultiChainWorkflow(t *testing.T) {
	t.Run("one chain past time", func(t *testing.T) {
		const (
			nChains              = 1
			howLong              = 100
			chainPaceInTimeTicks = 23
			printBranchTx        = false
		)
		nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
		r := initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())
		txBytesSeq := r.createSequencerChain1(0, chainPaceInTimeTicks, true, func(i int, tx *state.Transaction) bool {
			return i == howLong
		})
		require.EqualValues(t, howLong, len(txBytesSeq))

		state.SetPrintEasyFLTraceOnFail(false)
		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName:  zapcore.DebugLevel,
			//workflow.SolidifyConsumerName: zapcore.DebugLevel,
			//workflow.AppendTxConsumerName: zapcore.DebugLevel,
		}

		wrk := workflow.New(r.ut, dbg)
		cd := countdown.New(howLong*nChains, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		listenCounter := 0
		err := wrk.Events().ListenSequencer(r.chainOrigins[0].ChainID, func(vid *utxo_tangle.WrappedTx) {
			//t.Logf("listen seq %s: %s", r.chainOrigins[0].ChainID.Short(), vertex.Tx.IDShort())
			listenCounter++
		})

		wrk.Start()

		for i, txBytes := range txBytesSeq {
			tx, err := state.TransactionFromBytes(txBytes)
			require.NoError(r.t, err)
			if tx.IsBranchTransaction() {
				t.Logf("append %d txid = %s <-- branch transaction", i, tx.IDShort())
			} else {
				t.Logf("append %d txid = %s", i, tx.IDShort())
			}
			if tx.IsBranchTransaction() {
				if printBranchTx {
					t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
				}
			}
			err = wrk.TransactionIn(txBytes)
			require.NoError(r.t, err)
		}

		err = cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		require.EqualValues(t, howLong*nChains, listenCounter)

		t.Logf("%s", r.ut.Info())
	})
	t.Run("one chain real time", func(t *testing.T) {
		const (
			nChains              = 1
			howLong              = 10
			chainPaceInTimeSlots = 23
			printBranchTx        = false
		)
		r := initMultiChainTest(t, nChains, false)
		txBytesSeq := r.createSequencerChain1(0, chainPaceInTimeSlots, true, func(i int, tx *state.Transaction) bool {
			return i == howLong
		})
		require.EqualValues(t, howLong, len(txBytesSeq))

		state.SetPrintEasyFLTraceOnFail(false)
		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName: zapcore.DebugLevel,
			//workflow.AppendTxConsumerName: zapcore.DebugLevel,
		}

		wrk := workflow.New(r.ut, dbg)
		cd := countdown.New(howLong*nChains, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for i, txBytes := range txBytesSeq {
			tx, err := state.TransactionFromBytes(txBytes)
			require.NoError(r.t, err)
			if tx.IsBranchTransaction() {
				t.Logf("append %d txid = %s <-- branch transaction", i, tx.IDShort())
			} else {
				t.Logf("append %d txid = %s", i, tx.IDShort())
			}
			if tx.IsBranchTransaction() {
				if printBranchTx {
					t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
				}
			}
			err = wrk.TransactionIn(txBytes)
			require.NoError(r.t, err)
		}

		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("%s", r.ut.Info())
	})
	t.Run("several chains until branch real time", func(t *testing.T) {
		const (
			nChains              = 15
			chainPaceInTimeSlots = 13
			printBranchTx        = false
		)
		r := initMultiChainTest(t, nChains, false)

		txBytesSeq := make([][][]byte, nChains)
		for i := range txBytesSeq {
			txBytesSeq[i] = r.createSequencerChain1(i, chainPaceInTimeSlots+i, false, func(i int, tx *state.Transaction) bool {
				// until first branch
				return i > 0 && tx.IsBranchTransaction()
			})
			t.Logf("seq %d, length: %d", i, len(txBytesSeq[i]))
		}

		state.SetPrintEasyFLTraceOnFail(false)

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
		}

		wrk := workflow.New(r.ut, dbg)
		nTransactions := 0
		for i := range txBytesSeq {
			nTransactions += len(txBytesSeq[i])
		}
		t.Logf("number of transactions: %d", nTransactions)
		cd := countdown.New(nTransactions, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for seqIdx := range txBytesSeq {
			for i, txBytes := range txBytesSeq[seqIdx] {
				//r.t.Logf("tangle info: %s", r.ut.Info())
				tx, err := state.TransactionFromBytes(txBytes)
				require.NoError(r.t, err)
				//if tx.IsBranchTransaction() {
				//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
				//} else {
				//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
				//}
				if tx.IsBranchTransaction() {
					if printBranchTx {
						t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
					}
				}
				err = wrk.TransactionIn(txBytes)
				require.NoError(r.t, err)
			}

		}
		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
	t.Run("several chains until branch past time", func(t *testing.T) {
		const (
			nChains              = 15
			chainPaceInTimeSlots = 13
			printBranchTx        = false
		)
		nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
		r := initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())

		txBytesSeq := make([][][]byte, nChains)
		for i := range txBytesSeq {
			txBytesSeq[i] = r.createSequencerChain1(i, chainPaceInTimeSlots+i, false, func(i int, tx *state.Transaction) bool {
				// until first branch
				return i > 0 && tx.IsBranchTransaction()
			})
			t.Logf("seq %d, length: %d", i, len(txBytesSeq[i]))
		}

		state.SetPrintEasyFLTraceOnFail(false)

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
		}

		wrk := workflow.New(r.ut, dbg)
		nTransactions := 0
		for i := range txBytesSeq {
			nTransactions += len(txBytesSeq[i])
		}
		t.Logf("number of transactions: %d", nTransactions)
		cd := countdown.New(nTransactions, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for seqIdx := range txBytesSeq {
			for i, txBytes := range txBytesSeq[seqIdx] {
				//r.t.Logf("tangle info: %s", r.ut.Info())
				tx, err := state.TransactionFromBytes(txBytes)
				require.NoError(r.t, err)
				//if tx.IsBranchTransaction() {
				//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
				//} else {
				//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
				//}
				if tx.IsBranchTransaction() {
					if printBranchTx {
						t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
					}
				}
				err = wrk.TransactionIn(txBytes)
				require.NoError(r.t, err)
			}

		}
		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
	t.Run("endorse conflicting chain", func(t *testing.T) {
		const (
			nChains              = 2
			chainPaceInTimeSlots = 7
			printBranchTx        = false
			howLong              = 50
			realTime             = false
		)
		var r *multiChainTestData
		if realTime {
			r = initMultiChainTest(t, nChains, false)
		} else {
			nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
			r = initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())
		}

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
			//workflow.ValidateConsumerName: zapcore.DebugLevel,
		}

		txBytesSeq := make([][][]byte, nChains)
		for i := range txBytesSeq {
			numBranches := 0
			txBytesSeq[i] = r.createSequencerChain1(i, chainPaceInTimeSlots, false, func(i int, tx *state.Transaction) bool {
				// up to given length and first non branch tx
				if tx != nil && tx.IsBranchTransaction() {
					numBranches++
				}
				return i >= howLong && numBranches > 0 && !tx.IsBranchTransaction()
			})
			t.Logf("seq %d, length: %d", i, len(txBytesSeq[i]))
		}
		// take the last transaction of the second sequence
		txBytes := txBytesSeq[1][len(txBytesSeq[1])-1]
		txEndorser, err := state.TransactionFromBytesAllChecks(txBytes)
		require.NoError(t, err)
		require.True(t, txEndorser.IsSequencerMilestone())
		require.False(t, txEndorser.IsBranchTransaction())
		require.EqualValues(t, 1, txEndorser.NumProducedOutputs())
		out := txEndorser.MustProducedOutputWithIDAt(0)
		t.Logf("output to consume:\n%s", out.Short())

		idToBeEndorsed, tsToBeEndorsed, err := state.TransactionIDAndTimestampFromTransactionBytes(txBytesSeq[0][len(txBytesSeq[0])-1])
		require.NoError(t, err)
		ts := core.MaxLogicalTime(tsToBeEndorsed, txEndorser.Timestamp())
		ts = ts.AddTimeTicks(core.TransactionTimePaceInTicks)
		t.Logf("timestamp to be endorsed: %s, endorser's timestamp: %s", tsToBeEndorsed.String(), ts.String())
		require.True(t, ts.TimeSlot() != 0 && ts.TimeSlot() == txEndorser.Timestamp().TimeSlot())
		t.Logf("ID to be endorsed: %s", idToBeEndorsed.Short())

		txBytesConflict, err := txbuilder.MakeSequencerTransaction(txbuilder.MakeSequencerTransactionParams{
			ChainInput: &core.OutputWithChainID{
				OutputWithID: *out,
				ChainID:      r.chainOrigins[1].ChainID,
			},
			Timestamp:    ts,
			Endorsements: []*core.TransactionID{&idToBeEndorsed},
			PrivateKey:   r.privKey,
		})
		require.NoError(t, err)

		state.SetPrintEasyFLTraceOnFail(false)

		wrk := workflow.New(r.ut, dbg)
		nTransactions := 0
		for i := range txBytesSeq {
			nTransactions += len(txBytesSeq[i])
		}
		t.Logf("number of transactions: %d", nTransactions)
		cd := countdown.New(nTransactions, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for seqIdx := range txBytesSeq {
			for i, txBytes := range txBytesSeq[seqIdx] {
				tx, err := state.TransactionFromBytes(txBytes)
				require.NoError(r.t, err)
				//if tx.IsBranchTransaction() {
				//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
				//} else {
				//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
				//}
				if tx.IsBranchTransaction() {
					if printBranchTx {
						t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
					}
				}
				err = wrk.TransactionIn(txBytes)
				require.NoError(r.t, err)
			}
		}
		err = wrk.TransactionInWithLog(txBytesConflict, "conflictingTx")
		require.NoError(r.t, err)

		err = cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
	t.Run("cross endorsing chains 1", func(t *testing.T) {
		const (
			nChains              = 15
			chainPaceInTimeSlots = 7
			printBranchTx        = false
			howLong              = 1000
			realTime             = false
		)
		var r *multiChainTestData
		if realTime {
			r = initMultiChainTest(t, nChains, false)
		} else {
			nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
			r = initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())
		}

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
			//workflow.ValidateConsumerName: zapcore.DebugLevel,
		}

		txBytesSeq := r.createSequencerChains1(chainPaceInTimeSlots, howLong)
		require.EqualValues(t, howLong, len(txBytesSeq))
		state.SetPrintEasyFLTraceOnFail(false)

		wrk := workflow.New(r.ut, dbg)
		cd := countdown.New(howLong, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for i, txBytes := range txBytesSeq {
			tx, err := state.TransactionFromBytes(txBytes)
			require.NoError(r.t, err)
			//if tx.IsBranchTransaction() {
			//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
			//} else {
			//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
			//}
			if tx.IsBranchTransaction() {
				if printBranchTx {
					t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
				}
			}
			err = wrk.TransactionIn(txBytes)
			require.NoError(r.t, err)
		}

		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
	t.Run("cross multi-endorsing chains", func(t *testing.T) {
		const (
			nChains              = 5
			chainPaceInTimeSlots = 7
			printBranchTx        = false
			howLong              = 1000
			realTime             = false
		)
		var r *multiChainTestData
		if realTime {
			r = initMultiChainTest(t, nChains, false)
		} else {
			nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
			r = initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())
		}

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
			//workflow.ValidateConsumerName: zapcore.DebugLevel,
		}

		txBytesSeq := r.createSequencerChains2(chainPaceInTimeSlots, howLong)

		state.SetPrintEasyFLTraceOnFail(false)

		wrk := workflow.New(r.ut, dbg)
		cd := countdown.New(howLong, 10*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for i, txBytes := range txBytesSeq {
			tx, err := state.TransactionFromBytes(txBytes)
			require.NoError(r.t, err)
			//if tx.IsBranchTransaction() {
			//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
			//} else {
			//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
			//}
			if tx.IsBranchTransaction() {
				if printBranchTx {
					t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
				}
			}
			err = wrk.TransactionIn(txBytes)
			require.NoError(r.t, err)
		}
		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
	t.Run("cross multi-endorsing chains with fees", func(t *testing.T) {
		const (
			nChains              = 5
			chainPaceInTimeSlots = 7
			printBranchTx        = false
			printTx              = false
			howLong              = 50 // 505 fails due to not enough tokens in the faucet
			realTime             = true
		)
		var r *multiChainTestData
		if realTime {
			r = initMultiChainTest(t, nChains, false)
		} else {
			nowisTs := core.LogicalTimeFromTime(time.Now().Add(-60 * time.Second))
			r = initMultiChainTest(t, nChains, false, nowisTs.TimeSlot())
		}

		dbg := workflow.DebugConfig{
			//workflow.PrimaryInputConsumerName: zapcore.DebugLevel,
			//workflow.PreValidateConsumerName: zapcore.DebugLevel,
			//workflow.SolidifyConsumerName:    zapcore.DebugLevel,
			//workflow.AppendTxConsumerName:    zapcore.DebugLevel,
			//workflow.ValidateConsumerName: zapcore.DebugLevel,
		}

		txBytesSeq := r.createSequencerChains3(chainPaceInTimeSlots, howLong, printTx)

		state.SetPrintEasyFLTraceOnFail(false)

		wrk := workflow.New(r.ut, dbg)
		cd := countdown.New(len(txBytesSeq), 20*time.Second)
		wrk.MustOnEvent(workflow.EventNewVertex, func(_ *workflow.NewVertexEventData) {
			cd.Tick()
		})
		wrk.Start()

		for i, txBytes := range txBytesSeq {
			tx, err := state.TransactionFromBytes(txBytes)
			require.NoError(r.t, err)
			//if tx.IsBranchTransaction() {
			//	t.Logf("append seq = %d, # = %d txid = %s <-- branch transaction", seqIdx, i, tx.IDShort())
			//} else {
			//	t.Logf("append seq = %d, # = %d txid = %s", seqIdx, i, tx.IDShort())
			//}
			if tx.IsBranchTransaction() {
				if printBranchTx {
					t.Logf("branch tx %d : %s", i, state.TransactionBytesToString(txBytes, r.ut.GetUTXO))
				}
			}
			err = wrk.TransactionIn(txBytes)
			require.NoError(r.t, err)
		}
		err := cd.Wait()
		require.NoError(t, err)
		wrk.Stop()
		t.Logf("UTXO tangle:\n%s", r.ut.Info())
	})
}
