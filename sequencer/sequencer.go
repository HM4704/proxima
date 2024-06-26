package sequencer

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sync"
	"time"

	"github.com/lunfardo314/proxima/core/txmetadata"
	"github.com/lunfardo314/proxima/core/vertex"
	"github.com/lunfardo314/proxima/core/workflow"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/sequencer/backlog"
	"github.com/lunfardo314/proxima/sequencer/factory"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"go.uber.org/zap"
)

type (
	Sequencer struct {
		*workflow.Workflow
		ctx            context.Context    // local context
		stopFun        context.CancelFunc // local stop function
		sequencerID    ledger.ChainID
		controllerKey  ed25519.PrivateKey
		config         *ConfigOptions
		log            *zap.SugaredLogger
		backlog        *backlog.InputBacklog
		factory        *factory.MilestoneFactory
		milestoneCount int
		branchCount    int
		prevTimeTarget ledger.Time
		infoMutex      sync.RWMutex
		info           Info
		//
		onCallbackMutex      sync.RWMutex
		onMilestoneSubmitted func(seq *Sequencer, vid *vertex.WrappedTx)
		onExit               func()
	}

	Info struct {
		In                     int
		Out                    int
		InflationAmount        uint64
		NumConsumedFeeOutputs  int
		NumFeeOutputsInTippool int
		NumOtherMsInTippool    int
		LedgerCoverage         uint64
		PrevLedgerCoverage     uint64
	}
)

const TraceTag = "sequencer"

func New(glb *workflow.Workflow, seqID ledger.ChainID, controllerKey ed25519.PrivateKey, opts ...ConfigOption) (*Sequencer, error) {
	cfg := configOptions(opts...)
	ret := &Sequencer{
		Workflow:      glb,
		sequencerID:   seqID,
		controllerKey: controllerKey,
		config:        cfg,
		log:           glb.Log().Named(fmt.Sprintf("[%s-%s]", cfg.SequencerName, seqID.StringVeryShort())),
	}
	ret.ctx, ret.stopFun = context.WithCancel(glb.Ctx())
	var err error

	if ret.backlog, err = backlog.New(ret); err != nil {
		return nil, err
	}
	if ret.factory, err = factory.New(ret); err != nil {
		return nil, err
	}
	if err = ret.LoadSequencerTips(seqID); err != nil {
		return nil, err
	}
	ret.Log().Infof("sequencer is starting with config:\n%s", cfg.lines(seqID, ledger.AddressED25519FromPrivateKey(controllerKey), "     ").String())
	return ret, nil
}

func NewFromConfig(name string, glb *workflow.Workflow) (*Sequencer, error) {
	cfg, seqID, controllerKey, err := paramsFromConfig(name)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return New(glb, seqID, controllerKey, cfg...)
}

func (seq *Sequencer) Start() {
	runFun := func() {
		seq.MarkWorkProcessStarted(seq.config.SequencerName)
		defer seq.MarkWorkProcessStopped(seq.config.SequencerName)

		if !seq.ensureFirstMilestone() {
			seq.log.Warnf("can't start sequencer. EXIT..")
			return
		}
		seq.mainLoop()

		seq.onCallbackMutex.RLock()
		defer seq.onCallbackMutex.RUnlock()

		if seq.onExit != nil {
			seq.onExit()
		}
	}

	const debuggerFriendly = true
	if debuggerFriendly {
		go runFun()
	} else {
		util.RunWrappedRoutine(seq.config.SequencerName+"[mainLoop]", runFun, func(err error) bool {
			seq.log.Fatal(err)
			return false
		})
	}
}

func (cfg *ConfigOptions) lines(seqID ledger.ChainID, controller ledger.AddressED25519, prefix ...string) *lines.Lines {
	return lines.New(prefix...).
		Add("ID: %s", seqID.String()).
		Add("Controller: %s", controller.String()).
		Add("Name: %s", cfg.SequencerName).
		Add("Pace: %d ticks", cfg.Pace).
		Add("MaxTagAlongInputs: %d", cfg.MaxTagAlongInputs).
		Add("MaxTargetTs: %s", cfg.MaxTargetTs.String()).
		Add("MaxBranches: %d", cfg.MaxBranches).
		Add("DelayStart: %v", cfg.DelayStart).
		Add("BacklogTTLSlots: %d", cfg.BacklogTTLSlots).
		Add("MilestoneTTLSlots: %d", cfg.MilestonesTTLSlots).
		Add("LogAttacherStats: %v", cfg.LogAttacherStats)
}

func (seq *Sequencer) Ctx() context.Context {
	return seq.ctx
}

func (seq *Sequencer) Stop() {
	seq.stopFun()
}

const ensureStartingMilestoneTimeout = time.Second

func (seq *Sequencer) ensureFirstMilestone() bool {
	ctx, cancel := context.WithTimeout(seq.Ctx(), ensureStartingMilestoneTimeout)
	var startingMilestoneOutput vertex.WrappedOutput

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				startingMilestoneOutput = seq.factory.OwnLatestMilestoneOutput()
				if startingMilestoneOutput.VID != nil {
					cancel()
					return
				}
			}
		}
	}()
	<-ctx.Done()
	if startingMilestoneOutput.VID == nil {
		seq.log.Errorf("failed to find a milestone to start")
		return false
	}
	amount, lock, err := startingMilestoneOutput.AmountAndLock()
	if err != nil {
		seq.log.Errorf("sequencer start output %s is not available: %v", startingMilestoneOutput.IDShortString(), err)
		return false
	}
	if !ledger.BelongsToAccount(lock, ledger.AddressED25519FromPrivateKey(seq.controllerKey)) {
		seq.log.Errorf("provided private key does match sequencer lock %s", lock.String())
		return false

	}
	seq.log.Infof("sequencer will start with the milestone output %s and amount %s (%s%% initial supply)",
		startingMilestoneOutput.IDShortString(), util.GoTh(amount), util.PercentString(int(amount), int(ledger.L().ID.InitialSupply)))

	seq.factory.AddOwnMilestone(startingMilestoneOutput.VID)

	sleepDuration := ledger.SleepDurationUntilFutureLedgerTime(startingMilestoneOutput.Timestamp())
	if sleepDuration > 0 {
		seq.log.Infof("will delay start for %v to sync starting milestone with the real clock", sleepDuration)
		time.Sleep(sleepDuration)
	}
	return true
}

func (seq *Sequencer) Backlog() *backlog.InputBacklog {
	return seq.backlog
}

func (seq *Sequencer) SequencerID() ledger.ChainID {
	return seq.sequencerID
}

func (seq *Sequencer) ControllerPrivateKey() ed25519.PrivateKey {
	return seq.controllerKey
}

func (seq *Sequencer) SequencerName() string {
	return seq.config.SequencerName
}

func (seq *Sequencer) Log() *zap.SugaredLogger {
	return seq.log
}

func (seq *Sequencer) mainLoop() {
	beginAt := time.Now().Add(seq.config.DelayStart)
	if seq.config.DelayStart > 0 {
		seq.log.Infof("wait for %v before starting the main loop", seq.config.DelayStart)
	}
	time.Sleep(time.Until(beginAt))

	seq.Log().Infof("STARTING sequencer")
	defer func() {
		seq.Log().Infof("sequencer STOPPING..")
		_ = seq.Log().Sync()
	}()

	for {
		select {
		case <-seq.Ctx().Done():
			return
		default:
			if !seq.doSequencerStep() {
				return
			}
		}
	}
}

func (seq *Sequencer) doSequencerStep() bool {
	seq.Tracef(TraceTag, "doSequencerStep")
	if seq.config.MaxBranches != 0 && seq.branchCount >= seq.config.MaxBranches {
		seq.log.Infof("reached max limit of branch milestones %d -> stopping", seq.config.MaxBranches)
		return false
	}

	timerStart := time.Now()

	targetTs, prevMsTs := seq.getNextTargetTime()

	seq.Assertf(ledger.ValidSequencerPace(prevMsTs, targetTs), "target is closer than allowed pace (%d): %s -> %s",
		ledger.TransactionPaceSequencer(), prevMsTs.String, targetTs.String)

	seq.Assertf(!targetTs.Before(seq.prevTimeTarget), "wrong target ts %s: must not be before previous target %s",
		targetTs.String(), seq.prevTimeTarget.String())

	seq.prevTimeTarget = targetTs

	if seq.config.MaxTargetTs != ledger.NilLedgerTime && targetTs.After(seq.config.MaxTargetTs) {
		seq.log.Infof("next target ts %s is after maximum ts %s -> stopping", targetTs, seq.config.MaxTargetTs)
		return false
	}

	seq.Tracef(TraceTag, "target ts: %s. Now is: %s", targetTs, ledger.TimeNow())

	msTx, meta := seq.factory.StartProposingForTargetLogicalTime(targetTs)
	if msTx == nil {
		seq.Tracef(TraceTag, "failed to generate msTx for target %s. Now is %s", targetTs, ledger.TimeNow())
		return true
	}

	seq.Tracef(TraceTag, "produced milestone %s for the target logical time %s in %v. Meta: %s",
		msTx.IDShortString, targetTs, time.Since(timerStart), meta.String)

	msVID := seq.submitMilestone(msTx, meta)
	if msVID == nil {
		return true
	}

	seq.factory.AddOwnMilestone(msVID)
	seq.milestoneCount++
	if msVID.IsBranchTransaction() {
		seq.branchCount++
	}
	seq.updateInfo(msVID)
	seq.runOnMilestoneSubmitted(msVID)
	return true
}

const sleepWaitingCurrentMilestoneTime = 10 * time.Millisecond

func (seq *Sequencer) getNextTargetTime() (ledger.Time, ledger.Time) {
	var prevMilestoneTs ledger.Time

	currentMsOutput := seq.factory.OwnLatestMilestoneOutput()
	seq.Assertf(currentMsOutput.VID != nil, "currentMsOutput.VID != nil")
	prevMilestoneTs = currentMsOutput.Timestamp()

	// synchronize clock
	nowis := ledger.TimeNow()
	if nowis.Before(prevMilestoneTs) {
		waitDuration := time.Duration(ledger.DiffTicks(prevMilestoneTs, nowis)) * ledger.TickDuration()
		seq.log.Warnf("nowis (%s) is before last milestone ts (%s). Sleep %v",
			nowis.String(), prevMilestoneTs.String(), waitDuration)
		time.Sleep(waitDuration)
	}
	nowis = ledger.TimeNow()
	for ; nowis.Before(prevMilestoneTs); nowis = ledger.TimeNow() {
		seq.log.Warnf("nowis (%s) is before last milestone ts (%s). Sleep %v",
			nowis.String(), prevMilestoneTs.String(), sleepWaitingCurrentMilestoneTime)
		time.Sleep(sleepWaitingCurrentMilestoneTime)
	}
	// ledger time now is approximately equal to the clock time
	nowis = ledger.TimeNow()
	seq.Assertf(!nowis.Before(prevMilestoneTs), "!core.TimeNow().Before(prevMilestoneTs)")

	// TODO take into account average speed of proposal generation

	targetAbsoluteMinimum := ledger.MaxTime(
		prevMilestoneTs.AddTicks(seq.config.Pace),
		nowis.AddTicks(1),
	)
	nextSlotBoundary := nowis.NextSlotBoundary()

	if !targetAbsoluteMinimum.Before(nextSlotBoundary) {
		return targetAbsoluteMinimum, prevMilestoneTs
	}
	// absolute minimum is before the next slot boundary, take the time now as a baseline
	minimumTicksAheadFromNow := (seq.config.Pace * 2) / 3 // seq.config.Pace
	targetAbsoluteMinimum = ledger.MaxTime(targetAbsoluteMinimum, nowis.AddTicks(minimumTicksAheadFromNow))
	if !targetAbsoluteMinimum.Before(nextSlotBoundary) {
		return targetAbsoluteMinimum, prevMilestoneTs
	}

	if targetAbsoluteMinimum.TicksToNextSlotBoundary() <= seq.config.Pace {
		return nextSlotBoundary, prevMilestoneTs
	}

	return targetAbsoluteMinimum, prevMilestoneTs
}

const submitTimeout = 5 * time.Second

func (seq *Sequencer) submitMilestone(tx *transaction.Transaction, meta *txmetadata.TransactionMetadata) *vertex.WrappedTx {
	seq.Tracef(TraceTag, "submit new milestone %s, meta: %s", tx.IDShortString, meta.String)
	deadline := time.Now().Add(submitTimeout)
	vid, err := seq.SequencerMilestoneAttachWait(tx.Bytes(), meta, submitTimeout, seq.config.LogAttacherStats)
	if err != nil {
		seq.Log().Errorf("failed to submit new milestone %s: '%v'", tx.IDShortString(), err)
		return nil
	}

	seq.Tracef(TraceTag, "new milestone %s submitted successfully", tx.IDShortString)
	if !seq.waitMilestoneInTippool(vid, deadline) {
		seq.Log().Errorf("timed out while waiting %v for submitted milestone %s in the tippool", submitTimeout, vid.IDShortString())
		return nil
	}
	return vid
}

func (seq *Sequencer) waitMilestoneInTippool(vid *vertex.WrappedTx, deadline time.Time) bool {
	for {
		if time.Now().After(deadline) {
			return false
		}
		if seq.GetLatestMilestone(seq.sequencerID) == vid {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (seq *Sequencer) OnMilestoneSubmitted(fun func(seq *Sequencer, ms *vertex.WrappedTx)) {
	seq.onCallbackMutex.Lock()
	defer seq.onCallbackMutex.Unlock()

	if seq.onMilestoneSubmitted == nil {
		seq.onMilestoneSubmitted = fun
	} else {
		prevFun := seq.onMilestoneSubmitted
		seq.onMilestoneSubmitted = func(seq *Sequencer, ms *vertex.WrappedTx) {
			prevFun(seq, ms)
			fun(seq, ms)
		}
	}
}

func (seq *Sequencer) OnExit(fun func()) {
	seq.onCallbackMutex.Lock()
	defer seq.onCallbackMutex.Unlock()

	if seq.onExit == nil {
		seq.onExit = fun
	} else {
		prevFun := seq.onExit
		seq.onExit = func() {
			prevFun()
			fun()
		}
	}
}

func (seq *Sequencer) runOnMilestoneSubmitted(ms *vertex.WrappedTx) {
	seq.onCallbackMutex.RLock()
	defer seq.onCallbackMutex.RUnlock()

	if seq.onMilestoneSubmitted != nil {
		seq.onMilestoneSubmitted(seq, ms)
	}
}

func (seq *Sequencer) MaxTagAlongOutputs() int {
	return seq.config.MaxTagAlongInputs
}

func (seq *Sequencer) BacklogTTLSlots() int {
	return seq.config.BacklogTTLSlots
}

func (seq *Sequencer) MilestonesTTLSlots() int {
	return seq.config.MilestonesTTLSlots
}
