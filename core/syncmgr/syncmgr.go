package syncmgr

import (
	"sync/atomic"
	"time"

	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/util"
	"github.com/spf13/viper"
)

// SyncManager is a daemon which monitors how far is the latest slot in the state DB from the
// current time. If difference becomes bigger than threshold, it starts pulling sync portions of
// branches from other nodes, while ignoring current flow of transactions
// SyncManager is optional optimization of the sync process. It can be enabled/disabled in the config

type (
	Environment interface {
		global.NodeGlobal
		StateStore() global.StateStore
		PullSyncPortion(startingFrom ledger.Slot, maxSlots int)
	}

	SyncManager struct {
		Environment
		syncPortionSlots            int
		syncToleranceThresholdSlots int

		endOfPortionCh                       chan struct{}
		syncPortionRequestedAtLeastUntilSlot ledger.Slot
		syncPortionDeadline                  time.Time
		latestSlotInDB                       atomic.Uint32 // cache for IgnoreFutureTxID

		loggedWhen time.Time
	}
)

func StartSyncManagerFromConfig(env Environment) *SyncManager {
	if !viper.GetBool("workflow.sync_manager.enable") {
		env.Infof0("[sync manager] is DISABLED")
		return nil
	}
	d := &SyncManager{
		Environment:                 env,
		syncPortionSlots:            viper.GetInt("workflow.sync_manager.sync_portion_slots"),
		syncToleranceThresholdSlots: viper.GetInt("workflow.sync_manager.sync_tolerance_threshold_slots"),
		endOfPortionCh:              make(chan struct{}, 1),
	}
	if d.syncPortionSlots < 1 || d.syncPortionSlots > global.MaxSyncPortionSlots {
		d.syncPortionSlots = global.MaxSyncPortionSlots
	}
	if d.syncToleranceThresholdSlots <= 5 || d.syncToleranceThresholdSlots > d.syncPortionSlots/2 {
		d.syncToleranceThresholdSlots = global.DefaultSyncToleranceThresholdSlots
	}

	go d.syncManagerLoop()
	return d
}

const (
	checkSyncEvery = 500 * time.Millisecond
	// portionExpectedIn when repeat portion pull
	portionExpectedIn = 5 * time.Second
)

func (d *SyncManager) syncManagerLoop() {
	d.Infof0("[sync manager] has been started. Sync portion: %d slots. Sync tolerance: %d slots",
		d.syncPortionSlots, d.syncToleranceThresholdSlots)

	for {
		select {
		case <-d.Ctx().Done():
			d.Infof0("[sync manager] stopped ")
			return

		case <-d.endOfPortionCh:
			d.Infof1("[sync manager] end of sync portion")
			d.checkSync(true)

		case <-time.After(checkSyncEvery):
			d.checkSync(false)
		}
	}
}

func (d *SyncManager) checkSync(endOfPortion bool) {
	latestSlotInDB := multistate.FetchLatestSlot(d.StateStore())
	d.latestSlotInDB.Store(uint32(latestSlotInDB)) // cache

	slotNow := ledger.TimeNow().Slot()
	util.Assertf(latestSlotInDB <= slotNow, "latestSlot (%d) <= slotNow (%d)", latestSlotInDB, slotNow)

	behind := slotNow - latestSlotInDB
	if int(behind) <= d.syncToleranceThresholdSlots {
		// synced or almost synced. Do not need to pull portions
		d.syncPortionRequestedAtLeastUntilSlot = 0
		d.syncPortionDeadline = time.Time{}
		return
	}
	if time.Since(d.loggedWhen) > 1*time.Second {
		d.Infof1("[sync manager] latest synced slot %d is behind current slot %d by %d",
			latestSlotInDB, slotNow, behind)
		d.loggedWhen = time.Now()
	}

	// above threshold, not synced
	if latestSlotInDB < d.syncPortionRequestedAtLeastUntilSlot {
		// we already pulled portion, but it is not here yet, it seems
		if !endOfPortion && time.Now().Before(d.syncPortionDeadline) {
			// still waiting for the portion, do nothing
			return
		}
		// repeat pull portion
	}

	d.syncPortionRequestedAtLeastUntilSlot = latestSlotInDB + ledger.Slot(d.syncPortionSlots)
	if d.syncPortionRequestedAtLeastUntilSlot > slotNow {
		d.syncPortionRequestedAtLeastUntilSlot = slotNow
	}
	d.syncPortionDeadline = time.Now().Add(portionExpectedIn)
	d.PullSyncPortion(latestSlotInDB, d.syncPortionSlots)
}

func (d *SyncManager) NotifyEndOfPortion() {
	select {
	case d.endOfPortionCh <- struct{}{}:
	default:
	}
}

// IgnoreFutureTxID returns true if transaction is too far in the future from the latest synced branch in DB
// We want to ignore all the current flow of transactions while syncing the state with sync manager
// After the state become synced, the tx flow will be accepted
func (d *SyncManager) IgnoreFutureTxID(txid *ledger.TransactionID) bool {
	slotNow := int(ledger.TimeNow().Slot())
	latestSlot := int(d.latestSlotInDB.Load())
	util.Assertf(latestSlot <= slotNow, "latestSlot <= slotNow")
	if slotNow-latestSlot < d.syncToleranceThresholdSlots {
		return false // accept all if not very unsynced
	}
	// not synced. Ignore all too close to the present time
	ignore := int(txid.Slot()) >= slotNow-2
	if ignore && txid.IsBranchTransaction() {
		d.Infof1("[sync manager] ignore transaction while syncing %s", txid.StringShort())
	}
	return ignore
}