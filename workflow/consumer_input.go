package workflow

import (
	"fmt"

	"github.com/lunfardo314/proxima/core"
	"github.com/lunfardo314/proxima/peering"
	"github.com/lunfardo314/proxima/transaction"
	"github.com/lunfardo314/proxima/util/eventtype"
	"github.com/lunfardo314/proxima/util/seenset"
)

// PrimaryInputConsumer is where transaction enters the workflow pipeline

const PrimaryInputConsumerName = "input"

// PrimaryInputConsumerData is a basic data of the raw transaction
type (
	TransactionSourceType byte

	// PrimaryInputConsumerData is an input message type for this consumer
	PrimaryInputConsumerData struct {
		Tx            *transaction.Transaction
		SourceType    TransactionSourceType
		ReceivedFrom  peering.PeerID
		eventCallback func(event string, data any)
	}

	TransactionInOption func(*PrimaryInputConsumerData)

	PrimaryConsumer struct {
		*Consumer[*PrimaryInputConsumerData]
		seen *seenset.SeenSet[core.TransactionID]
	}
)

const (
	TransactionSourceTypeAPI = TransactionSourceType(iota)
	TransactionSourceTypeSequencer
	TransactionSourceTypePeer
	TransactionSourceTypeStore
)

// EventCodeDuplicateTx this consumer rises the event with transaction ID as a parameter whenever duplicate is detected
var EventCodeDuplicateTx = eventtype.RegisterNew[*core.TransactionID]("duplicateTx")

// initPrimaryInputConsumer initializes the consumer
func (w *Workflow) initPrimaryInputConsumer() {
	c := &PrimaryConsumer{
		Consumer: NewConsumer[*PrimaryInputConsumerData](PrimaryInputConsumerName, w),
		seen:     seenset.New[core.TransactionID](),
	}
	c.AddOnConsume(func(inp *PrimaryInputConsumerData) {
		// tracing every input message
		c.Debugf(inp, "IN")
	})
	c.AddOnConsume(c.consume) // process input
	c.AddOnClosed(func() {
		// cleanup downstream on close
		w.pullConsumer.Stop()
		w.preValidateConsumer.Stop()
		w.respondTxQueryConsumer.Stop()
		w.txOutboundConsumer.Stop()

		w.terminateWG.Done()
	})

	nmDuplicate := EventCodeDuplicateTx.String()
	w.MustOnEvent(EventCodeDuplicateTx, func(txid *core.TransactionID) {
		// log duplicate transaction upon event
		c.glb.IncCounter(c.Name() + "." + nmDuplicate)
		c.Log().Debugf("%s: %s", nmDuplicate, txid.StringShort())
	})
	// the consumer is globally known in the workflow
	w.primaryInputConsumer = c
}

// consume processes the input
func (c *PrimaryConsumer) consume(inp *PrimaryInputConsumerData) {
	inp.eventCallback(PrimaryInputConsumerName+".in", inp.Tx)

	// the input is preparse transaction with base validation ok. It means it is identifiable as a transaction
	if c.isDuplicate(inp.Tx.ID()) {
		// if duplicate, rise the event
		inp.eventCallback("finish."+PrimaryInputConsumerName, fmt.Errorf("duplicate %s", inp.Tx.IDShort()))
		c.glb.PostEvent(EventCodeDuplicateTx, inp.Tx.ID())
		return
	}
	c.glb.IncCounter(c.Name() + ".out")
	// passes identifiable transaction which is not a duplicate to the pre-validation consumer
	c.glb.preValidateConsumer.Push(&PreValidateConsumerInputData{
		PrimaryInputConsumerData: inp,
	})
}

func (c *PrimaryConsumer) isDuplicate(txid *core.TransactionID) bool {
	if c.glb.utxoTangle.HasTransactionOnTangle(txid) {
		c.glb.IncCounter(c.Name() + ".duplicate.tangle")
		c.Log().Debugf("already on tangle -- " + txid.StringShort())
		return true
	}
	if c.seen.Seen(*txid) {
		c.glb.IncCounter(c.Name() + ".duplicate.seen")
		c.Log().Debugf("already seen -- " + txid.String())
		return true
	}
	return false
}
