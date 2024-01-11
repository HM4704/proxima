package transaction

import (
	"bytes"
	"encoding/hex"
	"os"
	"slices"

	"github.com/lunfardo314/easyfl"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lazybytes"
	"github.com/lunfardo314/proxima/util/lines"
	"golang.org/x/crypto/blake2b"
)

func SaveTransactionAsFile(txBytes []byte, fname ...string) error {
	var fn string
	if len(fname) > 0 {
		fn = fname[0]
	} else {
		txID, _, err := IDAndTimestampFromTransactionBytes(txBytes)
		if err != nil {
			return err
		}
		fn = txID.AsFileName()
	}
	return os.WriteFile(fn, txBytes, 0644)
}

func (ctx *TransactionContext) String() string {
	return ctx.Lines().String()
}

func (ctx *TransactionContext) Lines(prefix ...string) *lines.Lines {
	txid := ctx.TransactionID()
	ret := lines.New(prefix...)
	ret.Add("Transaction ID: %s, size: %d", txid.String(), len(ctx.TransactionBytes()))
	tsBin, ts := ctx.MustTimestampData()
	ret.Add("Timestamp: %s %s", easyfl.Fmt(tsBin), ts)

	seqIdx, stemIdx := ctx.SequencerAndStemOutputIndices()
	ret.Add("Sequencer output index: %d, sequencer milestone: %v", seqIdx, seqIdx != 0xff)
	ret.Add("Stem output index: %d, stem output: %v", stemIdx, seqIdx != 0xff && stemIdx != 0xff)

	ret.Add("Total amount stored: %s", util.GoThousands(ctx.TotalAmountStored()))

	inpCom := ctx.InputCommitment()
	ret.Add("Input commitment: %s", easyfl.Fmt(inpCom))
	h := ctx.ConsumedOutputHash()
	eqCom := ""
	if !bytes.Equal(inpCom, h[:]) {
		eqCom = "   !!! NOT EQUAL WITH INPUT COMMITMENT !!!!"
	}
	ret.Add("Consumed output hash: %s%s", easyfl.Fmt(h[:]), eqCom)
	sign := ctx.Signature()
	ret.Add("Signature: %s", easyfl.Fmt(sign))
	if len(sign) == 96 {
		sender := blake2b.Sum256(sign[64:])
		ret.Add("     ED25519 sender address: %s", easyfl.Fmt(sender[:]))
	}

	ret.Add("Endorsements (%d):", ctx.NumEndorsements())
	ctx.ForEachEndorsement(func(idx byte, txid *ledger.TransactionID) bool {
		ret.Add("  %d: %s", idx, txid.String())
		return true
	})

	ret.Add("Inputs (%d consumed outputs): ", ctx.NumInputs())
	ctx.ForEachConsumedOutput(func(idx byte, oid *ledger.OutputID, out *ledger.Output) bool {
		if out == nil {
			ret.Add("  #%d: %s (parse error)", idx, oid.String())
			return true
		}
		unlockBin := ctx.UnlockData(idx)
		arr := lazybytes.ArrayFromBytesReadOnly(unlockBin)
		ret.Add("  #%d: %s", idx, oid.String()).
			Add("       bytes (%d): %s", len(out.Bytes()), hex.EncodeToString(out.Bytes())).
			Append(out.Lines("     ")).
			Add("     Unlock data: %s", arr.ParsedString())
		return true
	})

	ret.Add("Outputs (%d produced): ", ctx.NumProducedOutputs())
	totalSum := uint64(0)
	ctx.ForEachProducedOutput(func(idx byte, out *ledger.Output, oid *ledger.OutputID) bool {
		if out == nil {
			ret.Add("  #%d : parse error", idx)
			return true
		}
		totalSum += out.Amount()
		chainIdStr := ""
		if cc, i := out.ChainConstraint(); i != 0xff {
			var cid ledger.ChainID
			if cc.IsOrigin() {
				oid := ledger.NewOutputID(txid, idx)
				cid = ledger.OriginChainID(&oid)
			} else {
				cid = cc.ID
			}
			chainIdStr = "                      chainID: " + cid.Short()
		}
		ret.Add("  #%d %s", idx, oid.String()).
			Add("       bytes (%d): %s", len(out.Bytes()), hex.EncodeToString(out.Bytes())).
			Append(out.Lines("     ")).
			Add(chainIdStr)
		return true
	})
	ret.Add("TOTAL: %s", util.GoThousands(totalSum))
	return ret
}

func ParseBytesToString(txBytes []byte, fetchOutput func(oid *ledger.OutputID) ([]byte, bool)) string {
	ctx, err := ContextFromTransferableBytes(txBytes, fetchOutput)
	if err != nil {
		return err.Error()
	}
	return ctx.String()
}

func PickOutputFromListFunc(lst []*ledger.OutputWithID) func(oid *ledger.OutputID) ([]byte, bool) {
	return func(oid *ledger.OutputID) ([]byte, bool) {
		idx := slices.IndexFunc(lst, func(o *ledger.OutputWithID) bool {
			return o.ID == *oid
		})
		if idx < 0 {
			return nil, false
		}
		return lst[idx].Output.Bytes(), true
	}
}
