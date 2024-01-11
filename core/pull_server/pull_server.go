package pull_server

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/multistate"
	"github.com/lunfardo314/proxima/txmetadata"
	"github.com/lunfardo314/proxima/util/queue"
	"github.com/lunfardo314/unitrie/common"
	"go.uber.org/zap"
)

type (
	Environment interface {
		TxBytesStore() global.TxBytesStore
		StateStore() global.StateStore
		SendTxBytesToPeer(id peer.ID, txBytes []byte, metadata *txmetadata.TransactionMetadata) bool
	}

	Input struct {
		TxID   ledger.TransactionID
		PeerID peer.ID
	}

	Queue struct {
		*queue.Queue[*Input]
		env Environment
	}
)

const chanBufferSize = 10

func Start(env Environment, ctx context.Context) *Queue {
	ret := &Queue{
		Queue: queue.NewConsumerWithBufferSize[*Input]("pullReq", chanBufferSize, zap.InfoLevel, nil),
		env:   env,
	}
	ret.AddOnConsume(ret.consume)
	go func() {
		ret.Log().Infof("starting..")
		ret.Run()
	}()

	go func() {
		<-ctx.Done()
		ret.Queue.Stop()
	}()
	return ret
}

func (q *Queue) consume(inp *Input) {
	if txBytes := q.env.TxBytesStore().GetTxBytes(&inp.TxID); len(txBytes) > 0 {
		var root common.VCommitment
		if inp.TxID.IsBranchTransaction() {
			if rr, found := multistate.FetchRootRecord(q.env.StateStore(), inp.TxID); found {
				root = rr.Root
			}
		}
		q.env.SendTxBytesToPeer(inp.PeerID, txBytes, &txmetadata.TransactionMetadata{
			SendType:  txmetadata.SendTypeResponseToPull,
			StateRoot: root,
		})
		//c.tracePull("-> FOUND %s", func() any { return inp.TxID.StringShort() })
	} else {
		// not found -> ignore
		//c.tracePull("-> NOT FOUND %s", func() any { return inp.TxID.StringShort() })
	}
}
