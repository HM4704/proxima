package peering

import (
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/core/txmetadata"
	"github.com/lunfardo314/unitrie/common"
)

func (ps *Peers) gossipStreamHandler(stream network.Stream) {
	id := stream.Conn().RemotePeer()

	if ps.isInBlacklist(id) {
		_ = stream.Reset()
		return
	}

	p := ps.getPeer(id)
	if p == nil {
		// peer not found
		_ = stream.Reset()
		ps.Tracef(TraceTag, "txBytes: unknown peer %s", id.String())
		return
	}

	txBytesWithMetadata, err := readFrame(stream)
	if err != nil {
		_ = stream.Reset()
		ps.dropPeer(p.id, "read error")
		ps.Log().Errorf("error while reading message from peer %s: %v", id.String(), err)
		return
	}
	metadataBytes, txBytes, err := txmetadata.SplitTxBytesWithMetadata(txBytesWithMetadata)
	if err != nil {
		_ = stream.Reset()
		ps.dropPeer(p.id, "error while parsing tx metadata")
		ps.Log().Errorf("error while parsing tx message from peer %s: %v", id.String(), err)
		return
	}
	metadata, err := txmetadata.TransactionMetadataFromBytes(metadataBytes)
	if err != nil {
		_ = stream.Reset()
		ps.dropPeer(p.id, "error while parsing tx metadata")
		ps.Log().Errorf("error while parsing tx message metadata from peer %s: %v", id.String(), err)
		return
	}

	defer stream.Close()

	p.evidence(_evidenceActivity("gossip"))
	ps.onReceiveTx(id, txBytes, metadata)
}

func (ps *Peers) GossipTxBytesToPeers(txBytes []byte, metadata *txmetadata.TransactionMetadata, except ...peer.ID) int {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	countSent := 0
	for id, p := range ps.peers {

		if _, inBlacklist := ps.blacklist[id]; inBlacklist {
			continue
		}
		if len(except) > 0 && id == except[0] {
			continue
		}
		if !p.isAlive() {
			continue
		}
		if ps.SendTxBytesWithMetadataToPeer(id, txBytes, metadata) {
			countSent++
		}
	}
	return countSent
}

func (ps *Peers) SendTxBytesWithMetadataToPeer(id peer.ID, txBytes []byte, metadata *txmetadata.TransactionMetadata) bool {
	ps.Tracef(TraceTag, "SendTxBytesWithMetadataToPeer to %s, length: %d (host %s)",
		func() any { return ShortPeerIDString(id) },
		len(txBytes),
		func() any { return ShortPeerIDString(ps.host.ID()) },
	)

	if ps.isInBlacklist(id) {
		return false
	}
	if p := ps.getPeer(id); p == nil {
		return false
	}

	stream, err := ps.host.NewStream(ps.Ctx(), id, ps.lppProtocolGossip)
	if err != nil {
		ps.Tracef(TraceTag, "SendTxBytesWithMetadataToPeer to %s: %v (host %s)",
			func() any { return ShortPeerIDString(id) }, err,
			func() any { return ShortPeerIDString(ps.host.ID()) },
		)
		return false
	}
	defer stream.Close()

	if err = writeFrame(stream, common.ConcatBytes(metadata.Bytes(), txBytes)); err != nil {
		ps.Tracef("SendTxBytesWithMetadataToPeer.writeFrame to %s: %v (host %s)", ShortPeerIDString(id), err, ShortPeerIDString(ps.host.ID()))
	}
	return err == nil
}
