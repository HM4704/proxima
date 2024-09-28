package peering

import (
	"math/rand"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/util"
	"golang.org/x/exp/maps"
)

func (ps *Peers) adjustRanks() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	sorted := maps.Values(ps.peers)

	// by lastMsgReceived
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].lastMsgReceived.Before(sorted[j].lastMsgReceived)
	})
	for i, p := range sorted {
		p.rankByLastMsgReceived = i
	}

	// by msg counter
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].msgCounter < sorted[j].msgCounter
	})
	for i, p := range sorted {
		p.rankByMsgCounter = i
	}

	// by clockDifferenceMedian
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].clockDifferenceMedian > sorted[j].clockDifferenceMedian
	})
	for i, p := range sorted {
		p.rankByClockDifference = i
	}

	// by errors
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].errorCounter > sorted[j].errorCounter
	})
	for i, p := range sorted {
		p.rankByErrors = i
	}

}

func (p *Peer) rank() int {
	//return p.rankByLastMsgReceived + p.rankByMsgCounter + p.rankByErrors + (3*p.rankByClockDifference)/2
	return p.rankByLastMsgReceived + p.rankByClockDifference
}

func (ps *Peers) randomPullTargets(n int) []peer.ID {
	rankedPeers, ranksCumulative := ps.pullTargetsRanked()
	return chooseRandomRankedPeers(n, rankedPeers, ranksCumulative)
}

func (ps *Peers) pullTargetsRanked() ([]peer.ID, []int) {
	ret := make([]peer.ID, 0)
	retRankCumulative := make([]int, 0)
	ps.forEachPeerRLock(func(p *Peer) bool {
		if !ps._isPullTarget(p) {
			return true
		}
		r := p.rank()
		if l := len(retRankCumulative); l == 0 {
			retRankCumulative = append(retRankCumulative, r)
		} else {
			retRankCumulative = append(retRankCumulative, retRankCumulative[l-1]+r)
		}
		ret = append(ret, p.id)
		return true
	})
	return ret, retRankCumulative
}

// random selection algorithm proportional to the rank taken from https://en.wikipedia.org/wiki/Fitness_proportionate_selection

func chooseRandomRankedPeers(n int, rankedPeers []peer.ID, cumulativeRank []int) []peer.ID {
	util.Assertf(len(rankedPeers) == len(cumulativeRank), "len(rankedPeers)==len(cumulativeRank)")

	if n >= len(rankedPeers) {
		return rankedPeers
	}
	util.Assertf(n < len(rankedPeers), "n < len(rankedPeers)")

	rndIdx := chooseRandomIndex(cumulativeRank)

	util.Assertf(rndIdx < len(cumulativeRank), "rndIdx < len(cumulativeRank)")
	if rndIdx+n > len(rankedPeers) {
		rndIdx = len(rankedPeers) - n
	}
	return rankedPeers[rndIdx : rndIdx+n]
}

func chooseRandomIndex(cumulativeRank []int) int {
	util.Assertf(len(cumulativeRank) > 0, "len(cumulativeRank)>0")

	rnd := rand.Intn(cumulativeRank[len(cumulativeRank)-1])
	for i, v := range cumulativeRank {
		if rnd < v {
			return i
		}
	}
	panic("inconsistency in chooseRandomIndex")
}
