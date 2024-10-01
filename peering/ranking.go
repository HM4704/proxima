package peering

import (
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"golang.org/x/exp/maps"
)

func (ps *Peers) adjustRanks() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	sorted := maps.Values(ps.peers)

	// by lastHeartbeatReceived
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].lastHeartbeatReceived.Before(sorted[j].lastHeartbeatReceived)
	})
	for i, p := range sorted {
		p.rankByLastHBReceived = i
	}
	// by clockDifferenceMedian
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].clockDifferenceMedian > sorted[j].clockDifferenceMedian
	})
	for i, p := range sorted {
		p.rankByClockDifference = i
	}
}

func (p *Peer) rank() int {
	return p.rankByLastHBReceived + p.rankByClockDifference
}

func (ps *Peers) _pullTargets() []*Peer {
	ret := make([]*Peer, 0)
	for _, p := range ps.peers {
		if ps._isPullTarget(p) {
			ret = append(ret, p)
		}
	}
	return ret
}

func (ps *Peers) _pullTargetsByRankDesc() []*Peer {
	ret := ps._pullTargets()
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].rank() > ret[j].rank()
	})
	return ret
}

func (ps *Peers) pullTargetsByRankDescLines(prefix ...string) *lines.Lines {
	ret := lines.New(prefix...)
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for _, p := range ps._pullTargetsByRankDesc() {
		ret.Add("%s: %d", ShortPeerIDString(p.id), p.rank())
	}
	return ret
}

func (ps *Peers) chooseNPullTargets(n int) []peer.ID {
	return ps.chooseBestNPullTargetsBestAndRandom(n)
}

// chooseBestNPullTargetsRandom just select random n out of all pull targets
func (ps *Peers) chooseBestNPullTargetsRandom(n int) []peer.ID {
	if n <= 0 {
		return nil
	}
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	ret := make([]peer.ID, 0)
	targets := ps._pullTargets()
	for _, i := range util.RandomNOutOfMPractical(n, len(targets)) {
		ret = append(ret, targets[i].id)
	}
	return ret
}

func (ps *Peers) chooseBestNPullTargetsBestAndRandom(n int) []peer.ID {
	if n <= 0 {
		return nil
	}
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	candidates := ps._pullTargetsByRankDesc()

	ret := make([]peer.ID, 0)
	if len(candidates) > n {
		// first the best one, the rest random
		ret = append(ret, candidates[0].id)
		for _, p := range util.RandomElements(n-1, candidates[1:]...) {
			ret = append(ret, p.id)
		}
	} else {
		for _, p := range candidates {
			ret = append(ret, p.id)
		}
	}
	return ret
}

//
//func (ps *Peers) randomPullTargets(n int) []peer.ID {
//	rankedPeers, ranksCumulative := ps.pullTargetsRanked()
//	return chooseRandomRankedPeers(n, rankedPeers, ranksCumulative)
//}
//
//func (ps *Peers) pullTargetsRanked() ([]peer.ID, []int) {
//	ret := make([]peer.ID, 0)
//	retRankCumulative := make([]int, 0)
//	ps.forEachPeerRLock(func(p *Peer) bool {
//		if !ps._isPullTarget(p) {
//			return true
//		}
//		r := p.rank()
//		if l := len(retRankCumulative); l == 0 {
//			retRankCumulative = append(retRankCumulative, r)
//		} else {
//			retRankCumulative = append(retRankCumulative, retRankCumulative[l-1]+r)
//		}
//		ret = append(ret, p.id)
//		return true
//	})
//	return ret, retRankCumulative
//}
//
//// random selection algorithm proportional to the rank taken from https://en.wikipedia.org/wiki/Fitness_proportionate_selection
//
//func chooseRandomRankedPeers(n int, rankedPeers []peer.ID, cumulativeRank []int) []peer.ID {
//	util.Assertf(len(rankedPeers) == len(cumulativeRank), "len(rankedPeers)==len(cumulativeRank)")
//
//	if n >= len(rankedPeers) {
//		return rankedPeers
//	}
//	util.Assertf(n < len(rankedPeers), "n < len(rankedPeers)")
//
//	rndIdx := chooseRandomIndex(cumulativeRank)
//
//	util.Assertf(rndIdx < len(cumulativeRank), "rndIdx < len(cumulativeRank)")
//	if rndIdx+n > len(rankedPeers) {
//		rndIdx = len(rankedPeers) - n
//	}
//	return rankedPeers[rndIdx : rndIdx+n]
//}
//
//func chooseRandomIndex(cumulativeRank []int) int {
//	util.Assertf(len(cumulativeRank) > 0, "len(cumulativeRank)>0")
//
//	rnd := rand.Intn(cumulativeRank[len(cumulativeRank)-1])
//	for i, v := range cumulativeRank {
//		if rnd < v {
//			return i
//		}
//	}
//	panic("inconsistency in chooseRandomIndex")
//}
