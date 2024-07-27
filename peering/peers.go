package peering

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	p2putil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/lunfardo314/proxima/core/txmetadata"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/util"
	"github.com/multiformats/go-multiaddr"
	"github.com/spf13/viper"
	"golang.org/x/exp/maps"
)

type (
	Environment interface {
		global.NodeGlobal
	}

	Config struct {
		HostIDPrivateKey   ed25519.PrivateKey
		HostID             peer.ID
		HostPort           int
		PreConfiguredPeers map[string]multiaddr.Multiaddr // name -> PeerAddr. Static peers used also for bootstrap
		// MaxDynamicPeers if MaxDynamicPeers <= len(PreConfiguredPeers), autopeering is disabled, otherwise up to
		// MaxDynamicPeers - len(PreConfiguredPeers) will be auto-peered
		MaxDynamicPeers int
	}

	Peers struct {
		Environment
		mutex            sync.RWMutex
		cfg              *Config
		stopOnce         sync.Once
		host             host.Host
		kademliaDHT      *dht.IpfsDHT // not nil if autopeering is enabled
		routingDiscovery *routing.RoutingDiscovery
		peers            map[peer.ID]*Peer // except self/host
		blacklist        map[peer.ID]time.Time
		// on receive handlers
		onReceiveTx              func(from peer.ID, txBytes []byte, mdata *txmetadata.TransactionMetadata)
		onReceivePullTx          func(from peer.ID, txids []ledger.TransactionID)
		onReceivePullSyncPortion func(from peer.ID, startingFrom ledger.Slot, maxBranches int)
		// lpp protocol names
		lppProtocolGossip    protocol.ID
		lppProtocolPull      protocol.ID
		lppProtocolHeartbeat protocol.ID
		rendezvousString     string
	}

	Peer struct {
		id                  peer.ID
		name                string
		isStatic            bool // statically pre-configured (manual peering)
		hasTxStore          bool
		whenAdded           time.Time
		lastMsgReceived     time.Time
		lastMsgReceivedFrom string
		lastLoggedConnected bool // toggle
		// ring buffer with last clock differences
		clockDifferences    [10]time.Duration
		clockDifferencesIdx int
	}
)

const (
	Name     = "peers"
	TraceTag = Name
)

const (
	// protocol name templates. Last component is first 8 bytes of ledger constraint library hash, interpreted as bigendian uint64
	// Peering is only possible between same versions of the ledger.
	// Nodes with different versions of the ledger constraints will just ignore each other
	lppProtocolGossip    = "/proxima/gossip/%d"
	lppProtocolPull      = "/proxima/pull/%d"
	lppProtocolHeartbeat = "/proxima/heartbeat/%d"

	// clockTolerance is how big the difference between local and remote clocks is tolerated.
	// The difference includes difference between local clocks (positive or negative) plus
	// positive heartbeat message latency between peers
	clockTolerance = 5 * time.Second

	// if the node is bootstrap, and it has configured less than numMaxDynamicPeersForBootNodeAtLeast
	// of dynamic peer cap, use this instead
	numMaxDynamicPeersForBootNodeAtLeast = 10

	// heartbeatRate heartbeat issued every period
	heartbeatRate      = 2 * time.Second
	aliveNumHeartbeats = 5 // if no hb over this period, it means not-alive -> dynamic peer will be dropped
	aliveDuration      = time.Duration(aliveNumHeartbeats) * heartbeatRate
	// gracePeriodAfterAdded period of time peer is considered not dead after added even if messages are not coming
	gracePeriodAfterAdded = 10 * heartbeatRate
	logPeersEvery         = 5 * time.Second
)

func NewPeersDummy() *Peers {
	return &Peers{
		peers:                    make(map[peer.ID]*Peer),
		blacklist:                make(map[peer.ID]time.Time),
		onReceiveTx:              func(_ peer.ID, _ []byte, _ *txmetadata.TransactionMetadata) {},
		onReceivePullTx:          func(_ peer.ID, _ []ledger.TransactionID) {},
		onReceivePullSyncPortion: func(_ peer.ID, _ ledger.Slot, _ int) {},
	}
}

func New(env Environment, cfg *Config) (*Peers, error) {
	hostIDPrivateKey, err := p2pcrypto.UnmarshalEd25519PrivateKey(cfg.HostIDPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("wrong private key: %w", err)
	}
	lppHost, err := libp2p.New(
		libp2p.Identity(hostIDPrivateKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.HostPort)),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.NoSecurity,
	)
	if err != nil {
		return nil, fmt.Errorf("unable create libp2p host: %w", err)
	}

	ledgerLibraryHash := ledger.L().Library.LibraryHash()
	rendezvousNumber := binary.BigEndian.Uint64(ledgerLibraryHash[:8])

	ret := &Peers{
		Environment:              env,
		cfg:                      cfg,
		host:                     lppHost,
		peers:                    make(map[peer.ID]*Peer),
		blacklist:                make(map[peer.ID]time.Time),
		onReceiveTx:              func(_ peer.ID, _ []byte, _ *txmetadata.TransactionMetadata) {},
		onReceivePullTx:          func(_ peer.ID, _ []ledger.TransactionID) {},
		onReceivePullSyncPortion: func(_ peer.ID, _ ledger.Slot, _ int) {},
		lppProtocolGossip:        protocol.ID(fmt.Sprintf(lppProtocolGossip, rendezvousNumber)),
		lppProtocolPull:          protocol.ID(fmt.Sprintf(lppProtocolPull, rendezvousNumber)),
		lppProtocolHeartbeat:     protocol.ID(fmt.Sprintf(lppProtocolHeartbeat, rendezvousNumber)),
		rendezvousString:         fmt.Sprintf("%d", rendezvousNumber),
	}

	env.Log().Infof("[peering] rendezvous number is %d", rendezvousNumber)
	for name, maddr := range cfg.PreConfiguredPeers {
		if err = ret.addStaticPeer(maddr, name); err != nil {
			return nil, err
		}
	}
	env.Log().Infof("[peering] number of statically pre-configured peers (manual peering): %d", len(cfg.PreConfiguredPeers))

	if env.IsBootstrapNode() || ret.isAutopeeringEnabled() {
		// autopeering enabled. The node also acts as a bootstrap node
		bootstrapPeers := peerstore.AddrInfos(ret.host.Peerstore(), maps.Keys(ret.peers))
		ret.kademliaDHT, err = dht.New(env.Ctx(), lppHost,
			dht.Mode(dht.ModeAutoServer),
			dht.RoutingTableRefreshPeriod(5*time.Second),
			dht.BootstrapPeers(bootstrapPeers...),
		)
		if err != nil {
			return nil, err
		}

		if err = ret.kademliaDHT.Bootstrap(env.Ctx()); err != nil {
			return nil, err
		}
		ret.routingDiscovery = routing.NewRoutingDiscovery(ret.kademliaDHT)
		p2putil.Advertise(env.Ctx(), ret.routingDiscovery, ret.rendezvousString)

		env.Log().Infof("[peering] autopeering is enabled with max dynamic peers = %d", cfg.MaxDynamicPeers)
	} else {
		env.Log().Infof("[peering] autopeering is disabled")
	}

	go ret.blacklistCleanupLoop()

	env.Log().Infof("[peering] initialized successfully")
	return ret, nil
}

func readPeeringConfig(boot bool) (*Config, error) {
	cfg := &Config{
		PreConfiguredPeers: make(map[string]multiaddr.Multiaddr),
	}
	cfg.HostPort = viper.GetInt("peering.host.port")
	if cfg.HostPort == 0 {
		return nil, fmt.Errorf("peering.host.port: wrong port")
	}
	pkStr := viper.GetString("peering.host.id_private_key")
	pkBin, err := hex.DecodeString(pkStr)
	if err != nil {
		return nil, fmt.Errorf("host.id_private_key: wrong id private key: %v", err)
	}
	if len(pkBin) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("host.private_key: wrong host id private key size")
	}
	cfg.HostIDPrivateKey = pkBin

	encodedHostID := viper.GetString("peering.host.id")
	cfg.HostID, err = peer.Decode(encodedHostID)
	if err != nil {
		return nil, fmt.Errorf("can't decode host ID: %v", err)
	}
	privKey, err := p2pcrypto.UnmarshalEd25519PrivateKey(cfg.HostIDPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("UnmarshalEd25519PrivateKey: %v", err)
	}

	if !cfg.HostID.MatchesPrivateKey(privKey) {
		return nil, fmt.Errorf("config: host private key does not match hostID")
	}

	peerNames := util.KeysSorted(viper.GetStringMap("peering.peers"), func(k1, k2 string) bool {
		return k1 < k2
	})

	if !boot && len(peerNames) == 0 {
		return nil, fmt.Errorf("at least one peer must be pre-configured for bootstrap")
	}
	for _, peerName := range peerNames {
		addrString := viper.GetString("peering.peers." + peerName)
		if cfg.PreConfiguredPeers[peerName], err = multiaddr.NewMultiaddr(addrString); err != nil {
			return nil, fmt.Errorf("can't parse multiaddress: %w", err)
		}
	}

	cfg.MaxDynamicPeers = viper.GetInt("peering.max_dynamic_peers")
	if cfg.MaxDynamicPeers < 0 {
		cfg.MaxDynamicPeers = 0
	}
	if boot && cfg.MaxDynamicPeers < numMaxDynamicPeersForBootNodeAtLeast {
		cfg.MaxDynamicPeers = numMaxDynamicPeersForBootNodeAtLeast
	}
	return cfg, nil
}

func NewPeersFromConfig(env Environment) (*Peers, error) {
	cfg, err := readPeeringConfig(env.IsBootstrapNode())
	if err != nil {
		return nil, err
	}

	return New(env, cfg)
}

func (ps *Peers) SelfID() peer.ID {
	return ps.host.ID()
}

func (ps *Peers) Run() {
	ps.Environment.MarkWorkProcessStarted(Name)

	ps.host.SetStreamHandler(ps.lppProtocolGossip, ps.gossipStreamHandler)
	ps.host.SetStreamHandler(ps.lppProtocolPull, ps.pullStreamHandler)
	ps.host.SetStreamHandler(ps.lppProtocolHeartbeat, ps.heartbeatStreamHandler)

	go ps.heartbeatLoop()
	if ps.isAutopeeringEnabled() {
		go ps.autopeeringLoop()
	}

	ps.Log().Infof("[peering] libp2p host %s (self) started on %v with %d pre-configured peers, maximum dynamic peers: %d, autopeering enabled: %v",
		ShortPeerIDString(ps.host.ID()), ps.host.Addrs(), len(ps.cfg.PreConfiguredPeers), ps.cfg.MaxDynamicPeers, ps.isAutopeeringEnabled())
	_ = ps.Log().Sync()
}

func (ps *Peers) isAutopeeringEnabled() bool {
	return ps.cfg.MaxDynamicPeers > 0
}

func (ps *Peers) Stop() {
	ps.stopOnce.Do(func() {
		ps.Environment.MarkWorkProcessStopped(Name)

		ps.Log().Infof("[peering] stopping libp2p host %s (self)..", ShortPeerIDString(ps.host.ID()))
		_ = ps.Log().Sync()
		_ = ps.host.Close()
		ps.Log().Infof("[peering] libp2p host %s (self) has been stopped", ShortPeerIDString(ps.host.ID()))
	})
}

// addStaticPeer adds preconfigured peer to the list. It will never be deleted
func (ps *Peers) addStaticPeer(maddr multiaddr.Multiaddr, name string) error {
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("can't get multiaddress info: %v", err)
	}

	ps.addPeer(info, name, true)
	return nil
}

func (ps *Peers) addPeer(addrInfo *peer.AddrInfo, name string, static bool) {
	ps.withPeer(addrInfo.ID, func(p *Peer) {
		if p != nil {
			return
		}
		ps._addPeer(addrInfo, name, static)
	})
}

func (ps *Peers) _addPeer(addrInfo *peer.AddrInfo, name string, static bool) {
	p := &Peer{
		id:        addrInfo.ID,
		name:      name,
		isStatic:  static,
		whenAdded: time.Now(),
	}
	ps.peers[addrInfo.ID] = p
	for _, a := range addrInfo.Addrs {
		ps.host.Peerstore().AddAddr(addrInfo.ID, a, peerstore.PermanentAddrTTL)
	}
	ps.Log().Infof("[peering] added %s peer %s (name='%s')", p.staticOrDynamic(), ShortPeerIDString(addrInfo.ID), name)
}

// dropPeer removes dynamic peer and blacklists for 1 min. Ignores otherwise
func (ps *Peers) dropPeer(id peer.ID, reason ...string) {
	ps.withPeer(id, func(p *Peer) {
		if p != nil && !p.isStatic {
			// ignore static
			ps._dropPeer(p, reason...)
		}
	})
}

func (ps *Peers) _dropPeer(p *Peer, reason ...string) {
	util.Assertf(p != nil, "removeDynamicPeer: p!=nil")
	util.Assertf(!p.isStatic, "removeDynamicPeer: must not be pre-configured")

	ps.host.Peerstore().RemovePeer(p.id)
	ps.kademliaDHT.RoutingTable().RemovePeer(p.id)
	_ = ps.host.Network().ClosePeer(p.id)
	delete(ps.peers, p.id)

	ps._addToBlacklist(p.id, time.Minute)

	why := ""
	if len(reason) > 0 {
		why = fmt.Sprintf(". Reason: '%s'", reason[0])
	}
	ps.Log().Infof("[peering] dropped dynamic peer %s - %s%s", ShortPeerIDString(p.id), p.name, why)
}

func (ps *Peers) _addToBlacklist(id peer.ID, ttl time.Duration) {
	ps.blacklist[id] = time.Now().Add(ttl)
}

func (ps *Peers) isInBlacklist(id peer.ID) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	_, yes := ps.blacklist[id]
	return yes
}

func (ps *Peers) OnReceiveTxBytes(fun func(from peer.ID, txBytes []byte, metadata *txmetadata.TransactionMetadata)) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.onReceiveTx = fun
}

func (ps *Peers) OnReceivePullTxRequest(fun func(from peer.ID, txids []ledger.TransactionID)) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.onReceivePullTx = fun
}

func (ps *Peers) OnReceivePullSyncPortion(fun func(from peer.ID, startingFrom ledger.Slot, maxSlots int)) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.onReceivePullSyncPortion = fun
}

func (ps *Peers) _getPeer(id peer.ID) *Peer {
	if ret, ok := ps.peers[id]; ok {
		return ret
	}
	return nil
}

func (ps *Peers) getPeer(id peer.ID) *Peer {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	return ps._getPeer(id)
}

func (ps *Peers) withPeer(id peer.ID, fun func(p *Peer)) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	fun(ps._getPeer(id))
}

func (ps *Peers) forEachPeer(fun func(p *Peer) bool) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for _, p := range ps.peers {
		if !fun(p) {
			return
		}
	}
}

func (ps *Peers) getPeerIDs() []peer.ID {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	return maps.Keys(ps.peers)
}

func (ps *Peers) PeerName(id peer.ID) string {
	p := ps.getPeer(id)
	if p == nil {
		return "(unknown peer)"
	}
	return p.name
}

func (ps *Peers) EvidenceIncomingTx(good bool, from peer.ID) {
	// TODO metrics
}

func (ps *Peers) blacklistCleanupLoop() {
	for {
		select {
		case <-ps.Ctx().Done():
			return
		case <-time.After(time.Second):
			ps.cleanBlacklist()
		}
	}
}

func (ps *Peers) cleanBlacklist() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	toDelete := make([]peer.ID, 0, len(ps.blacklist))
	nowis := time.Now()
	for id, deadline := range ps.blacklist {
		if deadline.Before(nowis) {
			toDelete = append(toDelete, id)
		}
	}
	for _, id := range toDelete {
		delete(ps.blacklist, id)
	}
}

func (ps *Peers) _isDead(id peer.ID) bool {
	p := ps._getPeer(id)
	util.Assertf(p != nil, "_isDead: peer %s bot found", func() string { return ShortPeerIDString(id) })
	return p._isDead()
}

func (p *Peer) _isDead() bool {
	return !p._isAlive() && time.Since(p.whenAdded) > gracePeriodAfterAdded
}

func (ps *Peers) IsAlive(id peer.ID) (isAlive bool) {
	ps.withPeer(id, func(p *Peer) {
		isAlive = p._isAlive()
	})
	return
}

func (p *Peer) _isAlive() bool {
	return time.Since(p.lastMsgReceived) < aliveDuration
}

func (p *Peer) staticOrDynamic() string {
	if p.isStatic {
		return "static"
	}
	return "dynamic"
}

func (ps *Peers) HasTxStore(id peer.ID) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	p := ps._getPeer(id)
	util.Assertf(p != nil, "HasTxStore: peer %s not found", func() string { return ShortPeerIDString(id) })
	return p.hasTxStore
}

func (p *Peer) _evidenceActivity(src string) {
	p.lastMsgReceived = time.Now()
	p.lastMsgReceivedFrom = src
}

func (p *Peer) _evidenceClockDifference(diff time.Duration) {
	p.clockDifferences[p.clockDifferencesIdx] = diff
	p.clockDifferencesIdx = (p.clockDifferencesIdx + 1) % len(p.clockDifferences)
}

// avgClockDifference calculates average over lates clock differences
func (p *Peer) avgClockDifference() time.Duration {
	var ret time.Duration

	nNonZero := 0
	for _, d := range p.clockDifferences {
		ret += d
		if d != 0 {
			nNonZero++
		}
	}
	if nNonZero == 0 {
		return 0
	}
	return ret / time.Duration(nNonZero)
}
