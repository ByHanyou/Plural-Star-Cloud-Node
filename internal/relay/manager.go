// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	msgio "github.com/libp2p/go-msgio"
)

var ErrNoRoute = errors.New("recipient not found in routing table")

type DeliverFunc func(*Packet)

// OfflineFunc holds a packet for a recipient that cannot be reached right now.
// It returns true when it accepted the packet; false leaves the packet with the
// in-memory queue, so a relay without a persistent store behaves exactly as it
// always has.
type OfflineFunc func(recipient peer.ID, p *Packet) bool

// OfflineReplayFunc hands every packet parked for recipient to deliver, oldest
// first, dropping each one deliver accepts, and reports how many it delivered.
type OfflineReplayFunc func(recipient peer.ID, deliver func(*Packet) bool) int

type Manager struct {
	ctx      context.Context
	h        host.Host
	self     peer.ID
	router   *Router
	dedup    *DedupCache
	presence *Presence

	queue *Queue

	offlineMu sync.RWMutex
	offline   OfflineFunc
	replay    OfflineReplayFunc

	mu         sync.RWMutex
	localApps  map[peer.ID]DeliverFunc
	refreshers map[peer.ID]context.CancelFunc
}

func NewManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, gossipPrefix string, onPeer PeerEvent) (*Manager, error) {
	router := NewRouter(ctx, RoutingTablePruneTicker)
	dedup := NewDedupCache(ctx, DedupCacheTTL, DedupCacheEvictInterval)
	queue := NewQueue(ctx)
	m := &Manager{
		ctx:        ctx,
		h:          h,
		self:       h.ID(),
		router:     router,
		dedup:      dedup,
		queue:      queue,
		localApps:  make(map[peer.ID]DeliverFunc),
		refreshers: make(map[peer.ID]context.CancelFunc),
	}
	wrapped := func(peerID, viaNode peer.ID, online bool) {
		if online {
			m.FlushQueued(peerID)
		}
		if onPeer != nil {
			onPeer(peerID, viaNode, online)
		}
	}
	presence, err := NewPresence(ctx, ps, h.ID(), gossipPrefix+"presence", router, PresenceTTL, wrapped)
	if err != nil {
		return nil, err
	}
	m.presence = presence
	h.SetStreamHandler(protocol.ID(RelayProtocol), m.handleStream)
	return m, nil
}

func (m *Manager) Router() *Router { return m.router }

// SetOffline installs the persistent store for undeliverable packets. A nil
// function restores the in-memory queue as the only holding place.
func (m *Manager) SetOffline(f OfflineFunc) {
	m.offlineMu.Lock()
	m.offline = f
	m.offlineMu.Unlock()
}

// SetOfflineReplay installs the drain for the persistent store, used when a
// recipient comes online through another node. Without it packets parked on
// disk waited for the app to connect to this node in particular.
func (m *Manager) SetOfflineReplay(f OfflineReplayFunc) {
	m.offlineMu.Lock()
	m.replay = f
	m.offlineMu.Unlock()
}

// hold parks a packet that could not be delivered or forwarded. The persistent
// store gets first refusal; the in-memory queue takes whatever it declines.
func (m *Manager) hold(recipient peer.ID, p *Packet) {
	m.offlineMu.RLock()
	f := m.offline
	m.offlineMu.RUnlock()
	if f != nil && f(recipient, p) {
		return
	}
	m.queue.Put(recipient, p)
}

// FlushQueued attempts delivery of everything held for recipient. Packets that
// still cannot be delivered are put back, so nothing is lost by a failed flush.
func (m *Manager) FlushQueued(recipient peer.ID) {
	pending := m.queue.Take(recipient)
	m.mu.RLock()
	deliver, isLocal := m.localApps[recipient]
	m.mu.RUnlock()
	if isLocal {
		for _, p := range pending {
			deliver(p)
		}
		return
	}
	via, ok := m.router.Lookup(recipient)
	if !ok || via == m.self {
		for _, p := range pending {
			m.hold(recipient, p)
		}
		return
	}
	for _, p := range pending {
		if err := m.forwardTo(via, p); err != nil {
			m.hold(recipient, p)
		}
	}
	m.offlineMu.RLock()
	replay := m.replay
	m.offlineMu.RUnlock()
	if replay != nil {
		replay(recipient, func(p *Packet) bool { return m.forwardTo(via, p) == nil })
	}
}

func (m *Manager) AppConnected(appPeer peer.ID, deliver DeliverFunc) {
	rctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	if old, ok := m.refreshers[appPeer]; ok {
		old()
	}
	m.localApps[appPeer] = deliver
	m.refreshers[appPeer] = cancel
	m.mu.Unlock()

	if err := m.presence.Announce(appPeer); err != nil {
		log.Printf("relay: announce presence for %s: %v", appPeer, err)
	}
	m.FlushQueued(appPeer)
	go m.refreshLoop(rctx, appPeer)
}

func (m *Manager) AppDisconnected(appPeer peer.ID) {
	m.mu.Lock()
	delete(m.localApps, appPeer)
	if cancel, ok := m.refreshers[appPeer]; ok {
		cancel()
		delete(m.refreshers, appPeer)
	}
	m.mu.Unlock()

	if err := m.presence.Tombstone(appPeer); err != nil {
		log.Printf("relay: tombstone presence for %s: %v", appPeer, err)
	}
}

func (m *Manager) refreshLoop(ctx context.Context, appPeer peer.ID) {
	ticker := time.NewTicker(PresenceRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.presence.Announce(appPeer); err != nil {
				log.Printf("relay: refresh presence for %s: %v", appPeer, err)
			}
		}
	}
}

func (m *Manager) Route(p *Packet) error {
	if m.dedup.SeenOrAdd(p.ID) {
		return nil
	}
	return m.forwardOrDeliver(p)
}

func (m *Manager) forwardOrDeliver(p *Packet) error {
	recipient, err := peer.IDFromBytes(p.RecipientID)
	if err != nil {
		return fmt.Errorf("invalid recipient id: %w", err)
	}

	m.mu.RLock()
	deliver, isLocal := m.localApps[recipient]
	m.mu.RUnlock()
	if isLocal {
		deliver(p)
		return nil
	}

	via, ok := m.router.Lookup(recipient)
	if !ok || via == m.self {
		m.hold(recipient, p)
		return nil
	}
	if err := m.forwardTo(via, p); err != nil {
		m.hold(recipient, p)
		return nil
	}
	return nil
}

func (m *Manager) forwardTo(via peer.ID, p *Packet) error {
	b, err := p.Marshal()
	if err != nil {
		return fmt.Errorf("marshal packet: %w", err)
	}
	streamCtx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	s, err := m.h.NewStream(streamCtx, via, protocol.ID(RelayProtocol))
	if err != nil {
		return fmt.Errorf("open relay stream to %s: %w", via, err)
	}
	defer s.Close()
	w := msgio.NewWriter(s)
	if err := w.WriteMsg(b); err != nil {
		_ = s.Reset()
		return fmt.Errorf("write relay packet to %s: %w", via, err)
	}
	return nil
}

func (m *Manager) handleStream(s corenet.Stream) {
	defer s.Close()
	r := msgio.NewReaderSize(s, MaxPacketBytes)
	b, err := r.ReadMsg()
	if err != nil {
		_ = s.Reset()
		return
	}
	p, err := UnmarshalPacket(b)
	r.ReleaseMsg(b)
	if err != nil {
		_ = s.Reset()
		return
	}
	if err := m.Route(p); err != nil && !errors.Is(err, ErrNoRoute) {
		log.Printf("relay: route packet: %v", err)
	}
}
