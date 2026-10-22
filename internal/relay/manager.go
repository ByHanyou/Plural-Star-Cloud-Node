// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	msgio "github.com/libp2p/go-msgio"
)

type Outcome string

const (
	OutcomeDelivered Outcome = "delivered"
	OutcomeForwarded Outcome = "forwarded"
	OutcomeHeld      Outcome = "held"
	OutcomeDuplicate Outcome = "duplicate"
)

type DeliverFunc func(*Packet)

type OfflineFunc func(recipient peer.ID, p *Packet) bool

type OfflineReplayFunc func(recipient peer.ID, deliver func(*Packet) bool) int

type Manager struct {
	lim      config.Limits
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

func NewManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, gossipPrefix string, lim config.Limits, onPeer PeerEvent) (*Manager, error) {
	router := NewRouter(ctx, lim.RoutingPruneInterval.Std())
	dedup := NewDedupCache(ctx, lim.DedupCacheTTL.Std(), lim.DedupEvictInterval.Std())
	queue := NewQueue(ctx, QueueLimits{
		EntryTTL:      lim.QueueEntryTTL.Std(),
		MaxSenders:    lim.QueueMaxSenders,
		MaxRecipients: lim.QueueMaxRecipients,
		MaxBytes:      int64(lim.QueueMaxBytes),
		SweepInterval: lim.QueueSweepInterval.Std(),
	})
	m := &Manager{
		lim:        lim,
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
	presence, err := NewPresence(ctx, ps, h.ID(), gossipPrefix+"presence", router, lim.PresenceTTL.Std(), lim.MaxPresenceTTL.Std(), wrapped)
	if err != nil {
		return nil, err
	}
	m.presence = presence
	h.SetStreamHandler(protocol.ID(RelayProtocol), m.handleStream)
	return m, nil
}

func (m *Manager) Router() *Router { return m.router }

func (m *Manager) SetOffline(f OfflineFunc) {
	m.offlineMu.Lock()
	m.offline = f
	m.offlineMu.Unlock()
}

func (m *Manager) SetOfflineReplay(f OfflineReplayFunc) {
	m.offlineMu.Lock()
	m.replay = f
	m.offlineMu.Unlock()
}

func (m *Manager) hold(recipient peer.ID, p *Packet) {
	m.offlineMu.RLock()
	f := m.offline
	m.offlineMu.RUnlock()
	if f != nil && f(recipient, p) {
		return
	}
	if !m.queue.Put(recipient, p) {
		slog.Warn("relay: packet dropped, hold queue full", "recipient", recipient)
	}
}

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
		m.forwardOrHold(recipient, via, p)
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
		slog.Warn("relay: announce presence failed", "peer", appPeer, "err", err)
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
		slog.Warn("relay: tombstone presence failed", "peer", appPeer, "err", err)
	}
}

func (m *Manager) refreshLoop(ctx context.Context, appPeer peer.ID) {
	ticker := time.NewTicker(m.lim.PresenceRefreshInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.presence.Announce(appPeer); err != nil {
				slog.Warn("relay: refresh presence failed", "peer", appPeer, "err", err)
			}
		}
	}
}

func (m *Manager) Route(p *Packet) (Outcome, error) {
	if m.dedup.SeenOrAdd(p.ID) {
		return OutcomeDuplicate, nil
	}
	return m.forwardOrDeliver(p)
}

func (m *Manager) forwardOrDeliver(p *Packet) (Outcome, error) {
	recipient, err := peer.IDFromBytes(p.RecipientID)
	if err != nil {
		return "", fmt.Errorf("invalid recipient id: %w", err)
	}

	m.mu.RLock()
	deliver, isLocal := m.localApps[recipient]
	m.mu.RUnlock()
	if isLocal {
		deliver(p)
		return OutcomeDelivered, nil
	}

	via, ok := m.router.Lookup(recipient)
	if !ok || via == m.self {
		m.hold(recipient, p)
		return OutcomeHeld, nil
	}
	if !m.forwardOrHold(recipient, via, p) {
		return OutcomeHeld, nil
	}
	return OutcomeForwarded, nil
}

func (m *Manager) forwardOrHold(recipient, via peer.ID, p *Packet) bool {
	if err := m.forwardTo(via, p); err != nil {
		slog.Debug("relay: forward failed, holding packet", "recipient", recipient, "via", via, "err", err)
		m.hold(recipient, p)
		return false
	}
	return true
}

func (m *Manager) forwardTo(via peer.ID, p *Packet) error {
	b, err := p.Marshal()
	if err != nil {
		return fmt.Errorf("marshal packet: %w", err)
	}
	streamCtx, cancel := context.WithTimeout(m.ctx, m.lim.RelayStreamTimeout.Std())
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
	r := msgio.NewReaderSize(s, m.lim.MaxPacketBytes)
	b, err := r.ReadMsg()
	if err != nil {
		slog.Debug("relay: reading packet failed", "peer", s.Conn().RemotePeer(), "err", err)
		_ = s.Reset()
		return
	}
	p, err := UnmarshalPacket(b)
	r.ReleaseMsg(b)
	if err != nil {
		slog.Debug("relay: malformed packet", "peer", s.Conn().RemotePeer(), "err", err)
		_ = s.Reset()
		return
	}
	if _, err := m.Route(p); err != nil {
		slog.Warn("relay: route packet failed", "err", err)
	}
}
