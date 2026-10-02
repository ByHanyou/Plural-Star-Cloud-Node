// SPDX-License-Identifier: AGPL-3.0-or-later

package ping

import (
	"context"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"

	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	pingsvc "github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type NodeInfo struct {
	PeerID     string   `json:"peer_id"`
	Multiaddr  string   `json:"multiaddr"`
	RTTms      int64    `json:"rtt_ms"`
	Roles      []string `json:"roles"`
	PushBoxPub string   `json:"push_box_pub,omitempty"`
}

type EventFunc func(peerID peer.ID, rttMs int64, connected bool)

type Manager struct {
	ctx     context.Context
	h       host.Host
	svc     *pingsvc.PingService
	onEvent EventFunc

	mu   sync.RWMutex
	rtts map[peer.ID]time.Duration
}

func NewManager(ctx context.Context, h host.Host, onEvent EventFunc) *Manager {
	m := &Manager{
		ctx:     ctx,
		h:       h,
		svc:     pingsvc.NewPingService(h),
		onEvent: onEvent,
		rtts:    make(map[peer.ID]time.Duration),
	}
	h.Network().Notify(&corenet.NotifyBundle{
		ConnectedF: func(_ corenet.Network, c corenet.Conn) {
			go m.measure(c.RemotePeer())
		},
		DisconnectedF: func(n corenet.Network, c corenet.Conn) {
			p := c.RemotePeer()
			if len(n.ConnsToPeer(p)) > 0 {
				return
			}
			m.mu.Lock()
			delete(m.rtts, p)
			m.mu.Unlock()
			if m.onEvent != nil {
				m.onEvent(p, 0, false)
			}
		},
	})
	return m
}

func (m *Manager) measure(p peer.ID) {
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()
	select {
	case res := <-m.svc.Ping(ctx, p):
		if res.Error != nil {
			return
		}
		m.mu.Lock()
		m.rtts[p] = res.RTT
		m.mu.Unlock()
		if m.onEvent != nil {
			m.onEvent(p, res.RTT.Milliseconds(), true)
		}
	case <-ctx.Done():
	}
}

func (m *Manager) Nodes() []NodeInfo {
	peers := m.h.Network().Peers()
	out := make([]NodeInfo, 0, len(peers))
	for _, p := range peers {
		conns := m.h.Network().ConnsToPeer(p)
		if len(conns) == 0 {
			continue
		}
		m.mu.RLock()
		rtt := m.rtts[p]
		m.mu.RUnlock()
		// Roles and the push box key arrive through identify as the peer's
		// AgentVersion. A peer that predates the cloud role, or whose identify
		// has not completed yet, parses to the plain relay role.
		agent := ""
		if v, err := m.h.Peerstore().Get(p, "AgentVersion"); err == nil {
			if s, ok := v.(string); ok {
				agent = s
			}
		}
		roles, pushPub := cloud.ParseAgent(agent)
		out = append(out, NodeInfo{
			PeerID:     p.String(),
			Multiaddr:  conns[0].RemoteMultiaddr().String(),
			RTTms:      rtt.Milliseconds(),
			Roles:      roles,
			PushBoxPub: pushPub,
		})
	}
	return out
}

func (m *Manager) Count() int {
	return len(m.h.Network().Peers())
}
