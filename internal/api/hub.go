// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Hub struct {
	mu       sync.RWMutex
	clients  map[*wsClient]struct{}
	reserved int
}

func NewHub() *Hub {
	return &Hub{clients: make(map[*wsClient]struct{})}
}

func (h *Hub) OnAppPeerEvent(peerID, viaNode peer.ID, online bool) {
	if online {
		h.broadcast(peerOnlineEvent{Type: "peer_online", PeerID: peerID.String(), ViaNode: viaNode.String()})
	} else {
		h.broadcast(peerOfflineEvent{Type: "peer_offline", PeerID: peerID.String()})
	}
}

func (h *Hub) OnNodeEvent(peerID peer.ID, rttMs int64, connected bool) {
	if connected {
		h.broadcast(nodeConnectedEvent{Type: "node_connected", NodePeerID: peerID.String(), RTTms: rttMs})
	} else {
		h.broadcast(nodeDisconnectedEvent{Type: "node_disconnected", NodePeerID: peerID.String()})
	}
}

func (h *Hub) broadcast(ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	h.mu.RLock()
	for c := range h.clients {
		c.trySend(b)
	}
	h.mu.RUnlock()
}

func (h *Hub) sendToApp(appPeer peer.ID, ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	h.mu.RLock()
	for c := range h.clients {
		if c.appPeer == appPeer {
			c.trySend(b)
		}
	}
	h.mu.RUnlock()
}

func (h *Hub) reserve(max int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if max > 0 && len(h.clients)+h.reserved >= max {
		return false
	}
	h.reserved++
	return true
}

func (h *Hub) release() {
	h.mu.Lock()
	if h.reserved > 0 {
		h.reserved--
	}
	h.mu.Unlock()
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	if h.reserved > 0 {
		h.reserved--
	}
	h.mu.Unlock()
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *Hub) hasClientFor(appPeer peer.ID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if c.appPeer == appPeer {
			return true
		}
	}
	return false
}

func (h *Hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) appPeers() []peer.ID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[peer.ID]struct{}, len(h.clients))
	out := make([]peer.ID, 0, len(h.clients))
	for c := range h.clients {
		if _, dup := seen[c.appPeer]; dup {
			continue
		}
		seen[c.appPeer] = struct{}{}
		out = append(out, c.appPeer)
	}
	return out
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	for c := range h.clients {
		c.close()
	}
	h.clients = make(map[*wsClient]struct{})
	h.mu.Unlock()
}
