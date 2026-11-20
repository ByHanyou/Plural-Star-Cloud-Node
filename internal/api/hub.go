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
	sessions map[string]*wsClient
	reserved int
	closed   bool
}

func NewHub() *Hub {
	return &Hub{
		clients:  make(map[*wsClient]struct{}),
		sessions: make(map[string]*wsClient),
	}
}

func (h *Hub) ownsSession(session string, appPeer peer.ID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	client := h.sessions[session]
	return client != nil && client.appPeer == appPeer
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
	if h.closed || (max > 0 && len(h.clients)+h.reserved >= max) {
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

func (h *Hub) add(c *wsClient) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reserved > 0 {
		h.reserved--
	}
	if h.closed {
		return false
	}
	h.clients[c] = struct{}{}
	h.sessions[c.session] = c
	return true
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	delete(h.sessions, c.session)
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

func (h *Hub) closeAll() {
	h.mu.Lock()
	h.closed = true
	for c := range h.clients {
		c.close()
		_ = c.conn.Close()
	}
	clear(h.clients)
	clear(h.sessions)
	h.mu.Unlock()
}
