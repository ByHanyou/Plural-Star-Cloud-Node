// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerRoute struct {
	Peer peer.ID
	Via  peer.ID
}

func (r *Router) Snapshot() []PeerRoute {
	now := time.Now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PeerRoute, 0, len(r.table))
	for id, e := range r.table {
		if now.Before(e.ExpiresAt) {
			out = append(out, PeerRoute{Peer: id, Via: e.ViaNode})
		}
	}
	return out
}
