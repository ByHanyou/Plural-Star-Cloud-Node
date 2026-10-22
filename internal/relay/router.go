// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ttlmap"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Router struct {
	table *ttlmap.Map[peer.ID, peer.ID]
}

func NewRouter(ctx context.Context, pruneInterval time.Duration) *Router {
	return &Router{table: ttlmap.New[peer.ID, peer.ID](ctx, pruneInterval, nil)}
}

func (r *Router) Upsert(target, via peer.ID, ttl time.Duration) {
	r.table.Put(target, via, ttl)
}

func (r *Router) Remove(target peer.ID) {
	r.table.Remove(target)
}

func (r *Router) Lookup(target peer.ID) (peer.ID, bool) {
	return r.table.Get(target)
}

type PeerRoute struct {
	Peer peer.ID
	Via  peer.ID
}

func (r *Router) Snapshot() []PeerRoute {
	out := make([]PeerRoute, 0, r.table.Len())
	r.table.Each(func(id, via peer.ID, _ time.Time) {
		out = append(out, PeerRoute{Peer: id, Via: via})
	})
	return out
}
