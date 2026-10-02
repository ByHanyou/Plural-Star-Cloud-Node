// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type RoutingEntry struct {
	ViaNode   peer.ID
	ExpiresAt time.Time
}

type Router struct {
	mu    sync.RWMutex
	table map[peer.ID]RoutingEntry
}

func NewRouter(ctx context.Context, pruneInterval time.Duration) *Router {
	r := &Router{table: make(map[peer.ID]RoutingEntry)}
	go r.pruneLoop(ctx, pruneInterval)
	return r
}

func (r *Router) Upsert(target, via peer.ID, ttl time.Duration) {
	r.mu.Lock()
	r.table[target] = RoutingEntry{ViaNode: via, ExpiresAt: time.Now().Add(ttl)}
	r.mu.Unlock()
}

func (r *Router) Remove(target peer.ID) {
	r.mu.Lock()
	delete(r.table, target)
	r.mu.Unlock()
}

func (r *Router) Lookup(target peer.ID) (peer.ID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.table[target]
	if !ok || time.Now().After(e.ExpiresAt) {
		return "", false
	}
	return e.ViaNode, true
}

func (r *Router) Online() []peer.ID {
	now := time.Now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]peer.ID, 0, len(r.table))
	for id, e := range r.table {
		if now.Before(e.ExpiresAt) {
			out = append(out, id)
		}
	}
	return out
}

func (r *Router) pruneLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.prune()
		}
	}
}

func (r *Router) prune() {
	now := time.Now()
	r.mu.Lock()
	for id, e := range r.table {
		if now.After(e.ExpiresAt) {
			delete(r.table, id)
		}
	}
	r.mu.Unlock()
}
