// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"sync"
	"time"
)

type DedupCache struct {
	mu  sync.Mutex
	m   map[[16]byte]time.Time
	ttl time.Duration
}

func NewDedupCache(ctx context.Context, ttl, evictInterval time.Duration) *DedupCache {
	d := &DedupCache{
		m:   make(map[[16]byte]time.Time),
		ttl: ttl,
	}
	go d.evictLoop(ctx, evictInterval)
	return d
}

func (d *DedupCache) SeenOrAdd(id [16]byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.m[id]; ok {
		return true
	}
	d.m[id] = time.Now()
	return false
}

func (d *DedupCache) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.m)
}

func (d *DedupCache) evictLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.evict()
		}
	}
}

func (d *DedupCache) evict() {
	now := time.Now()
	d.mu.Lock()
	for id, at := range d.m {
		if now.Sub(at) > d.ttl {
			delete(d.m, id)
		}
	}
	d.mu.Unlock()
}
