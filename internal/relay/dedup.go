// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ttlmap"
)

type DedupCache struct {
	seen *ttlmap.Map[[16]byte, struct{}]
	ttl  time.Duration
}

func NewDedupCache(ctx context.Context, ttl, evictInterval time.Duration) *DedupCache {
	return &DedupCache{seen: ttlmap.New[[16]byte, struct{}](ctx, evictInterval, nil), ttl: ttl}
}

func (d *DedupCache) SeenOrAdd(id [16]byte) bool {
	return !d.seen.PutIfAbsent(id, struct{}{}, d.ttl)
}

func (d *DedupCache) Len() int {
	return d.seen.Len()
}
