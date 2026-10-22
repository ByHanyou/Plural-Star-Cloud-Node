// SPDX-License-Identifier: AGPL-3.0-or-later

package ttlmap

import (
	"context"
	"sync"
	"time"
)

type entry[V any] struct {
	val       V
	expiresAt time.Time
}

type Map[K comparable, V any] struct {
	mu       sync.RWMutex
	m        map[K]entry[V]
	onExpire func(K, V)
}

type UpdateFunc[V any] func(cur V, ok bool, expiresAt time.Time, size int) (next V, ttl time.Duration, write bool)

func New[K comparable, V any](ctx context.Context, pruneEvery time.Duration, onExpire func(K, V)) *Map[K, V] {
	t := &Map[K, V]{m: make(map[K]entry[V]), onExpire: onExpire}
	if pruneEvery > 0 {
		go t.pruneLoop(ctx, pruneEvery)
	}
	return t
}

func (t *Map[K, V]) Put(k K, v V, ttl time.Duration) {
	t.mu.Lock()
	t.m[k] = entry[V]{val: v, expiresAt: time.Now().Add(ttl)}
	t.mu.Unlock()
}

func (t *Map[K, V]) PutIfAbsent(k K, v V, ttl time.Duration) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.m[k]; ok && now.Before(e.expiresAt) {
		return false
	}
	t.m[k] = entry[V]{val: v, expiresAt: now.Add(ttl)}
	return true
}

func (t *Map[K, V]) Update(k K, fn UpdateFunc[V]) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[k]
	if ok && !now.Before(e.expiresAt) {
		e = entry[V]{}
		ok = false
	}
	next, ttl, write := fn(e.val, ok, e.expiresAt, len(t.m))
	if !write {
		return false
	}
	t.m[k] = entry[V]{val: next, expiresAt: now.Add(ttl)}
	return true
}

func (t *Map[K, V]) Get(k K) (V, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.m[k]
	if !ok || !time.Now().Before(e.expiresAt) {
		var zero V
		return zero, false
	}
	return e.val, true
}

func (t *Map[K, V]) Remove(k K) {
	t.mu.Lock()
	delete(t.m, k)
	t.mu.Unlock()
}

func (t *Map[K, V]) Len() int {
	now := time.Now()
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	for _, e := range t.m {
		if now.Before(e.expiresAt) {
			n++
		}
	}
	return n
}

func (t *Map[K, V]) Each(fn func(k K, v V, expiresAt time.Time)) {
	now := time.Now()
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, e := range t.m {
		if now.Before(e.expiresAt) {
			fn(k, e.val, e.expiresAt)
		}
	}
}

func (t *Map[K, V]) Prune() int {
	now := time.Now()
	type expired struct {
		k K
		v V
	}
	var gone []expired
	removed := 0
	t.mu.Lock()
	for k, e := range t.m {
		if now.Before(e.expiresAt) {
			continue
		}
		delete(t.m, k)
		removed++
		if t.onExpire != nil {
			gone = append(gone, expired{k: k, v: e.val})
		}
	}
	t.mu.Unlock()
	for _, x := range gone {
		t.onExpire(x.k, x.v)
	}
	return removed
}

func (t *Map[K, V]) pruneLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.Prune()
		}
	}
}
