// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"sync"
	"time"
)

const (
	windowAddress = time.Minute
	windowVault   = time.Hour
)

type rateLimiter struct {
	perAddress int
	perVault   int
	mu         sync.Mutex
	byAddr     map[string][]time.Time
	byVault    map[string][]time.Time
}

func newRateLimiter(perAddress, perVault int) *rateLimiter {
	return &rateLimiter{
		perAddress: perAddress,
		perVault:   perVault,
		byAddr:     make(map[string][]time.Time),
		byVault:    make(map[string][]time.Time),
	}
}

func prune(events []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(events) && events[i].Before(cutoff) {
		i++
	}
	return events[i:]
}

func (l *rateLimiter) check(addr, vault string) (ok bool, retryAfter time.Duration) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	a := prune(l.byAddr[addr], now.Add(-windowAddress))
	l.byAddr[addr] = a
	if len(a) >= l.perAddress {
		return false, a[0].Add(windowAddress).Sub(now)
	}
	if vault != "" {
		v := prune(l.byVault[vault], now.Add(-windowVault))
		l.byVault[vault] = v
		if len(v) >= l.perVault {
			return false, v[0].Add(windowVault).Sub(now)
		}
	}
	return true, 0
}

func (l *rateLimiter) record(addr, vault string) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.byAddr[addr] = append(prune(l.byAddr[addr], now.Add(-windowAddress)), now)
	if vault != "" {
		l.byVault[vault] = append(prune(l.byVault[vault], now.Add(-windowVault)), now)
	}
}

func (l *rateLimiter) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.sweep(time.Now())
		}
	}
}

func (l *rateLimiter) sweep(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sweepEvents(l.byAddr, now.Add(-windowAddress))
	sweepEvents(l.byVault, now.Add(-windowVault))
}

func sweepEvents(m map[string][]time.Time, cutoff time.Time) {
	for k, ev := range m {
		if ev = prune(ev, cutoff); len(ev) == 0 {
			delete(m, k)
		} else {
			m[k] = ev
		}
	}
}
