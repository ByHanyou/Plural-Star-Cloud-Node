// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"sync"
	"time"
)

const (
	limitPerAddress = 10
	windowAddress   = time.Minute
	limitPerVault   = 20
	windowVault     = time.Hour
)

type rateLimiter struct {
	mu      sync.Mutex
	byAddr  map[string][]time.Time
	byVault map[string][]time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{
		byAddr:  make(map[string][]time.Time),
		byVault: make(map[string][]time.Time),
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
	if len(a) >= limitPerAddress {
		return false, a[0].Add(windowAddress).Sub(now)
	}
	if vault != "" {
		v := prune(l.byVault[vault], now.Add(-windowVault))
		l.byVault[vault] = v
		if len(v) >= limitPerVault {
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
			now := time.Now()
			l.mu.Lock()
			for k, ev := range l.byAddr {
				ev = prune(ev, now.Add(-windowAddress))
				if len(ev) == 0 {
					delete(l.byAddr, k)
				} else {
					l.byAddr[k] = ev
				}
			}
			for k, ev := range l.byVault {
				ev = prune(ev, now.Add(-windowVault))
				if len(ev) == 0 {
					delete(l.byVault, k)
				} else {
					l.byVault[k] = ev
				}
			}
			l.mu.Unlock()
		}
	}
}
