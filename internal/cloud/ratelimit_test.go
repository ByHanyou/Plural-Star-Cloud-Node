// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"fmt"
	"testing"
	"time"
)

func TestRateLimiterPerAddress(t *testing.T) {
	l := newRateLimiter(10, 20)
	for i := 0; i < l.perAddress; i++ {
		if ok, _ := l.check("10.0.0.1", ""); !ok {
			t.Fatalf("attempt %d refused before the limit", i)
		}
		l.record("10.0.0.1", "")
	}
	ok, retry := l.check("10.0.0.1", "")
	if ok || retry <= 0 || retry > windowAddress {
		t.Fatalf("address limit not applied: ok=%v retry=%v", ok, retry)
	}
	if ok, _ := l.check("10.0.0.2", ""); !ok {
		t.Fatal("another address was limited")
	}
}

func TestRateLimiterPerVault(t *testing.T) {
	l := newRateLimiter(10, 20)
	for i := 0; i < l.perVault; i++ {
		l.record(fmt.Sprintf("10.0.%d.1", i), "vault-a")
	}
	ok, retry := l.check("10.9.9.9", "vault-a")
	if ok || retry <= 0 || retry > windowVault {
		t.Fatalf("vault limit not applied: ok=%v retry=%v", ok, retry)
	}
	if ok, _ := l.check("10.9.9.9", "vault-b"); !ok {
		t.Fatal("another vault was limited")
	}
}

func TestRateLimiterForgetsOldEvents(t *testing.T) {
	l := newRateLimiter(10, 20)
	old := time.Now().Add(-2 * windowVault)
	for i := 0; i < l.perAddress; i++ {
		l.byAddr["a"] = append(l.byAddr["a"], old)
	}
	if ok, _ := l.check("a", ""); !ok {
		t.Fatal("events outside the window still count")
	}
	l.byAddr["stale"] = []time.Time{old}
	l.byVault["stale"] = []time.Time{old}
	l.byAddr["fresh"] = []time.Time{time.Now()}
	l.sweep(time.Now())
	if _, ok := l.byAddr["stale"]; ok {
		t.Fatal("sweep kept a stale address")
	}
	if _, ok := l.byVault["stale"]; ok {
		t.Fatal("sweep kept a stale vault")
	}
	if _, ok := l.byAddr["fresh"]; !ok {
		t.Fatal("sweep dropped a fresh address")
	}
}
