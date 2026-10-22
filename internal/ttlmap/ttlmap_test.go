// SPDX-License-Identifier: AGPL-3.0-or-later

package ttlmap

import (
	"context"
	"sync"
	"testing"
	"time"
)

func (t *Map[K, V]) rawLen() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m)
}

func TestPutGetExpire(t *testing.T) {
	m := New[string, int](context.Background(), 0, nil)
	m.Put("a", 1, 50*time.Millisecond)
	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("get after put: ok=%v v=%d", ok, v)
	}
	if m.Len() != 1 {
		t.Fatalf("len = %d, want 1", m.Len())
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := m.Get("a"); ok {
		t.Fatal("expired entry still visible")
	}
	if m.Len() != 0 {
		t.Fatalf("len after expiry = %d, want 0", m.Len())
	}
	m.Put("a", 2, time.Minute)
	m.Remove("a")
	if _, ok := m.Get("a"); ok {
		t.Fatal("removed entry still visible")
	}
}

func TestPutIfAbsent(t *testing.T) {
	m := New[int, struct{}](context.Background(), 0, nil)
	if !m.PutIfAbsent(1, struct{}{}, 30*time.Millisecond) {
		t.Fatal("first put should succeed")
	}
	if m.PutIfAbsent(1, struct{}{}, 30*time.Millisecond) {
		t.Fatal("duplicate put should be refused")
	}
	time.Sleep(50 * time.Millisecond)
	if !m.PutIfAbsent(1, struct{}{}, 30*time.Millisecond) {
		t.Fatal("expired key should be free again")
	}
}

func TestUpdate(t *testing.T) {
	m := New[string, string](context.Background(), 0, nil)
	wrote := m.Update("k", func(cur string, ok bool, _ time.Time, size int) (string, time.Duration, bool) {
		if ok || size != 0 {
			t.Fatalf("first update saw ok=%v size=%d", ok, size)
		}
		return "v1", time.Minute, true
	})
	if !wrote {
		t.Fatal("first update should write")
	}
	wrote = m.Update("k", func(cur string, ok bool, _ time.Time, size int) (string, time.Duration, bool) {
		if !ok || cur != "v1" || size != 1 {
			t.Fatalf("second update saw ok=%v cur=%q size=%d", ok, cur, size)
		}
		return cur, 0, false
	})
	if wrote {
		t.Fatal("refused update should not write")
	}
	if v, ok := m.Get("k"); !ok || v != "v1" {
		t.Fatalf("value after refused update: ok=%v v=%q", ok, v)
	}
	m.Put("old", "x", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	m.Update("old", func(_ string, ok bool, _ time.Time, _ int) (string, time.Duration, bool) {
		if ok {
			t.Fatal("expired entry presented as live")
		}
		return "", 0, false
	})
}

func TestPruneCallsOnExpire(t *testing.T) {
	var mu sync.Mutex
	expired := map[string]int{}
	m := New[string, int](context.Background(), 0, func(k string, v int) {
		mu.Lock()
		expired[k] = v
		mu.Unlock()
	})
	m.Put("gone", 7, 10*time.Millisecond)
	m.Put("stays", 8, time.Minute)
	time.Sleep(30 * time.Millisecond)
	if n := m.Prune(); n != 1 {
		t.Fatalf("prune removed %d, want 1", n)
	}
	mu.Lock()
	got, ok := expired["gone"]
	mu.Unlock()
	if !ok || got != 7 {
		t.Fatalf("onExpire not called with the expired value: ok=%v got=%d", ok, got)
	}
	if m.Len() != 1 || m.rawLen() != 1 {
		t.Fatalf("len after prune = %d (raw %d), want 1", m.Len(), m.rawLen())
	}
	m.Each(func(k string, v int, _ time.Time) {
		if k != "stays" || v != 8 {
			t.Fatalf("unexpected entry %q=%d", k, v)
		}
	})
}

func TestPruneLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := New[string, int](ctx, 10*time.Millisecond, nil)
	m.Put("a", 1, 10*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.rawLen() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("prune loop did not remove the expired entry")
}
