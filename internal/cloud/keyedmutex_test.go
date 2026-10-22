// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"sync"
	"testing"
)

func TestKeyedMutexSerializesPerKey(t *testing.T) {
	km := newKeyedMutex[string]()
	keys := []string{"a", "b", "c"}
	const workers = 32
	const rounds = 200
	var counters [3]int
	var active [3]int
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			idx := w % len(keys)
			for i := 0; i < rounds; i++ {
				unlock := km.lock(keys[idx])
				active[idx]++
				if active[idx] != 1 {
					t.Errorf("key %s held by %d goroutines at once", keys[idx], active[idx])
				}
				counters[idx]++
				active[idx]--
				unlock()
			}
		}(w)
	}
	wg.Wait()
	want := [3]int{11 * rounds, 11 * rounds, 10 * rounds}
	for idx, n := range want {
		if counters[idx] != n {
			t.Errorf("key %s: counted %d, want %d", keys[idx], counters[idx], n)
		}
	}
	if km.size() != 0 {
		t.Errorf("lock table still holds %d entries after every unlock", km.size())
	}
}

func TestKeyedMutexIndependentKeys(t *testing.T) {
	km := newKeyedMutex[int]()
	unlockA := km.lock(1)
	done := make(chan struct{})
	go func() {
		unlockB := km.lock(2)
		unlockB()
		close(done)
	}()
	<-done
	if km.size() != 1 {
		t.Errorf("expected only the held key to remain, got %d entries", km.size())
	}
	unlockA()
	if km.size() != 0 {
		t.Errorf("expected an empty lock table, got %d entries", km.size())
	}
}
