// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import "sync"

type keyedMutex[K comparable] struct {
	mu    sync.Mutex
	locks map[K]*keyedLock
}

type keyedLock struct {
	mu      sync.Mutex
	holders int
}

func newKeyedMutex[K comparable]() *keyedMutex[K] {
	return &keyedMutex[K]{locks: make(map[K]*keyedLock)}
}

func (k *keyedMutex[K]) lock(key K) func() {
	k.mu.Lock()
	l, ok := k.locks[key]
	if !ok {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.holders++
	k.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		l.holders--
		if l.holders == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}

func (k *keyedMutex[K]) size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.locks)
}
