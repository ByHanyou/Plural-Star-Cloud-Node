// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ttlmap"
)

type rendezvousEntry struct {
	record string
	local  bool
}

type persistedEntry struct {
	Record    string `json:"record"`
	ExpiresAt int64  `json:"expires_at"`
	Local     bool   `json:"local,omitempty"`
}

type rendezvousLocalEntry struct {
	Namespace string
	Record    string
	ExpiresAt time.Time
}

type RendezvousStore struct {
	maxEntries int
	entries    *ttlmap.Map[string, rendezvousEntry]
	path       string
	fileMu     sync.Mutex

	timerMu      sync.Mutex
	persistTimer *time.Timer
}

const rendezvousPersistDelay = 500 * time.Millisecond

func rendezvousPathFor(configPath string) string {
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), "rendezvous.json")
}

func NewRendezvousStore(ctx context.Context, configPath string, lim config.Limits) *RendezvousStore {
	return newRendezvousStore(ctx, rendezvousPathFor(configPath), lim.RendezvousMaxEntries)
}

func newRendezvousStore(ctx context.Context, path string, maxEntries int) *RendezvousStore {
	rs := &RendezvousStore{path: path, maxEntries: maxEntries}
	rs.entries = ttlmap.New[string, rendezvousEntry](ctx, time.Minute, func(string, rendezvousEntry) {
		rs.schedulePersist()
	})
	rs.load()
	return rs
}

func (rs *RendezvousStore) load() {
	if rs.path == "" {
		return
	}
	b, err := os.ReadFile(rs.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("rendezvous: reading saved records failed", "path", rs.path, "err", err)
		}
		return
	}
	var m map[string]persistedEntry
	if err := json.Unmarshal(b, &m); err != nil {
		slog.Warn("rendezvous: saved records unreadable", "path", rs.path, "err", err)
		return
	}
	now := time.Now()
	for ns, e := range m {
		exp := time.Unix(e.ExpiresAt, 0)
		if e.Record != "" && exp.After(now) {
			rs.entries.Put(ns, rendezvousEntry{record: e.Record, local: e.Local}, exp.Sub(now))
		}
	}
}

func (rs *RendezvousStore) snapshot() []byte {
	if rs.path == "" {
		return nil
	}
	m := make(map[string]persistedEntry)
	rs.entries.Each(func(ns string, e rendezvousEntry, exp time.Time) {
		m[ns] = persistedEntry{Record: e.record, ExpiresAt: exp.Unix(), Local: e.local}
	})
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

func (rs *RendezvousStore) persist(b []byte) {
	if rs.path == "" || b == nil {
		return
	}
	rs.fileMu.Lock()
	defer rs.fileMu.Unlock()
	tmp := rs.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		slog.Warn("rendezvous: persist failed", "path", rs.path, "err", err)
		return
	}
	if err := os.Rename(tmp, rs.path); err != nil {
		slog.Warn("rendezvous: persist failed", "path", rs.path, "err", err)
	}
}

func (rs *RendezvousStore) schedulePersist() {
	if rs.path == "" {
		return
	}
	rs.timerMu.Lock()
	defer rs.timerMu.Unlock()
	if rs.persistTimer != nil {
		rs.persistTimer.Reset(rendezvousPersistDelay)
		return
	}
	rs.persistTimer = time.AfterFunc(rendezvousPersistDelay, func() {
		rs.persist(rs.snapshot())
	})
}

func (rs *RendezvousStore) put(namespace, record string, ttl time.Duration) bool {
	written := rs.entries.Update(namespace, func(_ rendezvousEntry, exists bool, _ time.Time, size int) (rendezvousEntry, time.Duration, bool) {
		if !exists && size >= rs.maxEntries {
			return rendezvousEntry{}, 0, false
		}
		return rendezvousEntry{record: record, local: true}, ttl, true
	})
	if written {
		rs.schedulePersist()
	}
	return written
}

func (rs *RendezvousStore) PutRemote(namespace, record string, ttl time.Duration) {
	expiresAt := time.Now().Add(ttl)
	written := rs.entries.Update(namespace, func(cur rendezvousEntry, exists bool, curExpiresAt time.Time, size int) (rendezvousEntry, time.Duration, bool) {
		if exists && (cur.local || !curExpiresAt.Before(expiresAt)) {
			return cur, 0, false
		}
		if !exists && size >= rs.maxEntries {
			return cur, 0, false
		}
		return rendezvousEntry{record: record, local: false}, ttl, true
	})
	if written {
		rs.schedulePersist()
	}
}

func (rs *RendezvousStore) localEntries() []rendezvousLocalEntry {
	var out []rendezvousLocalEntry
	rs.entries.Each(func(ns string, e rendezvousEntry, exp time.Time) {
		if e.local {
			out = append(out, rendezvousLocalEntry{Namespace: ns, Record: e.record, ExpiresAt: exp})
		}
	})
	return out
}

func (rs *RendezvousStore) get(namespace string) (string, bool) {
	e, ok := rs.entries.Get(namespace)
	if !ok {
		return "", false
	}
	return e.record, true
}
