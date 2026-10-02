// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	rendezvousMaxEntries   = 20000
	rendezvousMaxRecordLen = 8 * 1024
	rendezvousMaxTTLSecs   = 3600
)

type rendezvousEntry struct {
	record    string
	expiresAt time.Time
	local     bool
}

type persistedEntry struct {
	Record    string `json:"record"`
	ExpiresAt int64  `json:"expires_at"`
	Local     bool   `json:"local,omitempty"`
}

type rendezvousStore struct {
	mu      sync.Mutex
	entries map[string]rendezvousEntry
	path    string
	fileMu  sync.Mutex
}

func rendezvousPathFor(configPath string) string {
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), "rendezvous.json")
}

func newRendezvousStore(path string) *rendezvousStore {
	rs := &rendezvousStore{entries: make(map[string]rendezvousEntry), path: path}
	rs.load()
	go rs.janitor()
	return rs
}

func (rs *rendezvousStore) load() {
	if rs.path == "" {
		return
	}
	b, err := os.ReadFile(rs.path)
	if err != nil {
		return
	}
	var m map[string]persistedEntry
	if json.Unmarshal(b, &m) != nil {
		return
	}
	now := time.Now()
	rs.mu.Lock()
	for ns, e := range m {
		exp := time.Unix(e.ExpiresAt, 0)
		if e.Record != "" && exp.After(now) {
			rs.entries[ns] = rendezvousEntry{record: e.Record, expiresAt: exp, local: e.Local}
		}
	}
	rs.mu.Unlock()
}

func (rs *rendezvousStore) snapshotLocked() []byte {
	if rs.path == "" {
		return nil
	}
	m := make(map[string]persistedEntry, len(rs.entries))
	for ns, e := range rs.entries {
		m[ns] = persistedEntry{Record: e.record, ExpiresAt: e.expiresAt.Unix(), Local: e.local}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

func (rs *rendezvousStore) persist(b []byte) {
	if rs.path == "" || b == nil {
		return
	}
	rs.fileMu.Lock()
	defer rs.fileMu.Unlock()
	tmp := rs.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) != nil {
		return
	}
	_ = os.Rename(tmp, rs.path)
}

func (rs *rendezvousStore) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		rs.mu.Lock()
		removed := 0
		for k, e := range rs.entries {
			if now.After(e.expiresAt) {
				delete(rs.entries, k)
				removed++
			}
		}
		var snapshot []byte
		if removed > 0 {
			snapshot = rs.snapshotLocked()
		}
		rs.mu.Unlock()
		rs.persist(snapshot)
	}
}

func (rs *rendezvousStore) put(namespace, record string, ttl time.Duration) bool {
	rs.mu.Lock()
	if _, exists := rs.entries[namespace]; !exists && len(rs.entries) >= rendezvousMaxEntries {
		rs.mu.Unlock()
		return false
	}
	rs.entries[namespace] = rendezvousEntry{record: record, expiresAt: time.Now().Add(ttl), local: true}
	snapshot := rs.snapshotLocked()
	rs.mu.Unlock()
	rs.persist(snapshot)
	return true
}

func (rs *rendezvousStore) putRemote(namespace, record string, ttl time.Duration) {
	expiresAt := time.Now().Add(ttl)
	rs.mu.Lock()
	cur, exists := rs.entries[namespace]
	if exists && !cur.expiresAt.Before(expiresAt) {
		rs.mu.Unlock()
		return
	}
	if !exists && len(rs.entries) >= rendezvousMaxEntries {
		rs.mu.Unlock()
		return
	}
	local := exists && cur.local
	rs.entries[namespace] = rendezvousEntry{record: record, expiresAt: expiresAt, local: local}
	snapshot := rs.snapshotLocked()
	rs.mu.Unlock()
	rs.persist(snapshot)
}

func (rs *rendezvousStore) localEntries() []struct {
	Namespace string
	Record    string
	ExpiresAt time.Time
} {
	now := time.Now()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var out []struct {
		Namespace string
		Record    string
		ExpiresAt time.Time
	}
	for ns, e := range rs.entries {
		if e.local && e.expiresAt.After(now) {
			out = append(out, struct {
				Namespace string
				Record    string
				ExpiresAt time.Time
			}{ns, e.record, e.expiresAt})
		}
	}
	return out
}

func (rs *rendezvousStore) get(namespace string) (string, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	e, ok := rs.entries[namespace]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiresAt) {
		delete(rs.entries, namespace)
		return "", false
	}
	return e.record, true
}

func (s *Server) handleRendezvousRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
		Record    string `json:"record"`
		TTLSecs   int    `json:"ttl_seconds"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Namespace == "" || req.Record == "" {
		writeError(w, http.StatusBadRequest, "namespace and record required")
		return
	}
	if len(req.Record) > rendezvousMaxRecordLen {
		writeError(w, http.StatusBadRequest, "record too large")
		return
	}
	ttl := req.TTLSecs
	if ttl <= 0 || ttl > rendezvousMaxTTLSecs {
		ttl = rendezvousMaxTTLSecs
	}
	if !s.rv.put(req.Namespace, req.Record, time.Duration(ttl)*time.Second) {
		writeError(w, http.StatusServiceUnavailable, "rendezvous full")
		return
	}
	if g := s.rendezvousGossip(); g != nil {
		if err := g.Announce(req.Namespace, req.Record, time.Now().Add(time.Duration(ttl)*time.Second)); err != nil {
			log.Printf("rendezvous: gossip announce failed: %v", err)
		}
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleRendezvousLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		writeError(w, http.StatusBadRequest, "namespace required")
		return
	}
	rec, ok := s.rv.get(ns)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"record": rec})
}
