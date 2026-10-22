// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type FrontEntry struct {
	Fronters    string   `json:"fronters"`
	StartTime   int64    `json:"start_time"`
	Name        string   `json:"name"`
	At          int64    `json:"at"`
	AuthoredAt  int64    `json:"authored_at,omitempty"`
	Primary     string   `json:"primary,omitempty"`
	CoFront     string   `json:"co_front,omitempty"`
	CoConscious string   `json:"co_conscious,omitempty"`
	Readers     []string `json:"readers,omitempty"`
}

type frontStore struct {
	dir string
	mu  sync.Mutex
}

func newFrontStore(dir string) *frontStore { return &frontStore{dir: dir} }

func (f *frontStore) path(peerID string) string { return filepath.Join(f.dir, peerID+".json") }

func (f *frontStore) Put(peerID string, e FrontEntry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return writeAtomic(f.path(peerID), raw)
}

func (f *frontStore) Get(peerID string) (FrontEntry, bool) {
	f.mu.Lock()
	raw, err := os.ReadFile(f.path(peerID))
	f.mu.Unlock()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("cloud: reading front failed", "peer", peerID, "err", err)
		}
		return FrontEntry{}, false
	}
	var e FrontEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		slog.Warn("cloud: stored front unreadable", "peer", peerID, "err", err)
		return FrontEntry{}, false
	}
	return e, true
}

func (f *frontStore) sweep(maxAge time.Duration) {
	entries := listDir(f.dir)
	cutoff := time.Now().Add(-maxAge)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil || info.ModTime().After(cutoff) {
			continue
		}
		removeFile(filepath.Join(f.dir, e.Name()))
	}
}
