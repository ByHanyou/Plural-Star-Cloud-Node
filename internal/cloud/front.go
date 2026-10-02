// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// FrontEntry mirrors the gateway's cache record so a reader can rebuild the
// same rows either way. Readers is the author's signed reader list; an entry
// with no list is readable by nobody.
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
		return FrontEntry{}, false
	}
	var e FrontEntry
	if json.Unmarshal(raw, &e) != nil {
		return FrontEntry{}, false
	}
	return e, true
}

func (f *frontStore) count() int {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return 0
	}
	return len(entries)
}
