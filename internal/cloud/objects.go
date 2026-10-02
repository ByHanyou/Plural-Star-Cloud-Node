// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"golang.org/x/crypto/blake2b"
)

const (
	TierBase  = "base"
	TierMedia = "media"
)

var (
	ErrHashMismatch = errors.New("object id does not match ciphertext hash")
	ErrOffset       = errors.New("upload offset mismatch")
	ErrTooLarge     = errors.New("object too large")
)

type objectStore struct {
	root  string
	tmp   string
	local *localBackend
	media Backend
	cfg   config.CloudConfig

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newObjectStore(root string, media Backend, cfg config.CloudConfig) *objectStore {
	return &objectStore{
		root:  root,
		tmp:   filepath.Join(root, "tmp"),
		local: newLocalBackend(filepath.Join(root, "objects")),
		media: media,
		cfg:   cfg,
		locks: make(map[string]*sync.Mutex),
	}
}

func (o *objectStore) maxBytes() int64 { return int64(o.cfg.ObjectMaxMB) << 20 }

func (o *objectStore) lock(key string) func() {
	o.mu.Lock()
	m, ok := o.locks[key]
	if !ok {
		m = &sync.Mutex{}
		o.locks[key] = m
	}
	o.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func (o *objectStore) backendFor(tier string) Backend {
	if tier == TierMedia {
		return o.media
	}
	return o.local
}

func (o *objectStore) Head(id string) (int64, string, error) {
	if size, err := o.local.Head(id); err == nil {
		return size, TierBase, nil
	} else if !errors.Is(err, ErrNotFound) {
		return 0, "", err
	}
	if o.media.Name() != config.MediaBackendLocal {
		if size, err := o.media.Head(id); err == nil {
			return size, TierMedia, nil
		} else if !errors.Is(err, ErrNotFound) {
			return 0, "", err
		}
	}
	return 0, "", ErrNotFound
}

func (o *objectStore) Get(id string) (io.ReadCloser, int64, error) {
	rc, size, err := o.local.Get(id)
	if err == nil {
		return rc, size, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, 0, err
	}
	if o.media.Name() != config.MediaBackendLocal {
		return o.media.Get(id)
	}
	return nil, 0, ErrNotFound
}

func (o *objectStore) Delete(id string) error {
	if err := o.local.Delete(id); err != nil {
		return err
	}
	if o.media.Name() != config.MediaBackendLocal {
		return o.media.Delete(id)
	}
	return nil
}

func (o *objectStore) partPath(vault, id, tier string, total int64) string {
	return filepath.Join(o.tmp, vault+"."+id+"."+tier+"."+strconv.FormatInt(total, 10)+".part")
}

func (o *objectStore) findPart(vault, id string) (path string, tier string, total int64, received int64, ok bool) {
	entries, err := os.ReadDir(o.tmp)
	if err != nil {
		return "", "", 0, 0, false
	}
	prefix := vault + "." + id + "."
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".part") {
			continue
		}
		rest := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".part")
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			continue
		}
		t, perr := strconv.ParseInt(parts[1], 10, 64)
		if perr != nil {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		return filepath.Join(o.tmp, name), parts[0], t, info.Size(), true
	}
	return "", "", 0, 0, false
}

// Received reports how many bytes of a pending upload have arrived, for resume.
func (o *objectStore) Received(vault, id string) (int64, bool) {
	unlock := o.lock(vault + "." + id)
	defer unlock()
	_, _, _, received, ok := o.findPart(vault, id)
	return received, ok
}

type uploadResult struct {
	Complete bool
	Received int64
	Total    int64
	Tier     string
}

// Append writes one chunk of an upload. start must equal the bytes already
// received; total is the object's full size. When the last byte lands the
// ciphertext is hashed, checked against id, and moved into its backend.
func (o *objectStore) Append(vault, id, tier string, start, total int64, body io.Reader) (uploadResult, error) {
	if tier != TierBase && tier != TierMedia {
		return uploadResult{}, fmt.Errorf("invalid tier %q", tier)
	}
	if total <= 0 || total > o.maxBytes() {
		return uploadResult{}, ErrTooLarge
	}
	unlock := o.lock(vault + "." + id)
	defer unlock()

	path, existingTier, existingTotal, received, ok := o.findPart(vault, id)
	if ok && (existingTier != tier || existingTotal != total) {
		_ = os.Remove(path)
		ok = false
		received = 0
	}
	if !ok {
		path = o.partPath(vault, id, tier, total)
		received = 0
	}
	if start != received {
		return uploadResult{Received: received, Total: total, Tier: tier}, ErrOffset
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return uploadResult{}, err
	}
	n, err := io.Copy(f, io.LimitReader(body, total-received+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return uploadResult{}, err
	}
	received += n
	if received > total {
		_ = os.Remove(path)
		return uploadResult{}, ErrTooLarge
	}
	if received < total {
		return uploadResult{Received: received, Total: total, Tier: tier}, nil
	}
	if err := o.finalize(path, id, tier); err != nil {
		_ = os.Remove(path)
		return uploadResult{}, err
	}
	return uploadResult{Complete: true, Received: total, Total: total, Tier: tier}, nil
}

func (o *objectStore) finalize(path, id, tier string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	h, err := blake2b.New256(nil)
	if err != nil {
		_ = f.Close()
		return err
	}
	_, err = io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != id {
		return ErrHashMismatch
	}
	backend := o.backendFor(tier)
	if _, err := backend.Head(id); err == nil {
		return os.Remove(path)
	}
	return backend.PutFile(id, path)
}

// Abandon deletes a pending upload for vault and id, if any.
func (o *objectStore) Abandon(vault, id string) {
	unlock := o.lock(vault + "." + id)
	defer unlock()
	if path, _, _, _, ok := o.findPart(vault, id); ok {
		_ = os.Remove(path)
	}
}

// sweepParts removes upload fragments nobody has touched for a day.
func (o *objectStore) sweepParts(olderThan time.Duration) {
	entries, err := os.ReadDir(o.tmp)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-olderThan)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || e.IsDir() {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(o.tmp, e.Name()))
		}
	}
}

// Hash computes the object ID of a ciphertext, for tests and tooling.
func Hash(b []byte) string {
	sum := blake2b.Sum256(b)
	return hex.EncodeToString(sum[:])
}
