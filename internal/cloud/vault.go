// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

var (
	ErrVaultExists   = errors.New("vault already exists")
	ErrVaultMissing  = errors.New("no vault")
	ErrBadAuth       = errors.New("bad auth secret")
	ErrVersion       = errors.New("manifest version conflict")
	ErrQuota         = errors.New("vault quota exceeded")
	ErrMissingObject = errors.New("manifest references missing objects")
)

type ObjRef struct {
	ID   string `json:"id"`
	Tier string `json:"tier"`
	Size int64  `json:"size"`
}

type Device struct {
	SubID    string `json:"sub_id"`
	Label    string `json:"label"`
	LastSeen int64  `json:"last_seen"`
}

type vaultMeta struct {
	Created          int64             `json:"created"`
	Updated          int64             `json:"updated"`
	AuthHash         string            `json:"auth_hash"`
	WrappedMasterKey string            `json:"wrapped_master_key"`
	Version          int64             `json:"version"`
	Devices          []Device          `json:"devices"`
	GraceStartedAt   int64             `json:"grace_started_at"`
	Pending          map[string]ObjRef `json:"pending"`
}

type VaultInfo struct {
	Created          int64            `json:"created"`
	Updated          int64            `json:"updated"`
	Version          int64            `json:"version"`
	Devices          []Device         `json:"devices"`
	GraceStartedAt   int64            `json:"grace_started_at,omitempty"`
	WrappedMasterKey string           `json:"wrapped_master_key"`
	Usage            map[string]int64 `json:"usage"`
	Quota            map[string]int64 `json:"quota"`
}

type Manifest struct {
	Version    int64    `json:"version"`
	Ciphertext []byte   `json:"ciphertext"`
	Objects    []ObjRef `json:"objects"`
}

type vaultStore struct {
	dir     string
	cfg     config.CloudConfig
	objects *objectStore

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newVaultStore(root string, cfg config.CloudConfig, objects *objectStore) *vaultStore {
	return &vaultStore{
		dir:     filepath.Join(root, "vaults"),
		cfg:     cfg,
		objects: objects,
		locks:   make(map[string]*sync.Mutex),
	}
}

func (v *vaultStore) lock(id string) func() {
	v.mu.Lock()
	m, ok := v.locks[id]
	if !ok {
		m = &sync.Mutex{}
		v.locks[id] = m
	}
	v.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func (v *vaultStore) path(id string) string { return filepath.Join(v.dir, id) }

func (v *vaultStore) quota(tier string) int64 {
	if tier == TierMedia {
		return int64(v.cfg.MediaQuotaMB) << 20
	}
	return int64(v.cfg.BaseQuotaMB) << 20
}

func (v *vaultStore) count() int {
	entries, err := os.ReadDir(v.dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && validHex32(e.Name()) {
			n++
		}
	}
	return n
}

func (v *vaultStore) ids() []string {
	entries, err := os.ReadDir(v.dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && validHex32(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out
}

func (v *vaultStore) exists(id string) bool {
	_, err := os.Stat(filepath.Join(v.path(id), "vault.json"))
	return err == nil
}

func (v *vaultStore) readMeta(id string) (*vaultMeta, error) {
	raw, err := os.ReadFile(filepath.Join(v.path(id), "vault.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrVaultMissing
		}
		return nil, err
	}
	var m vaultMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("vault %s: corrupt vault.json: %w", id, err)
	}
	if m.Pending == nil {
		m.Pending = make(map[string]ObjRef)
	}
	return &m, nil
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return renameRetry(tmp, path)
}

func (v *vaultStore) writeMeta(id string, m *vaultMeta) error {
	m.Updated = time.Now().UnixMilli()
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(v.path(id), "vault.json"), raw)
}

// authed loads the vault and checks the auth secret. The caller holds the lock.
func (v *vaultStore) authed(id, secret string) (*vaultMeta, error) {
	m, err := v.readMeta(id)
	if err != nil {
		return nil, err
	}
	if !secretMatches(m.AuthHash, secret) {
		return nil, ErrBadAuth
	}
	return m, nil
}

func (v *vaultStore) Create(id, secret, wrappedMasterKey string) error {
	unlock := v.lock(id)
	defer unlock()
	if v.exists(id) {
		return ErrVaultExists
	}
	if err := os.MkdirAll(v.path(id), 0o700); err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	m := &vaultMeta{
		Created:          now,
		AuthHash:         hashSecret(secret),
		WrappedMasterKey: wrappedMasterKey,
		Devices:          []Device{},
		Pending:          make(map[string]ObjRef),
	}
	return v.writeMeta(id, m)
}

func (v *vaultStore) refs(id string, version int64) ([]ObjRef, error) {
	if version <= 0 {
		return nil, nil
	}
	raw, err := os.ReadFile(filepath.Join(v.path(id), "manifest."+strconv.FormatInt(version, 10)+".refs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var refs []ObjRef
	if err := json.Unmarshal(raw, &refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func (v *vaultStore) usage(id string, m *vaultMeta) (map[string]int64, map[string]ObjRef, error) {
	refs, err := v.refs(id, m.Version)
	if err != nil {
		return nil, nil, err
	}
	known := make(map[string]ObjRef, len(refs)+len(m.Pending))
	for _, r := range refs {
		known[r.ID] = r
	}
	for _, r := range m.Pending {
		if _, seen := known[r.ID]; !seen {
			known[r.ID] = r
		}
	}
	use := map[string]int64{TierBase: 0, TierMedia: 0}
	for _, r := range known {
		use[r.Tier] += r.Size
	}
	// The current manifest counts against the quota too.
	if m.Version > 0 {
		if st, serr := os.Stat(filepath.Join(v.path(id), "manifest."+strconv.FormatInt(m.Version, 10))); serr == nil {
			use[TierBase] += st.Size()
		}
	}
	return use, known, nil
}

func (v *vaultStore) Info(id, secret string) (*VaultInfo, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, err
	}
	use, _, err := v.usage(id, m)
	if err != nil {
		return nil, err
	}
	return &VaultInfo{
		Created:          m.Created,
		Updated:          m.Updated,
		Version:          m.Version,
		Devices:          m.Devices,
		GraceStartedAt:   m.GraceStartedAt,
		WrappedMasterKey: m.WrappedMasterKey,
		Usage:            use,
		Quota:            map[string]int64{TierBase: v.quota(TierBase), TierMedia: v.quota(TierMedia)},
	}, nil
}

// ReserveObject checks the tier quota for an upload of size bytes.
func (v *vaultStore) ReserveObject(id, secret, objID, tier string, size int64) error {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return err
	}
	use, known, err := v.usage(id, m)
	if err != nil {
		return err
	}
	if _, already := known[objID]; already {
		return nil
	}
	if use[tier]+size > v.quota(tier) {
		return ErrQuota
	}
	return nil
}

// CommitObject records a finished upload as pending until a manifest names it.
func (v *vaultStore) CommitObject(id, secret string, ref ObjRef) error {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return err
	}
	m.Pending[ref.ID] = ref
	return v.writeMeta(id, m)
}

// Owns reports whether objID belongs to the vault (referenced or pending).
func (v *vaultStore) Owns(id, secret, objID string) (bool, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return false, err
	}
	_, known, err := v.usage(id, m)
	if err != nil {
		return false, err
	}
	_, ok := known[objID]
	return ok, nil
}

func (v *vaultStore) GetManifest(id, secret string, version int64) (*Manifest, string, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, "", err
	}
	if version <= 0 {
		version = m.Version
	}
	if version <= 0 {
		return &Manifest{Version: 0}, m.WrappedMasterKey, nil
	}
	raw, err := os.ReadFile(filepath.Join(v.path(id), "manifest."+strconv.FormatInt(version, 10)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	refs, err := v.refs(id, version)
	if err != nil {
		return nil, "", err
	}
	if refs == nil {
		refs = []ObjRef{}
	}
	return &Manifest{Version: version, Ciphertext: raw, Objects: refs}, m.WrappedMasterKey, nil
}

type missingError struct{ IDs []string }

func (e *missingError) Error() string { return ErrMissingObject.Error() }
func (e *missingError) Unwrap() error { return ErrMissingObject }

type versionError struct{ Current int64 }

func (e *versionError) Error() string { return ErrVersion.Error() }
func (e *versionError) Unwrap() error { return ErrVersion }

func (v *vaultStore) PutManifest(id, secret string, expected int64, ciphertext []byte, refs []ObjRef) (int64, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return 0, err
	}
	if expected != m.Version {
		return m.Version, &versionError{Current: m.Version}
	}
	use := map[string]int64{TierBase: 0, TierMedia: 0}
	seen := make(map[string]struct{}, len(refs))
	var missing []string
	clean := make([]ObjRef, 0, len(refs))
	for _, r := range refs {
		if !validHex32(r.ID) {
			return m.Version, fmt.Errorf("invalid object id %q", r.ID)
		}
		if r.Tier != TierBase && r.Tier != TierMedia {
			return m.Version, fmt.Errorf("invalid tier %q for object %s", r.Tier, r.ID)
		}
		if _, dup := seen[r.ID]; dup {
			continue
		}
		seen[r.ID] = struct{}{}
		size, _, herr := v.objects.Head(r.ID)
		if herr != nil {
			if errors.Is(herr, ErrNotFound) {
				missing = append(missing, r.ID)
				continue
			}
			return m.Version, herr
		}
		r.Size = size
		use[r.Tier] += size
		clean = append(clean, r)
	}
	if len(missing) > 0 {
		return m.Version, &missingError{IDs: missing}
	}
	use[TierBase] += int64(len(ciphertext))
	for tier, used := range use {
		if used > v.quota(tier) {
			return m.Version, ErrQuota
		}
	}
	next := m.Version + 1
	base := filepath.Join(v.path(id), "manifest."+strconv.FormatInt(next, 10))
	refsRaw, err := json.Marshal(clean)
	if err != nil {
		return m.Version, err
	}
	if err := writeAtomic(base+".refs", refsRaw); err != nil {
		return m.Version, err
	}
	if err := writeAtomic(base, ciphertext); err != nil {
		_ = os.Remove(base + ".refs")
		return m.Version, err
	}
	m.Version = next
	m.Pending = make(map[string]ObjRef)
	if err := v.writeMeta(id, m); err != nil {
		return m.Version, err
	}
	v.pruneManifests(id, next)
	return next, nil
}

func (v *vaultStore) pruneManifests(id string, latest int64) {
	keep := int64(v.cfg.ManifestVersions)
	if keep <= 0 {
		keep = 1
	}
	entries, err := os.ReadDir(v.path(id))
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "manifest.") {
			continue
		}
		rest := strings.TrimPrefix(name, "manifest.")
		rest = strings.TrimSuffix(rest, ".refs")
		ver, perr := strconv.ParseInt(rest, 10, 64)
		if perr != nil {
			continue
		}
		if ver <= latest-keep {
			_ = os.Remove(filepath.Join(v.path(id), name))
		}
	}
}

func (v *vaultStore) keptVersions(id string) []int64 {
	entries, err := os.ReadDir(v.path(id))
	if err != nil {
		return nil
	}
	var out []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "manifest.") || !strings.HasSuffix(name, ".refs") {
			continue
		}
		ver, perr := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "manifest."), ".refs"), 10, 64)
		if perr == nil {
			out = append(out, ver)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (v *vaultStore) LinkDevice(id, secret, subID, label string) ([]Device, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	found := false
	for i := range m.Devices {
		if m.Devices[i].SubID == subID {
			m.Devices[i].Label = label
			m.Devices[i].LastSeen = now
			found = true
		}
	}
	if !found {
		m.Devices = append(m.Devices, Device{SubID: subID, Label: label, LastSeen: now})
	}
	m.GraceStartedAt = 0
	if err := v.writeMeta(id, m); err != nil {
		return nil, err
	}
	return m.Devices, nil
}

func (v *vaultStore) UnlinkDevice(id, secret, subID string) ([]Device, error) {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, err
	}
	kept := m.Devices[:0]
	for _, d := range m.Devices {
		if d.SubID != subID {
			kept = append(kept, d)
		}
	}
	m.Devices = kept
	if len(m.Devices) == 0 && m.GraceStartedAt == 0 {
		m.GraceStartedAt = time.Now().UnixMilli()
	}
	if err := v.writeMeta(id, m); err != nil {
		return nil, err
	}
	return m.Devices, nil
}

func (v *vaultStore) Rewrap(id, secret, newID, newSecret, wrappedMasterKey string) error {
	unlock := v.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return err
	}
	if wrappedMasterKey != "" {
		m.WrappedMasterKey = wrappedMasterKey
	}
	if newSecret != "" {
		m.AuthHash = hashSecret(newSecret)
	}
	if newID == "" || newID == id {
		return v.writeMeta(id, m)
	}
	unlockNew := v.lock(newID)
	defer unlockNew()
	if v.exists(newID) {
		return ErrVaultExists
	}
	if err := v.writeMeta(id, m); err != nil {
		return err
	}
	return renameRetry(v.path(id), v.path(newID))
}

func (v *vaultStore) Delete(id string) error {
	unlock := v.lock(id)
	defer unlock()
	return os.RemoveAll(v.path(id))
}

// liveObjects returns every object ID referenced by any kept manifest version
// or pending in any vault, plus the vaults whose grace period has elapsed.
//
// Unreadable metadata is an error, never an empty vault: the caller deletes
// every object not in live.
func (v *vaultStore) liveObjects(graceCutoff time.Time) (live map[string]struct{}, expiredVaults []string, err error) {
	live = make(map[string]struct{})
	for _, id := range v.ids() {
		unlock := v.lock(id)
		m, rerr := v.readMeta(id)
		if rerr != nil {
			unlock()
			if errors.Is(rerr, ErrVaultMissing) {
				// A directory without vault.json is a create that died halfway.
				continue
			}
			return nil, nil, rerr
		}
		// A vault with no linked device expires GraceDays after the unlink that
		// emptied it, or, when nothing was ever linked, after its last write.
		graceStart := m.GraceStartedAt
		if graceStart == 0 {
			graceStart = m.Updated
		}
		if len(m.Devices) == 0 && graceStart > 0 && time.UnixMilli(graceStart).Before(graceCutoff) {
			expiredVaults = append(expiredVaults, id)
			unlock()
			continue
		}
		for _, r := range m.Pending {
			live[r.ID] = struct{}{}
		}
		for _, ver := range v.keptVersions(id) {
			refs, rerr := v.refs(id, ver)
			if rerr != nil {
				unlock()
				return nil, nil, fmt.Errorf("vault %s manifest %d refs: %w", id, ver, rerr)
			}
			for _, r := range refs {
				live[r.ID] = struct{}{}
			}
		}
		unlock()
	}
	return live, expiredVaults, nil
}
