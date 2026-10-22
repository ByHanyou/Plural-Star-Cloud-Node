// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

var (
	ErrVaultExists  = errors.New("vault already exists")
	ErrVaultMissing = errors.New("no vault")
	ErrBadAuth      = errors.New("bad auth secret")
	ErrQuota        = errors.New("vault quota exceeded")
)

type ObjRef struct {
	ID   string `json:"id"`
	Tier string `json:"tier"`
	Size int64  `json:"size"`
}

type pendingRef struct {
	ObjRef
	At int64 `json:"at,omitempty"`
}

func (p pendingRef) expired(now time.Time, grace time.Duration) bool {
	return p.At <= 0 || now.Sub(time.UnixMilli(p.At)) > grace
}

type vaultMeta struct {
	Created          int64                 `json:"created"`
	Updated          int64                 `json:"updated"`
	AuthHash         string                `json:"auth_hash"`
	WrappedMasterKey string                `json:"wrapped_master_key"`
	Version          int64                 `json:"version"`
	Devices          []deviceEntry         `json:"devices"`
	GraceStartedAt   int64                 `json:"grace_started_at"`
	Pending          map[string]pendingRef `json:"pending"`
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

type vaultStore struct {
	dir     string
	cfg     config.CloudConfig
	objects *objectStore
	locks   *keyedMutex[string]
	grace   time.Duration
}

func newVaultStore(root string, cfg config.CloudConfig, objects *objectStore, grace time.Duration) *vaultStore {
	return &vaultStore{
		dir:     filepath.Join(root, "vaults"),
		cfg:     cfg,
		objects: objects,
		locks:   newKeyedMutex[string](),
		grace:   grace,
	}
}

func (v *vaultStore) path(id string) string { return filepath.Join(v.dir, id) }

func (v *vaultStore) metaPath(id string) string { return filepath.Join(v.path(id), "vault.json") }

func (v *vaultStore) quota(tier string) int64 {
	if tier == TierMedia {
		return int64(v.cfg.MediaQuotaMB) << 20
	}
	return int64(v.cfg.BaseQuotaMB) << 20
}

func (v *vaultStore) count() int { return len(v.ids()) }

func (v *vaultStore) ids() []string {
	entries := listDir(v.dir)
	var out []string
	for _, e := range entries {
		if e.IsDir() && validHex32(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out
}

func (v *vaultStore) exists(id string) bool {
	_, err := os.Stat(v.metaPath(id))
	return err == nil
}

func (v *vaultStore) readMeta(id string) (*vaultMeta, error) {
	raw, err := os.ReadFile(v.metaPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrVaultMissing
		}
		return nil, err
	}
	var m vaultMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("vault %s: corrupt vault.json: %w", id, err)
	}
	if m.Pending == nil {
		m.Pending = make(map[string]pendingRef)
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
	return writeAtomic(v.metaPath(id), raw)
}

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
	unlock := v.locks.lock(id)
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
		Devices:          []deviceEntry{},
		Pending:          make(map[string]pendingRef),
	}
	return v.writeMeta(id, m)
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
	now := time.Now()
	for _, p := range m.Pending {
		if p.expired(now, v.grace) {
			continue
		}
		if _, seen := known[p.ID]; !seen {
			known[p.ID] = p.ObjRef
		}
	}
	use := map[string]int64{TierBase: 0, TierMedia: 0}
	for _, r := range known {
		use[r.Tier] += r.Size
	}
	if m.Version > 0 {
		if st, serr := os.Stat(v.manifestPath(id, m.Version)); serr == nil {
			use[TierBase] += st.Size()
		}
	}
	return use, known, nil
}

func (v *vaultStore) Info(id, secret string) (*VaultInfo, error) {
	unlock := v.locks.lock(id)
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
		Devices:          publicDevices(m.Devices),
		GraceStartedAt:   m.GraceStartedAt,
		WrappedMasterKey: m.WrappedMasterKey,
		Usage:            use,
		Quota:            map[string]int64{TierBase: v.quota(TierBase), TierMedia: v.quota(TierMedia)},
	}, nil
}

func (v *vaultStore) ReserveObject(id, secret, objID, tier string, size int64) error {
	unlock := v.locks.lock(id)
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

func (v *vaultStore) CommitObject(id, secret string, ref ObjRef) error {
	unlock := v.locks.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return err
	}
	m.Pending[ref.ID] = pendingRef{ObjRef: ref, At: time.Now().UnixMilli()}
	return v.writeMeta(id, m)
}

func (v *vaultStore) Owns(id, secret, objID string) (bool, error) {
	unlock := v.locks.lock(id)
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

func (v *vaultStore) Rewrap(id, secret, newID, newSecret, wrappedMasterKey string) error {
	unlock := v.locks.lock(id)
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
	unlockNew := v.locks.lock(newID)
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
	unlock := v.locks.lock(id)
	defer unlock()
	return os.RemoveAll(v.path(id))
}
