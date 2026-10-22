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
)

var (
	ErrVersion       = errors.New("manifest version conflict")
	ErrMissingObject = errors.New("manifest references missing objects")
)

type Manifest struct {
	Version    int64    `json:"version"`
	Ciphertext []byte   `json:"ciphertext"`
	Objects    []ObjRef `json:"objects"`
}

func (v *vaultStore) manifestPath(id string, version int64) string {
	return filepath.Join(v.path(id), "manifest."+strconv.FormatInt(version, 10))
}

func manifestVersion(name string) (version int64, refs bool, ok bool) {
	if !strings.HasPrefix(name, "manifest.") {
		return 0, false, false
	}
	rest := strings.TrimPrefix(name, "manifest.")
	refs = strings.HasSuffix(rest, ".refs")
	ver, err := strconv.ParseInt(strings.TrimSuffix(rest, ".refs"), 10, 64)
	return ver, refs, err == nil
}

func (v *vaultStore) refs(id string, version int64) ([]ObjRef, error) {
	if version <= 0 {
		return nil, nil
	}
	raw, err := os.ReadFile(v.manifestPath(id, version) + ".refs")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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

func (v *vaultStore) GetManifest(id, secret string, version int64) (*Manifest, string, error) {
	unlock := v.locks.lock(id)
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
	raw, err := os.ReadFile(v.manifestPath(id, version))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
	unlock := v.locks.lock(id)
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
		if !validTier(r.Tier) {
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
	base := v.manifestPath(id, next)
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
	m.Pending = make(map[string]pendingRef)
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
	entries := listDir(v.path(id))
	for _, e := range entries {
		name := e.Name()
		ver, _, ok := manifestVersion(name)
		if !ok {
			continue
		}
		if ver <= latest-keep {
			removeFile(filepath.Join(v.path(id), name))
		}
	}
}

func (v *vaultStore) keptVersions(id string) []int64 {
	entries := listDir(v.path(id))
	var out []int64
	for _, e := range entries {
		if ver, refs, ok := manifestVersion(e.Name()); ok && refs {
			out = append(out, ver)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
