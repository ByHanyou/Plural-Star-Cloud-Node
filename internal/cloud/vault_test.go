// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	"golang.org/x/crypto/blake2b"
)

var testVaultSecret = strings.Repeat("5e", 32)

var testGrace = config.DefaultLimits().GCObjectGrace.Std()

func testHash(b []byte) string {
	sum := blake2b.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newTestVault(t *testing.T, baseQuotaMB int) (*vaultStore, string) {
	t.Helper()
	root := t.TempDir()
	for _, sub := range []string{"vaults", "objects", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.CloudConfig{
		BaseQuotaMB:      baseQuotaMB,
		MediaQuotaMB:     1024,
		ObjectMaxMB:      64,
		ManifestVersions: 30,
		GraceDays:        30,
	}
	objects := newObjectStore(root, newLocalBackend(filepath.Join(root, "objects")), cfg)
	vs := newVaultStore(root, cfg, objects, testGrace)
	id := testHash([]byte("test vault"))
	if err := vs.Create(id, testVaultSecret, "wrapped-master-key"); err != nil {
		t.Fatalf("create vault: %v", err)
	}
	if _, _, _, err := vs.LinkDevice(id, testVaultSecret, "device-1", "Phone"); err != nil {
		t.Fatalf("link device: %v", err)
	}
	return vs, id
}

func testObjectID(name string) string { return testHash([]byte(name)) }

func storeObject(t *testing.T, vs *vaultStore, name string, size int) string {
	t.Helper()
	data := bytes.Repeat([]byte{0x5a}, size)
	copy(data, name)
	id := testHash(data)
	if err := vs.objects.local.Put(id, bytes.NewReader(data), int64(size)); err != nil {
		t.Fatalf("store object: %v", err)
	}
	return id
}

func pendingAt(id string, size int64, at time.Time) pendingRef {
	return pendingRef{ObjRef: ObjRef{ID: id, Tier: TierBase, Size: size}, At: at.UnixMilli()}
}

func setPending(t *testing.T, vs *vaultStore, id string, entries ...pendingRef) {
	t.Helper()
	m, err := vs.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}
	m.Pending = make(map[string]pendingRef, len(entries))
	for _, p := range entries {
		m.Pending[p.ID] = p
	}
	if err := vs.writeMeta(id, m); err != nil {
		t.Fatal(err)
	}
}

func patchVaultJSON(t *testing.T, vs *vaultStore, id string, patch func(doc map[string]json.RawMessage)) {
	t.Helper()
	path := filepath.Join(vs.path(id), "vault.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	patch(doc)
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

func addLegacyPending(t *testing.T, vs *vaultStore, id string, refs ...ObjRef) {
	t.Helper()
	patchVaultJSON(t, vs, id, func(doc map[string]json.RawMessage) {
		pending := map[string]json.RawMessage{}
		if raw, ok := doc["pending"]; ok && string(raw) != "null" {
			if err := json.Unmarshal(raw, &pending); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range refs {
			entry, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			pending[r.ID] = entry
		}
		raw, err := json.Marshal(pending)
		if err != nil {
			t.Fatal(err)
		}
		doc["pending"] = raw
	})
}

func readPending(t *testing.T, vs *vaultStore, id string) map[string]pendingRef {
	t.Helper()
	m, err := vs.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}
	return m.Pending
}

func baseUsage(t *testing.T, vs *vaultStore, id string) int64 {
	t.Helper()
	info, err := vs.Info(id, testVaultSecret)
	if err != nil {
		t.Fatal(err)
	}
	return info.Usage[TierBase]
}

func owns(t *testing.T, vs *vaultStore, id, objID string) bool {
	t.Helper()
	ok, err := vs.Owns(id, testVaultSecret, objID)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func liveSet(t *testing.T, vs *vaultStore) map[string]struct{} {
	t.Helper()
	live, expired, err := vs.liveObjects(time.Now().Add(-30 * 24 * time.Hour))
	if err != nil {
		t.Fatalf("liveObjects: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("vault expired unexpectedly: %v", expired)
	}
	return live
}

func TestPendingWithinGraceCountsTowardQuota(t *testing.T) {
	vs, id := newTestVault(t, 1)
	const size = 900 << 10
	obj := testObjectID("fresh upload")

	before := time.Now().UnixMilli()
	if err := vs.CommitObject(id, testVaultSecret, ObjRef{ID: obj, Tier: TierBase, Size: size}); err != nil {
		t.Fatal(err)
	}
	if at := readPending(t, vs, id)[obj].At; at < before || at > time.Now().UnixMilli() {
		t.Fatalf("CommitObject stamped at=%d, want the commit time (>= %d)", at, before)
	}
	raw, err := os.ReadFile(filepath.Join(vs.path(id), "vault.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Pending map[string]map[string]any `json:"pending"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range doc.Pending[obj] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, ","); got != "at,id,size,tier" {
		t.Fatalf("pending entry keys in vault.json = %s, want at,id,size,tier", got)
	}

	setPending(t, vs, id, pendingAt(obj, size, time.Now().Add(-testGrace+time.Minute)))
	if got := baseUsage(t, vs, id); got != size {
		t.Fatalf("base usage = %d, want %d", got, size)
	}
	if err := vs.ReserveObject(id, testVaultSecret, testObjectID("next upload"), TierBase, 200<<10); !errors.Is(err, ErrQuota) {
		t.Fatalf("reservation over quota: got %v, want ErrQuota", err)
	}
	if err := vs.ReserveObject(id, testVaultSecret, obj, TierBase, 2<<20); err != nil {
		t.Fatalf("re-reserving a pending object should be a no-op, got %v", err)
	}
	if !owns(t, vs, id, obj) {
		t.Fatal("pending object inside the grace is not owned")
	}
	if _, ok := liveSet(t, vs)[obj]; !ok {
		t.Fatal("pending object inside the grace is not live")
	}
	if _, ok := readPending(t, vs, id)[obj]; !ok {
		t.Fatal("GC pruned a pending entry inside the grace")
	}
}

func TestPendingPastGraceIsIgnored(t *testing.T) {
	vs, id := newTestVault(t, 1)
	const size = 900 << 10
	obj := storeObject(t, vs, "stale upload", size)
	setPending(t, vs, id, pendingAt(obj, size, time.Now().Add(-testGrace-time.Minute)))

	if got := baseUsage(t, vs, id); got != 0 {
		t.Fatalf("base usage = %d, want 0", got)
	}
	if err := vs.ReserveObject(id, testVaultSecret, testObjectID("next upload"), TierBase, 200<<10); err != nil {
		t.Fatalf("reservation after expiry: %v", err)
	}
	if err := vs.ReserveObject(id, testVaultSecret, obj, TierBase, 2<<20); !errors.Is(err, ErrQuota) {
		t.Fatalf("expired pending object treated as already uploaded: got %v, want ErrQuota", err)
	}
	if owns(t, vs, id, obj) {
		t.Fatal("expired pending object is still owned")
	}
	live := liveSet(t, vs)
	if _, ok := live[obj]; ok {
		t.Fatal("expired pending object is still live")
	}

	gc := &Service{lim: config.DefaultLimits()}
	if removed, _ := gc.collectObjects(vs.objects.local, live, time.Now()); removed != 0 {
		t.Fatalf("GC removed %d object(s) inside the object mtime grace", removed)
	}
	old := time.Now().Add(-testGrace - time.Hour)
	if err := os.Chtimes(vs.objects.local.path(obj), old, old); err != nil {
		t.Fatal(err)
	}
	if removed, _ := gc.collectObjects(vs.objects.local, live, time.Now()); removed != 1 {
		t.Fatalf("GC removed %d object(s), want 1", removed)
	}
	if _, err := vs.objects.local.Head(obj); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired object still on disk: %v", err)
	}
}

func TestPendingWithoutTimestampIsExpired(t *testing.T) {
	vs, id := newTestVault(t, 1)
	const size = 900 << 10
	obj := testObjectID("legacy upload")
	addLegacyPending(t, vs, id, ObjRef{ID: obj, Tier: TierBase, Size: size})

	if p, ok := readPending(t, vs, id)[obj]; !ok || p.At != 0 || p.Size != size {
		t.Fatalf("legacy entry decoded as %+v (present %v)", p, ok)
	}
	if got := baseUsage(t, vs, id); got != 0 {
		t.Fatalf("base usage = %d, want 0", got)
	}
	if err := vs.ReserveObject(id, testVaultSecret, testObjectID("next upload"), TierBase, 200<<10); err != nil {
		t.Fatalf("reservation with an untimestamped entry: %v", err)
	}
	if owns(t, vs, id, obj) {
		t.Fatal("untimestamped pending object is owned")
	}
	if _, ok := liveSet(t, vs)[obj]; ok {
		t.Fatal("untimestamped pending object is live")
	}
}

func TestGCPrunesExpiredPending(t *testing.T) {
	vs, id := newTestVault(t, 200)
	now := time.Now()
	fresh := pendingAt(testObjectID("fresh"), 1000, now.Add(-time.Hour))
	stale := pendingAt(testObjectID("stale"), 2000, now.Add(-testGrace-time.Hour))
	legacy := ObjRef{ID: testObjectID("legacy"), Tier: TierBase, Size: 3000}
	setPending(t, vs, id, fresh, stale)
	addLegacyPending(t, vs, id, legacy)
	if n := len(readPending(t, vs, id)); n != 3 {
		t.Fatalf("seeded %d pending entries, want 3", n)
	}

	live := liveSet(t, vs)
	if _, ok := live[fresh.ID]; !ok {
		t.Fatal("fresh pending object is not live")
	}
	for _, gone := range []string{stale.ID, legacy.ID} {
		if _, ok := live[gone]; ok {
			t.Fatalf("expired pending object %s is live", gone)
		}
	}
	pending := readPending(t, vs, id)
	if len(pending) != 1 {
		t.Fatalf("vault.json keeps %d pending entries after GC, want 1: %+v", len(pending), pending)
	}
	if got, ok := pending[fresh.ID]; !ok || got != fresh {
		t.Fatalf("fresh entry after GC = %+v (present %v), want %+v", got, ok, fresh)
	}

	const sentinel = int64(1234567)
	patchVaultJSON(t, vs, id, func(doc map[string]json.RawMessage) {
		doc["updated"] = json.RawMessage(fmt.Sprint(sentinel))
	})
	liveSet(t, vs)
	m, err := vs.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}
	if m.Updated != sentinel {
		t.Fatalf("GC rewrote vault.json with nothing expired (updated %d)", m.Updated)
	}
	if len(m.Pending) != 1 {
		t.Fatalf("pending entries = %d, want 1", len(m.Pending))
	}
}

func TestPutManifestClearsPendingAndEnforcesQuota(t *testing.T) {
	vs, id := newTestVault(t, 1)
	const size = 600 << 10
	x := storeObject(t, vs, "x", size)
	y := storeObject(t, vs, "y", size)
	for _, obj := range []string{x, y} {
		if err := vs.CommitObject(id, testVaultSecret, ObjRef{ID: obj, Tier: TierBase, Size: size}); err != nil {
			t.Fatal(err)
		}
	}
	ciphertext := []byte("sealed manifest")

	_, err := vs.PutManifest(id, testVaultSecret, 0, ciphertext, []ObjRef{{ID: x, Tier: TierBase}, {ID: y, Tier: TierBase}})
	if !errors.Is(err, ErrQuota) {
		t.Fatalf("manifest over quota: got %v, want ErrQuota", err)
	}
	m, err := vs.readMeta(id)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 0 || len(m.Pending) != 2 {
		t.Fatalf("refused manifest changed the vault: version %d, %d pending", m.Version, len(m.Pending))
	}

	ver, err := vs.PutManifest(id, testVaultSecret, 0, ciphertext, []ObjRef{{ID: x, Tier: TierBase}})
	if err != nil || ver != 1 {
		t.Fatalf("manifest within quota: version %d, err %v", ver, err)
	}
	if m, err = vs.readMeta(id); err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 || len(m.Pending) != 0 {
		t.Fatalf("after manifest: version %d, %d pending, want 1 and 0", m.Version, len(m.Pending))
	}
	refsRaw, err := os.ReadFile(filepath.Join(vs.path(id), "manifest.1.refs"))
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf(`[{"id":%q,"tier":"base","size":%d}]`, x, size); string(refsRaw) != want {
		t.Fatalf("manifest refs = %s, want %s", refsRaw, want)
	}
	if got, want := baseUsage(t, vs, id), int64(size+len(ciphertext)); got != want {
		t.Fatalf("base usage = %d, want %d", got, want)
	}
}

func TestExpiredPendingBacklogNoLongerLocksVault(t *testing.T) {
	const (
		backlog = 300
		each    = 800 << 10
	)
	refs := make([]ObjRef, backlog)
	for i := range refs {
		refs[i] = ObjRef{ID: testObjectID(fmt.Sprintf("generation-%d", i)), Tier: TierBase, Size: each}
	}
	stamped := func(at time.Time) []pendingRef {
		out := make([]pendingRef, len(refs))
		for i, r := range refs {
			out[i] = pendingAt(r.ID, r.Size, at)
		}
		return out
	}

	cases := []struct {
		name string
		seed func(t *testing.T, vs *vaultStore, id string)
	}{
		{"past grace", func(t *testing.T, vs *vaultStore, id string) {
			setPending(t, vs, id, stamped(time.Now().Add(-testGrace-time.Hour))...)
		}},
		{"untimestamped", func(t *testing.T, vs *vaultStore, id string) {
			setPending(t, vs, id)
			addLegacyPending(t, vs, id, refs...)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vs, id := newTestVault(t, 200)
			small := []byte("a small object uploaded after the app restarted")
			smallID := testHash(small)

			setPending(t, vs, id, stamped(time.Now())...)
			if err := vs.ReserveObject(id, testVaultSecret, smallID, TierBase, int64(len(small))); !errors.Is(err, ErrQuota) {
				t.Fatalf("backlog inside the grace: got %v, want ErrQuota", err)
			}

			tc.seed(t, vs, id)
			m, err := vs.readMeta(id)
			if err != nil {
				t.Fatal(err)
			}
			if m.Version != 0 || len(m.Pending) != backlog {
				t.Fatalf("seeded version %d with %d pending, want 0 and %d", m.Version, len(m.Pending), backlog)
			}

			if err := vs.ReserveObject(id, testVaultSecret, smallID, TierBase, int64(len(small))); err != nil {
				t.Fatalf("reservation with an expired backlog: %v", err)
			}
			res, err := vs.objects.Append(id, smallID, TierBase, 0, int64(len(small)), bytes.NewReader(small))
			if err != nil || !res.Complete {
				t.Fatalf("upload: %+v, %v", res, err)
			}
			if err := vs.CommitObject(id, testVaultSecret, ObjRef{ID: smallID, Tier: TierBase, Size: int64(len(small))}); err != nil {
				t.Fatal(err)
			}
			ciphertext := []byte("sealed manifest after the restart")
			ver, err := vs.PutManifest(id, testVaultSecret, 0, ciphertext, []ObjRef{{ID: smallID, Tier: TierBase}})
			if err != nil || ver != 1 {
				t.Fatalf("manifest with an expired backlog: version %d, err %v", ver, err)
			}
			if n := len(readPending(t, vs, id)); n != 0 {
				t.Fatalf("%d pending entries left after the manifest", n)
			}
			if got, want := baseUsage(t, vs, id), int64(len(small)+len(ciphertext)); got != want {
				t.Fatalf("base usage = %d, want %d", got, want)
			}
		})
	}
}
