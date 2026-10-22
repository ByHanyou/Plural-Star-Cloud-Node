// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func newTestService(t *testing.T) (*Service, http.Handler) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.CloudConfig{
		Enabled:          true,
		StoragePath:      filepath.Join(dir, "cloud"),
		MediaBackend:     config.MediaBackendLocal,
		WatermarkPercent: 99,
		BaseQuotaMB:      4,
		MediaQuotaMB:     4,
		ObjectMaxMB:      1,
		ManifestVersions: 3,
		InboxTTLDays:     7,
		InboxQuotaMB:     4,
		GraceDays:        30,
		GCHourUTC:        3,
		PushBoxKeyPath:   filepath.Join(dir, "push.key"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc, err := New(ctx, cfg, config.DefaultLimits(), dir)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if svc.atWatermark() {
		t.Skip("the test disk is above the configured watermark")
	}
	mux := http.NewServeMux()
	svc.Register(mux, func(h http.HandlerFunc) http.HandlerFunc { return h })
	return svc, mux
}

type call struct {
	method  string
	path    string
	body    []byte
	headers map[string]string
}

func do(t *testing.T, h http.Handler, c call) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var body io.Reader
	if c.body != nil {
		body = bytes.NewReader(c.body)
	}
	req := httptest.NewRequest(c.method, c.path, body)
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

func jsonBody(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func merged(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

type testVault struct {
	lookup, secret, subID, key string
	creds                      map[string]string
	device                     map[string]string
}

func setupVault(t *testing.T, h http.Handler, name string) testVault {
	t.Helper()
	v := testVault{lookup: testHash([]byte(name)), secret: strings.Repeat("5e", 32), subID: "phone-1"}
	v.creds = map[string]string{headerVaultID: v.lookup, headerVaultAuth: v.secret}
	rec, _ := do(t, h, call{"POST", "/cloud/vault/create", jsonBody(t, map[string]string{
		"lookup_id": v.lookup, "auth_secret": v.secret, "wrapped_master_key": base64.StdEncoding.EncodeToString([]byte("wrapped")),
	}), nil})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status %d body %s", rec.Code, rec.Body.String())
	}
	rec, out := do(t, h, call{"POST", "/cloud/vault/devices", jsonBody(t, map[string]string{
		"lookup_id": v.lookup, "auth_secret": v.secret, "sub_id": v.subID, "label": "Phone",
	}), nil})
	if rec.Code != http.StatusOK {
		t.Fatalf("link device: status %d body %s", rec.Code, rec.Body.String())
	}
	v.key, _ = out["device_key"].(string)
	if !validHex32(v.key) || out["existed"] != false {
		t.Fatalf("link device body %v", out)
	}
	v.device = merged(v.creds, map[string]string{headerDeviceID: v.subID, headerDeviceKey: v.key})
	return v
}

func TestVaultLifecycleOverHTTP(t *testing.T) {
	_, h := newTestService(t)
	lookup := testHash([]byte("vault-one"))
	secret := strings.Repeat("5e", 32)

	rec, out := do(t, h, call{"POST", "/cloud/vault/lookup", jsonBody(t, map[string]string{"lookup_id": lookup}), nil})
	if rec.Code != http.StatusOK || out["exists"] != false {
		t.Fatalf("lookup before create: %d %v", rec.Code, out)
	}
	rec, _ = do(t, h, call{"POST", "/cloud/vault/lookup", []byte(`{"lookup_id":"short"}`), nil})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad lookup id: status %d", rec.Code)
	}

	v := setupVault(t, h, "vault-one")
	rec, out = do(t, h, call{"POST", "/cloud/vault/create", jsonBody(t, map[string]string{
		"lookup_id": lookup, "auth_secret": secret, "wrapped_master_key": base64.StdEncoding.EncodeToString([]byte("wrapped")),
	}), nil})
	if rec.Code != http.StatusConflict || out["code"] != CodeVaultExists {
		t.Fatalf("duplicate create: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"POST", "/cloud/vault/lookup", jsonBody(t, map[string]string{"lookup_id": lookup}), nil})
	if rec.Code != http.StatusOK || out["exists"] != true {
		t.Fatalf("lookup after create: %d %v", rec.Code, out)
	}

	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, v.creds})
	if rec.Code != http.StatusUnauthorized || out["code"] != CodeDeviceKeyRequired {
		t.Fatalf("info without device: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, merged(v.device, map[string]string{headerVaultAuth: strings.Repeat("cd", 32)})})
	if rec.Code != http.StatusUnauthorized || out["code"] != CodeBadAuth {
		t.Fatalf("info with wrong secret: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, nil})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("info without credentials: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, merged(v.device, map[string]string{headerDeviceKey: strings.Repeat("00", 32)})})
	if rec.Code != http.StatusUnauthorized || out["code"] != CodeDeviceUnlinked {
		t.Fatalf("info with wrong device key: %d %v", rec.Code, out)
	}

	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, v.device})
	if rec.Code != http.StatusOK || out["version"] != float64(0) || out["wrapped_master_key"] != base64.StdEncoding.EncodeToString([]byte("wrapped")) {
		t.Fatalf("info: %d %v", rec.Code, out)
	}
	devices, _ := out["devices"].([]any)
	if len(devices) != 1 {
		t.Fatalf("devices %v", out["devices"])
	}
	quota, _ := out["quota"].(map[string]any)
	if quota[TierBase] != float64(4<<20) || quota[TierMedia] != float64(4<<20) {
		t.Fatalf("quota %v", out["quota"])
	}

	rec, out = do(t, h, call{"POST", "/cloud/vault/devices/" + v.subID + "/key", jsonBody(t, map[string]string{"lookup_id": lookup, "auth_secret": secret}), nil})
	if rec.Code != http.StatusConflict || out["code"] != CodeDeviceKeyIssued {
		t.Fatalf("second key for a linked device: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"POST", "/cloud/vault/devices/ghost/key", jsonBody(t, map[string]string{"lookup_id": lookup, "auth_secret": secret}), nil})
	if rec.Code != http.StatusUnauthorized || out["code"] != CodeDeviceUnlinked {
		t.Fatalf("key for an unknown device: %d %v", rec.Code, out)
	}

	rec, out = do(t, h, call{"DELETE", "/cloud/vault/devices/" + v.subID, nil, v.device})
	if rec.Code != http.StatusOK || out["grace_days"] != float64(30) {
		t.Fatalf("unlink: %d %v", rec.Code, out)
	}
	if left, _ := out["devices"].([]any); len(left) != 0 {
		t.Fatalf("devices after unlink %v", out["devices"])
	}
	rec, out = do(t, h, call{"GET", "/cloud/vault/info", nil, v.device})
	if rec.Code != http.StatusUnauthorized || out["code"] != CodeDeviceUnlinked {
		t.Fatalf("info after unlink: %d %v", rec.Code, out)
	}
}

func TestLookupRateLimited(t *testing.T) {
	s, h := newTestService(t)
	body := jsonBody(t, map[string]string{"lookup_id": testHash([]byte("nobody"))})
	for i := 0; i < s.lim.RateLimitPerAddress; i++ {
		if rec, _ := do(t, h, call{"POST", "/cloud/vault/lookup", body, nil}); rec.Code != http.StatusOK {
			t.Fatalf("lookup %d: status %d", i, rec.Code)
		}
	}
	rec, out := do(t, h, call{"POST", "/cloud/vault/lookup", body, nil})
	if rec.Code != http.StatusTooManyRequests || out["code"] != CodeRateLimited {
		t.Fatalf("limited lookup: %d %v", rec.Code, out)
	}
	if ra, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || ra <= 0 {
		t.Fatalf("Retry-After header %q", rec.Header().Get("Retry-After"))
	}
	if secs, _ := out["retry_after_seconds"].(float64); secs <= 0 {
		t.Fatalf("retry_after_seconds %v", out["retry_after_seconds"])
	}
}

func TestObjectUploadResumeAndManifest(t *testing.T) {
	_, h := newTestService(t)
	v := setupVault(t, h, "vault-two")
	data := make([]byte, 300000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	id := testHash(data)
	put := func(objID string, start, end, total int, chunk []byte) (*httptest.ResponseRecorder, map[string]any) {
		return do(t, h, call{"PUT", "/cloud/vault/objects/" + objID, chunk, merged(v.device, map[string]string{
			"Content-Range": fmt.Sprintf("bytes %d-%d/%d", start, end, total),
			headerTier:      TierBase,
		})})
	}

	rec, out := put(id, 0, 99999, len(data), data[:100000])
	if rec.Code != http.StatusAccepted || out["complete"] != false || out["received"] != float64(100000) || rec.Header().Get(headerUploadOffs) != "100000" {
		t.Fatalf("first chunk: %d %v offset=%q", rec.Code, out, rec.Header().Get(headerUploadOffs))
	}
	rec, _ = do(t, h, call{"HEAD", "/cloud/vault/objects/" + id, nil, v.device})
	if rec.Code != http.StatusNotFound || rec.Header().Get(headerUploadOffs) != "100000" {
		t.Fatalf("head mid-upload: %d offset=%q", rec.Code, rec.Header().Get(headerUploadOffs))
	}
	rec, out = put(id, 50000, 149999, len(data), data[50000:150000])
	if rec.Code != http.StatusConflict || out["code"] != CodeUploadOffsetMismatch || out["received"] != float64(100000) || rec.Header().Get(headerUploadOffs) != "100000" {
		t.Fatalf("wrong offset: %d %v", rec.Code, out)
	}
	rec, out = put(id, 100000, 299999, len(data), data[100000:])
	if rec.Code != http.StatusCreated || out["complete"] != true || out["size"] != float64(300000) {
		t.Fatalf("final chunk: %d %v", rec.Code, out)
	}
	rec, _ = do(t, h, call{"HEAD", "/cloud/vault/objects/" + id, nil, v.device})
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Length") != "300000" {
		t.Fatalf("head complete: %d length=%q", rec.Code, rec.Header().Get("Content-Length"))
	}
	rec, _ = do(t, h, call{"GET", "/cloud/vault/objects/" + id, nil, v.device})
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("get: %d (%d bytes)", rec.Code, rec.Body.Len())
	}
	rec, out = put(id, 0, 299999, len(data), data)
	if rec.Code != http.StatusOK || out["exists"] != true {
		t.Fatalf("re-upload of a stored object: %d %v", rec.Code, out)
	}

	rec, out = do(t, h, call{"PUT", "/cloud/vault/manifest", jsonBody(t, map[string]any{
		"expected_version": 0, "ciphertext": []byte("manifest-1"), "objects": []ObjRef{{ID: id, Tier: TierBase, Size: int64(len(data))}},
	}), v.device})
	if rec.Code != http.StatusOK || out["version"] != float64(1) {
		t.Fatalf("manifest put: %d %v", rec.Code, out)
	}
	rec, out = do(t, h, call{"GET", "/cloud/vault/manifest", nil, v.device})
	if rec.Code != http.StatusOK || out["version"] != float64(1) || out["ciphertext"] != base64.StdEncoding.EncodeToString([]byte("manifest-1")) {
		t.Fatalf("manifest get: %d %v", rec.Code, out)
	}
	objs, _ := out["objects"].([]any)
	first, _ := objs[0].(map[string]any)
	if len(objs) != 1 || first["id"] != id || first["size"] != float64(len(data)) {
		t.Fatalf("manifest objects %v", out["objects"])
	}
	rec, out = do(t, h, call{"PUT", "/cloud/vault/manifest", jsonBody(t, map[string]any{
		"expected_version": 0, "ciphertext": []byte("manifest-stale"), "objects": []ObjRef{},
	}), v.device})
	if rec.Code != http.StatusConflict || out["code"] != CodeManifestVersionConflict || out["version"] != float64(1) {
		t.Fatalf("stale manifest put: %d %v", rec.Code, out)
	}
	missing := testHash([]byte("never uploaded"))
	rec, out = do(t, h, call{"PUT", "/cloud/vault/manifest", jsonBody(t, map[string]any{
		"expected_version": 1, "ciphertext": []byte("manifest-2"), "objects": []ObjRef{{ID: missing, Tier: TierBase}},
	}), v.device})
	if rec.Code != http.StatusBadRequest || out["code"] != CodeManifestMissingObjects {
		t.Fatalf("manifest with a missing object: %d %v", rec.Code, out)
	}

	other := make([]byte, 1000)
	if _, err := rand.Read(other); err != nil {
		t.Fatal(err)
	}
	rec, out = put(testHash([]byte("wrong id")), 0, 999, 1000, other)
	if rec.Code != http.StatusBadRequest || out["code"] != CodeHashMismatch {
		t.Fatalf("hash mismatch: %d %v", rec.Code, out)
	}
	rec, out = put(testHash(other), 0, 999, 2<<20, other)
	if rec.Code != http.StatusRequestEntityTooLarge || out["code"] != CodeObjectTooLarge {
		t.Fatalf("too large: %d %v", rec.Code, out)
	}
	rec, _ = do(t, h, call{"PUT", "/cloud/vault/objects/not-hex", other, v.device})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad object id: %d", rec.Code)
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		header        string
		contentLength int64
		start, total  int64
		ok            bool
	}{
		{"", 100, 0, 100, true},
		{"", 0, 0, 0, false},
		{"bytes 0-99/300", 100, 0, 300, true},
		{"bytes 100-199/300", 100, 100, 300, true},
		{"bytes 100-199/300", -1, 100, 300, true},
		{"bytes 0-99/300", 50, 0, 0, false},
		{"bytes 5-4/10", -1, 0, 0, false},
		{"bytes 0-10/10", 11, 0, 0, false},
		{"0-9/10", 10, 0, 0, false},
		{"bytes x-9/10", 10, 0, 0, false},
	}
	for _, c := range cases {
		start, total, err := parseContentRange(c.header, c.contentLength)
		if (err == nil) != c.ok {
			t.Errorf("%q/%d: err=%v want ok=%v", c.header, c.contentLength, err, c.ok)
			continue
		}
		if c.ok && (start != c.start || total != c.total) {
			t.Errorf("%q/%d: start=%d total=%d want %d/%d", c.header, c.contentLength, start, total, c.start, c.total)
		}
	}
}

func TestObjectPutDoesNotClaimAnotherVaultsObject(t *testing.T) {
	_, h := newTestService(t)
	owner := setupVault(t, h, "vault-owner")
	other := setupVault(t, h, "vault-other")
	data := make([]byte, 2000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	id := testHash(data)
	rec, out := do(t, h, call{"PUT", "/cloud/vault/objects/" + id, data, merged(owner.device, map[string]string{
		"Content-Range": fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)),
		headerTier:      TierBase,
	})})
	if rec.Code != http.StatusCreated || out["complete"] != true {
		t.Fatalf("owner upload: %d %v", rec.Code, out)
	}
	wrong := make([]byte, 2000)
	rec, out = do(t, h, call{"PUT", "/cloud/vault/objects/" + id, wrong, merged(other.device, map[string]string{
		"Content-Range": fmt.Sprintf("bytes 0-%d/%d", len(wrong)-1, len(wrong)),
		headerTier:      TierBase,
	})})
	if rec.Code != http.StatusBadRequest || out["code"] != CodeHashMismatch {
		t.Fatalf("claim without the bytes: %d %v, want 400 %s", rec.Code, out, CodeHashMismatch)
	}
	rec, _ = do(t, h, call{"GET", "/cloud/vault/objects/" + id, nil, other.device})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("other vault can read the object: %d", rec.Code)
	}
	rec, out = do(t, h, call{"PUT", "/cloud/vault/objects/" + id, data, merged(other.device, map[string]string{
		"Content-Range": fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)),
		headerTier:      TierBase,
	})})
	if rec.Code != http.StatusCreated || out["complete"] != true {
		t.Fatalf("other vault upload with the bytes: %d %v", rec.Code, out)
	}
	rec, _ = do(t, h, call{"GET", "/cloud/vault/objects/" + id, nil, other.device})
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("other vault get after proving the bytes: %d", rec.Code)
	}
}
