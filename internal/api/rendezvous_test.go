// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func TestRendezvousStorePersistsAndReloads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "rendezvous.json")
	rs := newRendezvousStore(ctx, path, 20000)
	if !rs.put("psf:alice", "rec-alice", time.Hour) {
		t.Fatal("local put refused")
	}
	rs.PutRemote("psf:bob", "rec-bob", time.Hour)
	rs.PutRemote("psf:gone", "rec-gone", 20*time.Millisecond)
	waitUntil(t, "store persisted", func() bool {
		_, err := os.Stat(path)
		return err == nil
	})
	time.Sleep(50 * time.Millisecond)

	again := newRendezvousStore(ctx, path, 20000)
	if rec, ok := again.get("psf:alice"); !ok || rec != "rec-alice" {
		t.Fatalf("local record not reloaded: %q %v", rec, ok)
	}
	if rec, ok := again.get("psf:bob"); !ok || rec != "rec-bob" {
		t.Fatalf("remote record not reloaded: %q %v", rec, ok)
	}
	if _, ok := again.get("psf:gone"); ok {
		t.Fatal("expired record reloaded")
	}
	locals := again.localEntries()
	if len(locals) != 1 || locals[0].Namespace != "psf:alice" || locals[0].Record != "rec-alice" {
		t.Fatalf("local entries %+v", locals)
	}
}

func TestRendezvousLocalRecordsWinOverGossip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := newRendezvousStore(ctx, "", 20000)
	rs.put("psf:alice", "mine", time.Hour)
	rs.PutRemote("psf:alice", "theirs", 2*time.Hour)
	if rec, _ := rs.get("psf:alice"); rec != "mine" {
		t.Fatalf("gossip replaced a local record: %q", rec)
	}
	rs.PutRemote("psf:carol", "old", time.Hour)
	rs.PutRemote("psf:carol", "older-expiry", 30*time.Minute)
	if rec, _ := rs.get("psf:carol"); rec != "old" {
		t.Fatalf("a shorter-lived gossip record replaced a longer one: %q", rec)
	}
	rs.PutRemote("psf:carol", "newer", 2*time.Hour)
	if rec, _ := rs.get("psf:carol"); rec != "newer" {
		t.Fatalf("a longer-lived gossip record was refused: %q", rec)
	}
	rs.put("psf:carol", "mine-now", time.Hour)
	if rec, _ := rs.get("psf:carol"); rec != "mine-now" {
		t.Fatalf("local put did not replace the gossip record: %q", rec)
	}
}

func TestRendezvousHandlers(t *testing.T) {
	s, _, _ := newTestServer(t, &config.Config{})
	post := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleRendezvousRegister(rec, httptest.NewRequest(http.MethodPost, "/rendezvous/register", strings.NewReader(body)))
		return rec
	}
	if rec := post(`{"namespace":"psf:x","record":"","ttl_seconds":60}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty record: status %d", rec.Code)
	}
	if rec := post(`{"namespace":"psf:x","record":"` + strings.Repeat("a", config.DefaultLimits().RendezvousMaxRecordBytes+1) + `","ttl_seconds":60}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized record: status %d", rec.Code)
	}
	if rec := post(`{"namespace":"` + strings.Repeat("n", config.DefaultLimits().RendezvousMaxNamespaceLen+1) + `","record":"abc","ttl_seconds":60}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized namespace: status %d", rec.Code)
	}
	if rec := post(`{"namespace":"psf:x","record":"abc","ttl_seconds":60}`); rec.Code != http.StatusOK {
		t.Fatalf("register: status %d body %s", rec.Code, rec.Body.String())
	}
	rec := httptest.NewRecorder()
	s.handleRendezvousLookup(rec, httptest.NewRequest(http.MethodGet, "/rendezvous/lookup?namespace=psf:x", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("lookup: status %d", rec.Code)
	}
	var out map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out["record"] != "abc" {
		t.Fatalf("lookup body %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleRendezvousLookup(rec, httptest.NewRequest(http.MethodGet, "/rendezvous/lookup?namespace=psf:none", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown lookup: status %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.handleRendezvousLookup(rec, httptest.NewRequest(http.MethodGet, "/rendezvous/lookup", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing namespace: status %d", rec.Code)
	}
}
