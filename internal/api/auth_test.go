// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func TestCheckAuth(t *testing.T) {
	open := &Server{cfg: &config.Config{}}
	if !open.checkAuth(httptest.NewRequest(http.MethodGet, "/health", nil)) {
		t.Fatal("open node refused a request without a token")
	}
	s := &Server{cfg: &config.Config{APIToken: "secret"}}
	if s.checkAuth(httptest.NewRequest(http.MethodGet, "/health", nil)) {
		t.Fatal("missing token accepted")
	}
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.Header.Set("Authorization", "Bearer secret")
	if !s.checkAuth(r) {
		t.Fatal("bearer token refused")
	}
	r.Header.Set("Authorization", "Bearer wrong")
	if s.checkAuth(r) {
		t.Fatal("wrong bearer token accepted")
	}
	if !s.checkAuth(httptest.NewRequest(http.MethodGet, "/ws?token=secret", nil)) {
		t.Fatal("query token refused")
	}
	if s.checkAuth(httptest.NewRequest(http.MethodGet, "/ws?token=wrong", nil)) {
		t.Fatal("wrong query token accepted")
	}
}

func TestOriginPolicy(t *testing.T) {
	cases := []struct {
		origin, host string
		allowed      bool
	}{
		{"", "node.example:8080", true},
		{"null", "node.example:8080", true},
		{"capacitor://localhost", "node.example:8080", true},
		{"http://localhost:5173", "node.example:8080", true},
		{"http://127.0.0.1:5173", "node.example:8080", true},
		{"http://[::1]:5173", "node.example:8080", true},
		{"https://node.example", "node.example:8080", true},
		{"https://NODE.example", "node.example", true},
		{"https://evil.example", "node.example:8080", false},
		{"https://node.example.evil", "node.example:8080", false},
		{"http://", "node.example:8080", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/health", nil)
		r.Host = c.host
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if got := originAllowed(r); got != c.allowed {
			t.Errorf("origin %q host %q: allowed=%v want %v", c.origin, c.host, got, c.allowed)
		}
	}
}

func TestAuthedMiddlewareOrder(t *testing.T) {
	s := &Server{cfg: &config.Config{APIToken: "secret"}}
	h := s.authed(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", rec.Code)
	}
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Origin", "https://evil.example")
	rec = httptest.NewRecorder()
	h(rec, r)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site: status %d, want 403", rec.Code)
	}
	r.Header.Del("Origin")
	rec = httptest.NewRecorder()
	h(rec, r)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("authorised: status %d, want 204", rec.Code)
	}
}

func TestOperatorOnly(t *testing.T) {
	open := &Server{cfg: &config.Config{}}
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/config", nil)
	r.RemoteAddr = "203.0.113.9:4444"
	if open.operatorOnly(rec, r, false) || rec.Code != http.StatusForbidden {
		t.Fatalf("remote caller allowed on an open node: status %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	r.RemoteAddr = "127.0.0.1:4444"
	if !open.operatorOnly(rec, r, false) {
		t.Fatal("loopback caller refused on an open node")
	}
	rec = httptest.NewRecorder()
	r.Header.Set("Content-Type", "text/plain")
	if open.operatorOnly(rec, r, true) || rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("non-JSON body accepted: status %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	if !open.operatorOnly(rec, r, true) {
		t.Fatal("JSON body refused")
	}
	tokened := &Server{cfg: &config.Config{APIToken: "secret"}}
	rec = httptest.NewRecorder()
	r.RemoteAddr = "203.0.113.9:4444"
	if !tokened.operatorOnly(rec, r, false) {
		t.Fatal("remote caller refused on a node with api_token")
	}
	rec = httptest.NewRecorder()
	r.Header.Set("Origin", "https://evil.example")
	if tokened.operatorOnly(rec, r, false) || rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site operator call accepted: status %d", rec.Code)
	}
}
