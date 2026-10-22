// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func TestReadEndpointsRejectOtherMethods(t *testing.T) {
	s, _, _ := newTestServer(t, &config.Config{APIToken: "secret", NetworkMode: config.ModePrivate})
	handlers := map[string]http.HandlerFunc{
		"/health":   s.handleHealth,
		"/nodes":    s.handleNodes,
		"/peers":    s.handlePeers,
		"/networks": s.handleNetworks,
		"/config":   s.handleConfig,
	}
	for path, h := range handlers {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, path, nil)
		r.Header.Set("Authorization", "Bearer secret")
		h(rec, r)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s: status %d, want 405", path, rec.Code)
		}
		rec = httptest.NewRecorder()
		r = httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer secret")
		h(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: status %d body %s", path, rec.Code, rec.Body.String())
		}
	}
}
