// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAuth(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Server) checkAuth(r *http.Request) bool {
	if s.cfg.APIToken == "" {
		return true
	}
	want := []byte(s.cfg.APIToken)

	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		got := []byte(strings.TrimPrefix(h, "Bearer "))
		if subtle.ConstantTimeCompare(got, want) == 1 {
			return true
		}
	}
	if q := r.URL.Query().Get("token"); q != "" {
		if subtle.ConstantTimeCompare([]byte(q), want) == 1 {
			return true
		}
	}
	return false
}
