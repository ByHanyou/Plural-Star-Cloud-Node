// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/subtle"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAuth(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !originAllowed(r) {
			writeError(w, http.StatusForbidden, "cross-site requests are not accepted")
			return
		}
		next(w, r)
	}
}

// originAllowed rejects cross-site requests from a browser page; the apps send
// no Origin header or a non-http one.
func originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || origin == "null" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	switch u.Scheme {
	case "http", "https", "ws", "wss":
	default:
		return true
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

// operatorOnly guards the endpoints that read or rewrite the node's own
// configuration: the bearer token when api_token is set, otherwise loopback
// only, and any body must be declared as JSON.
func (s *Server) operatorOnly(w http.ResponseWriter, r *http.Request, requireJSON bool) bool {
	if requireJSON {
		ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if ct != "application/json" {
			writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
			return false
		}
	}
	if s.cfg.APIToken != "" {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		writeError(w, http.StatusForbidden, "only the local machine may call this endpoint on a node without api_token")
		return false
	}
	return true
}
