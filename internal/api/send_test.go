// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/libp2p/go-libp2p/core/test"
)

func postSend(t *testing.T, s *Server, session, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body))
	req.Header.Set("X-Relay-Session", session)
	s.handleSend(rec, req)
	out := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %q", rec.Body.String())
	}
	return rec, out
}

func TestNewRequiresDeps(t *testing.T) {
	if _, err := New(&config.Config{}, "", nil, Deps{}); err == nil {
		t.Fatal("New accepted a nil host")
	}
}

func TestHandleSendOutcomes(t *testing.T) {
	s, fr, _ := newTestServer(t, &config.Config{})
	ts := httptest.NewServer(s.routes())
	defer ts.Close()
	app, key := appIdentity(t)
	sender := app.String()
	socket, session := connectApp(t, "ws"+strings.TrimPrefix(ts.URL, "http")+"/ws?peer_id="+sender, key)
	recipient := test.RandPeerIDFatal(t).String()
	payload := base64.StdEncoding.EncodeToString([]byte("sealed"))
	body := func(extra string) string {
		return `{"sender_peer_id":"` + sender + `","recipient_peer_id":"` + recipient + `","payload":"` + payload + `"` + extra + `}`
	}

	for _, outcome := range []relay.Outcome{relay.OutcomeDelivered, relay.OutcomeDuplicate, relay.OutcomeForwarded, relay.OutcomeHeld} {
		fr.outcome = outcome
		rec, out := postSend(t, s, session, body(""))
		if rec.Code != http.StatusAccepted || out["status"] != "queued" {
			t.Fatalf("%s: status %d body %v", outcome, rec.Code, out)
		}
		if id, _ := out["packet_id"].(string); len(id) != 32 {
			t.Fatalf("%s: packet_id %q", outcome, id)
		}
	}

	fr.outcome = relay.OutcomeDelivered
	id := strings.Repeat("ab", 16)
	rec, out := postSend(t, s, session, body(`,"packet_id":"`+id+`","ephemeral":true`))
	if rec.Code != http.StatusAccepted || out["packet_id"] != id {
		t.Fatalf("explicit packet_id not echoed: %d %v", rec.Code, out)
	}
	last := fr.routed[len(fr.routed)-1]
	if hex.EncodeToString(last.ID[:]) != id || string(last.Payload) != "sealed" || !last.Ephemeral {
		t.Fatalf("routed packet mismatch: id=%x payload=%q ephemeral=%v", last.ID, last.Payload, last.Ephemeral)
	}
	if last.Timestamp <= 0 || time.Since(time.UnixMilli(last.Timestamp)) > time.Minute {
		t.Fatalf("routed packet timestamp %d", last.Timestamp)
	}

	fr.err = errors.New("boom")
	rec, _ = postSend(t, s, session, body(""))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("route error: status %d, want 500", rec.Code)
	}
	fr.err = nil

	bad := []string{
		`{"recipient_peer_id":"` + recipient + `","payload":"` + payload + `"}`,
		`{"sender_peer_id":"nope","recipient_peer_id":"` + recipient + `","payload":"` + payload + `"}`,
		`{"sender_peer_id":"` + sender + `","recipient_peer_id":"nope","payload":"` + payload + `"}`,
		`{"sender_peer_id":"` + sender + `","recipient_peer_id":"` + recipient + `","payload":"@@@"}`,
		body(`,"packet_id":"short"`),
		`not json`,
	}
	for _, b := range bad {
		rec, _ := postSend(t, s, session, b)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status %d, want 400", b, rec.Code)
		}
	}
	rec, out = postSend(t, s, session, body(`,"pad":"`+strings.Repeat("a", config.DefaultLimits().MaxSendBodyBytes)+`"`))
	if rec.Code != http.StatusRequestEntityTooLarge || out["code"] != "TOO_LARGE" {
		t.Fatalf("oversized body: status %d body %v, want 413", rec.Code, out)
	}
	rec = httptest.NewRecorder()
	s.handleSend(rec, httptest.NewRequest(http.MethodGet, "/send", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /send: status %d", rec.Code)
	}

	routed := len(fr.routed)
	rec, _ = postSend(t, s, "", body(""))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing session: status %d, want 401", rec.Code)
	}
	rec, _ = postSend(t, s, session, strings.Replace(body(""), sender, recipient, 1))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong sender: status %d, want 401", rec.Code)
	}
	_ = socket.Close()
	waitUntil(t, "session removed", func() bool { return s.hub.count() == 0 })
	rec, _ = postSend(t, s, session, body(""))
	if rec.Code != http.StatusUnauthorized || len(fr.routed) != routed {
		t.Fatalf("expired session: status %d, routed packets %d", rec.Code, len(fr.routed))
	}
}

func TestHealthAndRemovedPeersEndpoint(t *testing.T) {
	s, _, _ := newTestServer(t, &config.Config{NetworkMode: config.ModePublic})
	rec := httptest.NewRecorder()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status %d", rec.Code)
	}
	var health map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if health["status"] != "ok" || health["peer_id"] != s.h.ID().String() || health["connected_nodes"] != float64(3) || health["connected_apps"] != float64(0) || health["network_id"] != "plural-star-global" {
		t.Fatalf("health body %v", health)
	}

	rec = httptest.NewRecorder()
	s.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/peers", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("peers status %d, want 404", rec.Code)
	}
}
