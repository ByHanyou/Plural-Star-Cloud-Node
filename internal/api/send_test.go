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

func postSend(t *testing.T, s *Server, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleSend(rec, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body)))
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
	sender := test.RandPeerIDFatal(t).String()
	recipient := test.RandPeerIDFatal(t).String()
	payload := base64.StdEncoding.EncodeToString([]byte("sealed"))
	body := func(extra string) string {
		return `{"sender_peer_id":"` + sender + `","recipient_peer_id":"` + recipient + `","payload":"` + payload + `"` + extra + `}`
	}

	for _, c := range []struct {
		outcome relay.Outcome
		status  int
	}{
		{relay.OutcomeDelivered, http.StatusOK},
		{relay.OutcomeDuplicate, http.StatusOK},
		{relay.OutcomeForwarded, http.StatusAccepted},
		{relay.OutcomeHeld, http.StatusAccepted},
	} {
		fr.outcome = c.outcome
		rec, out := postSend(t, s, body(""))
		if rec.Code != c.status || out["status"] != string(c.outcome) {
			t.Fatalf("%s: status %d body %v", c.outcome, rec.Code, out)
		}
		if id, _ := out["packet_id"].(string); len(id) != 32 {
			t.Fatalf("%s: packet_id %q", c.outcome, id)
		}
	}

	fr.outcome = relay.OutcomeDelivered
	id := strings.Repeat("ab", 16)
	rec, out := postSend(t, s, body(`,"packet_id":"`+id+`"`))
	if rec.Code != http.StatusOK || out["packet_id"] != id {
		t.Fatalf("explicit packet_id not echoed: %d %v", rec.Code, out)
	}
	last := fr.routed[len(fr.routed)-1]
	if hex.EncodeToString(last.ID[:]) != id || string(last.Payload) != "sealed" {
		t.Fatalf("routed packet mismatch: id=%x payload=%q", last.ID, last.Payload)
	}
	if last.Timestamp <= 0 || time.Since(time.UnixMilli(last.Timestamp)) > time.Minute {
		t.Fatalf("routed packet timestamp %d", last.Timestamp)
	}

	fr.err = errors.New("boom")
	rec, _ = postSend(t, s, body(""))
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
		rec, _ := postSend(t, s, b)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status %d, want 400", b, rec.Code)
		}
	}
	rec, out = postSend(t, s, body(`,"pad":"`+strings.Repeat("a", config.DefaultLimits().MaxSendBodyBytes)+`"`))
	if rec.Code != http.StatusRequestEntityTooLarge || out["code"] != "TOO_LARGE" {
		t.Fatalf("oversized body: status %d body %v, want 413", rec.Code, out)
	}
	rec = httptest.NewRecorder()
	s.handleSend(rec, httptest.NewRequest(http.MethodGet, "/send", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /send: status %d", rec.Code)
	}
}

func TestHealthAndPeers(t *testing.T) {
	s, fr, _ := newTestServer(t, &config.Config{NetworkMode: config.ModePublic})
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

	app := test.RandPeerIDFatal(t)
	via := test.RandPeerIDFatal(t)
	fr.router.Upsert(app, via, time.Hour)
	rec = httptest.NewRecorder()
	s.handlePeers(rec, httptest.NewRequest(http.MethodGet, "/peers", nil))
	var peers []map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &peers); err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0]["peer_id"] != app.String() || peers[0]["via_node"] != via.String() {
		t.Fatalf("peers body %v", peers)
	}
}
