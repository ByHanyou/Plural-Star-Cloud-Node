// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial %s: %v (status %d)", url, err, status)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func appIdentity(t *testing.T) (peer.ID, crypto.PrivKey) {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return id, key
}

func signChallenge(t *testing.T, c *websocket.Conn, key crypto.PrivKey) {
	t.Helper()
	challenge := readUntil(t, c, "auth_challenge")
	nonce, _ := challenge["nonce"].(string)
	if len(nonce) != 64 {
		t.Fatal("invalid auth challenge nonce")
	}
	sig, err := key.Sign([]byte(wsAuthDomain + nonce))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteJSON(wsAuthMessage{Type: "auth_response", Signature: base64.StdEncoding.EncodeToString(sig)}); err != nil {
		t.Fatal(err)
	}
}

func connectApp(t *testing.T, url string, key crypto.PrivKey) (*websocket.Conn, string) {
	t.Helper()
	c := dialWS(t, url)
	signChallenge(t, c, key)
	ready := readUntil(t, c, "auth_ready")
	session, _ := ready["session"].(string)
	if len(session) != 64 {
		t.Fatal("invalid auth session")
	}
	return c, session
}

func readUntil(t *testing.T, c *websocket.Conn, typ string) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("message is not JSON: %q", raw)
		}
		if m["type"] == typ {
			return m
		}
	}
}

func TestWebSocketFanOutToSiblingDevices(t *testing.T) {
	s, fr, _ := newTestServer(t, &config.Config{})
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()
	app, key := appIdentity(t)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?peer_id=" + app.String()

	c1, _ := connectApp(t, url, key)
	waitUntil(t, "first client registered", func() bool { return s.hub.count() == 1 && fr.deliverer(app) != nil })
	c2, _ := connectApp(t, url, key)
	waitUntil(t, "second client registered", func() bool { return s.hub.count() == 2 })

	id, _ := relay.NewPacketID()
	sender := test.RandPeerIDFatal(t)
	fr.deliverer(app)(&relay.Packet{ID: id, SenderID: []byte(sender), RecipientID: []byte(app), Payload: []byte("hello"), Timestamp: 42})
	for i, c := range []*websocket.Conn{c1, c2} {
		m := readUntil(t, c, "packet_received")
		if m["sender_peer_id"] != sender.String() || m["payload"] != base64.StdEncoding.EncodeToString([]byte("hello")) || m["timestamp"] != float64(42) {
			t.Fatalf("client %d packet body %v", i+1, m)
		}
	}

	_ = c1.Close()
	waitUntil(t, "first client gone", func() bool { return s.hub.count() == 1 })
	if len(fr.disconnected()) != 0 {
		t.Fatal("relay told the app was gone while a sibling socket is still open")
	}
	_ = c2.Close()
	waitUntil(t, "second client gone", func() bool { return len(fr.disconnected()) == 1 })
	if fr.disconnected()[0] != app || s.hub.count() != 0 {
		t.Fatalf("disconnect bookkeeping: gone=%v count=%d", fr.disconnected(), s.hub.count())
	}

	rec := httptest.NewRecorder()
	s.handleWS(rec, httptest.NewRequest(http.MethodGet, "/ws?peer_id=nope", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad peer_id: status %d", rec.Code)
	}
}

func TestWebSocketConnectionCap(t *testing.T) {
	s, _, _ := newTestServer(t, &config.Config{MaxAppConnections: 1})
	ts := httptest.NewServer(http.HandlerFunc(s.handleWS))
	defer ts.Close()
	base := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?peer_id="
	first, key := appIdentity(t)
	connectApp(t, base+first.String(), key)
	waitUntil(t, "first client registered", func() bool { return s.hub.count() == 1 })
	second, _ := appIdentity(t)
	_, resp, err := websocket.DefaultDialer.Dial(base+second.String(), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("second connection: err=%v status=%d, want 503", err, status)
	}
}

func TestWebSocketRequiresPeerKeyBeforeInboxReplay(t *testing.T) {
	s, fr, dir := newTestServer(t, &config.Config{})
	inboxConfig := config.DefaultCloud()
	inboxConfig.WatermarkPercent = 100
	service, err := cloud.New(t.Context(), inboxConfig, s.config().Limits, dir)
	if err != nil {
		t.Fatal(err)
	}
	s.cloud = service
	ts := httptest.NewServer(s.routes())
	defer ts.Close()

	id, key := appIdentity(t)
	sender, wrongKey := appIdentity(t)
	payload := []byte("offline packet")
	if !service.Inbox().Store(id, &relay.Packet{SenderID: []byte(sender), RecipientID: []byte(id), Payload: payload}) {
		t.Fatal("could not seed inbox")
	}
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?peer_id=" + id.String()
	c := dialWS(t, url)
	signChallenge(t, c, wrongKey)
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("socket authenticated with a different key")
	}
	if s.hub.count() != 0 || fr.deliverer(id) != nil {
		t.Fatal("rejected socket registered")
	}

	c = dialWS(t, url)
	signChallenge(t, c, key)
	var ready wsAuthMessage
	if err := c.ReadJSON(&ready); err != nil || ready.Type != "auth_ready" || ready.Session == "" {
		t.Fatalf("first message after authentication: %+v, %v", ready, err)
	}
	got := readUntil(t, c, "packet_received")
	if got["payload"] != base64.StdEncoding.EncodeToString(payload) {
		t.Fatalf("inbox replay payload: %v", got)
	}
}
