// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/gorilla/websocket"
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
	app := test.RandPeerIDFatal(t)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?peer_id=" + app.String()

	c1 := dialWS(t, url)
	waitUntil(t, "first client registered", func() bool { return s.hub.count() == 1 && fr.deliverer(app) != nil })
	c2 := dialWS(t, url)
	waitUntil(t, "second client registered", func() bool { return s.hub.count() == 2 })

	online := readUntil(t, c1, "peer_online")
	if online["peer_id"] != app.String() || online["via_node"] != s.h.ID().String() {
		t.Fatalf("peer_online body %v", online)
	}

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
	dialWS(t, base+test.RandPeerIDFatal(t).String())
	waitUntil(t, "first client registered", func() bool { return s.hub.count() == 1 })
	_, resp, err := websocket.DefaultDialer.Dial(base+test.RandPeerIDFatal(t).String(), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("second connection: err=%v status=%d, want 503", err, status)
	}
}
