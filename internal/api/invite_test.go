// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"

	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"
)

func TestInviteAcceptSwapsConfigWithoutRacingReaders(t *testing.T) {
	existing := "/ip4/198.51.100.7/tcp/4001/p2p/" + test.RandPeerIDFatal(t).String()
	cfg := &config.Config{APIToken: "secret", NetworkMode: config.ModePublic, BootstrapPeers: []string{existing}}
	s, _, dir := newTestServer(t, cfg)
	pskPath := filepath.Join(dir, "network.psk")
	cfg.PSKPath = pskPath

	addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/p2p/" + test.RandPeerIDFatal(t).String())
	if err != nil {
		t.Fatal(err)
	}
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatal(err)
	}
	invite, err := network.EncodeInvite(network.Invite{Multiaddrs: []ma.Multiaddr{addr}, PSK: psk, Label: "friends"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec := httptest.NewRecorder()
				s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
				rec = httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/config", nil)
				r.Header.Set("Authorization", "Bearer secret")
				s.handleConfig(rec, r)
			}
		}()
	}

	body, _ := json.Marshal(map[string]string{"invite": invite})
	r := httptest.NewRequest(http.MethodPost, "/invite/accept", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleInviteAccept(rec, r)
	close(stop)
	wg.Wait()
	if rec.Code != http.StatusOK {
		t.Fatalf("accept: status %d body %s", rec.Code, rec.Body.String())
	}

	next := s.config()
	if next.NetworkMode != config.ModePrivate || next.NetworkID != "friends" || next.PSKPath != pskPath {
		t.Fatalf("config not updated: %+v", next)
	}
	if len(next.BootstrapPeers) != 2 || next.BootstrapPeers[0] != existing || next.BootstrapPeers[1] != addr.String() {
		t.Fatalf("bootstrap peers %v", next.BootstrapPeers)
	}
	if cfg.NetworkMode != config.ModePublic || len(cfg.BootstrapPeers) != 1 {
		t.Fatal("the previous config object was mutated in place")
	}
	saved, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatalf("config not saved: %v", err)
	}
	if !strings.Contains(string(saved), "network_mode: private") {
		t.Fatalf("saved config does not carry the new mode:\n%s", saved)
	}
	if got, err := os.ReadFile(pskPath); err != nil || len(got) != 32 {
		t.Fatalf("psk not written: %v (%d bytes)", err, len(got))
	}
	rec = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/invite/accept", strings.NewReader(`{"invite":"psnode://v1/zzz"}`))
	r.Header.Set("Content-Type", "application/json")
	s.authed(s.handleInviteAccept)(rec, r)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated accept: status %d", rec.Code)
	}
}
