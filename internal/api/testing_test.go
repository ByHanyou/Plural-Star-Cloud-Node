// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

type fakePing struct{ n int }

func (f fakePing) Count() int             { return f.n }
func (f fakePing) Nodes() []ping.NodeInfo { return nil }

type fakeRelay struct {
	mu       sync.Mutex
	outcome  relay.Outcome
	err      error
	routed   []*relay.Packet
	router   *relay.Router
	delivers map[peer.ID]relay.DeliverFunc
	gone     []peer.ID
}

func newFakeRelay(ctx context.Context) *fakeRelay {
	return &fakeRelay{
		outcome:  relay.OutcomeDelivered,
		router:   relay.NewRouter(ctx, time.Hour),
		delivers: make(map[peer.ID]relay.DeliverFunc),
	}
}

func (f *fakeRelay) Route(p *relay.Packet) (relay.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routed = append(f.routed, p)
	return f.outcome, f.err
}

func (f *fakeRelay) Router() *relay.Router { return f.router }

func (f *fakeRelay) AppConnected(p peer.ID, deliver relay.DeliverFunc) {
	f.mu.Lock()
	f.delivers[p] = deliver
	f.mu.Unlock()
}

func (f *fakeRelay) AppDisconnected(p peer.ID) {
	f.mu.Lock()
	delete(f.delivers, p)
	f.gone = append(f.gone, p)
	f.mu.Unlock()
}

func (f *fakeRelay) deliverer(p peer.ID) relay.DeliverFunc {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.delivers[p]
}

func (f *fakeRelay) disconnected() []peer.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]peer.ID(nil), f.gone...)
}

func newTestServer(t *testing.T, cfg *config.Config) (*Server, *fakeRelay, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if cfg.Limits == (config.Limits{}) {
		cfg.Limits = config.DefaultLimits()
	}
	fr := newFakeRelay(ctx)
	srv, err := New(cfg, configPath, h, Deps{
		Hub:        NewHub(),
		Rendezvous: NewRendezvousStore(ctx, configPath, cfg.Limits),
		Ping:       fakePing{n: 3},
		Relay:      fr,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv, fr, dir
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
