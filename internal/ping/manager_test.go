// SPDX-License-Identifier: AGPL-3.0-or-later

package ping

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	return h
}

type pingEvent struct {
	peer      peer.ID
	rttMs     int64
	connected bool
}

func waitEvent(t *testing.T, events <-chan pingEvent) pingEvent {
	t.Helper()
	select {
	case e := <-events:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("no ping event")
		return pingEvent{}
	}
}

func TestManagerMeasuresAndForgetsPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h1 := newTestHost(t)
	defer h1.Close()
	h2 := newTestHost(t)
	defer h2.Close()

	events := make(chan pingEvent, 8)
	m := NewManager(ctx, h1, 10*time.Second, func(p peer.ID, rttMs int64, connected bool) {
		events <- pingEvent{peer: p, rttMs: rttMs, connected: connected}
	})
	NewManager(ctx, h2, 10*time.Second, nil)

	if m.Count() != 0 || len(m.Nodes()) != 0 {
		t.Fatalf("fresh manager: count=%d nodes=%d", m.Count(), len(m.Nodes()))
	}
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	e := waitEvent(t, events)
	if e.peer != h2.ID() || !e.connected || e.rttMs < 0 {
		t.Fatalf("first event %+v, want connected %s", e, h2.ID())
	}
	if m.Count() != 1 {
		t.Fatalf("count = %d, want 1", m.Count())
	}
	nodes := m.Nodes()
	if len(nodes) != 1 || nodes[0].PeerID != h2.ID().String() || nodes[0].Multiaddr == "" || nodes[0].RTTms < 0 {
		t.Fatalf("nodes = %+v", nodes)
	}

	if err := h1.Network().ClosePeer(h2.ID()); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	e = waitEvent(t, events)
	if e.peer != h2.ID() || e.connected {
		t.Fatalf("second event %+v, want disconnected", e)
	}
	if m.Count() != 0 || len(m.Nodes()) != 0 {
		t.Fatalf("after disconnect: count=%d nodes=%d", m.Count(), len(m.Nodes()))
	}
}
