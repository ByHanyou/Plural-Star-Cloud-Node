// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	return h
}

func connect(t *testing.T, ctx context.Context, a, b host.Host) {
	t.Helper()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

func TestTwoNodeRelayDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	defer h1.Close()
	h2 := newTestHost(t)
	defer h2.Close()
	connect(t, ctx, h1, h2)

	ps1, err := pubsub.NewGossipSub(ctx, h1)
	if err != nil {
		t.Fatalf("pubsub1: %v", err)
	}
	ps2, err := pubsub.NewGossipSub(ctx, h2)
	if err != nil {
		t.Fatalf("pubsub2: %v", err)
	}

	const prefix = "plural-star:test:"
	m1, err := NewManager(ctx, h1, ps1, prefix, config.DefaultLimits(), nil)
	if err != nil {
		t.Fatalf("manager1: %v", err)
	}
	m2, err := NewManager(ctx, h2, ps2, prefix, config.DefaultLimits(), nil)
	if err != nil {
		t.Fatalf("manager2: %v", err)
	}

	appPeer := test.RandPeerIDFatal(t)
	delivered := make(chan *Packet, 4)
	m2.AppConnected(appPeer, func(p *Packet) { delivered <- p })

	m1.Router().Upsert(appPeer, h2.ID(), time.Minute)

	id, _ := NewPacketID()
	pkt := &Packet{
		ID:          id,
		SenderID:    []byte("origin-app"),
		RecipientID: []byte(appPeer),
		Payload:     []byte("hello across the relay"),
		Timestamp:   time.Now().UnixMilli(),
	}

	if out, err := m1.Route(pkt); err != nil || out != OutcomeForwarded {
		t.Fatalf("route 1: outcome=%q err=%v", out, err)
	}
	if out, err := m1.Route(pkt); err != nil || out != OutcomeDuplicate {
		t.Fatalf("route 2: outcome=%q err=%v", out, err)
	}

	select {
	case got := <-delivered:
		if string(got.Payload) != "hello across the relay" {
			t.Fatalf("payload mismatch: %q", got.Payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("packet was not delivered to the local app")
	}

	select {
	case <-delivered:
		t.Fatal("duplicate packet was delivered twice (dedup failed)")
	case <-time.After(500 * time.Millisecond):
	}

	id2, _ := NewPacketID()
	local := &Packet{
		ID:          id2,
		SenderID:    []byte("origin-app"),
		RecipientID: []byte(appPeer),
		Payload:     []byte("local"),
		Timestamp:   time.Now().UnixMilli(),
	}
	if out, err := m2.Route(local); err != nil || out != OutcomeDelivered {
		t.Fatalf("local route: outcome=%q err=%v", out, err)
	}
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("local packet was not delivered")
	}

	stranger := test.RandPeerIDFatal(t)
	id3, _ := NewPacketID()
	held := &Packet{
		ID:          id3,
		SenderID:    []byte("origin-app"),
		RecipientID: []byte(stranger),
		Payload:     []byte("later"),
		Timestamp:   time.Now().UnixMilli(),
	}
	if out, err := m1.Route(held); err != nil || out != OutcomeHeld {
		t.Fatalf("held route: outcome=%q err=%v", out, err)
	}
	if got := m1.queue.Take(stranger); len(got) != 1 {
		t.Fatalf("held packet count = %d, want 1", len(got))
	}
}
