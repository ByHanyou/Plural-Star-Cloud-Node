// SPDX-License-Identifier: AGPL-3.0-or-later

package gossip

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type note struct {
	Text string `json:"text"`
}

type received struct {
	from peer.ID
	m    note
}

func newHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func newPubSub(t *testing.T, ctx context.Context, h host.Host) *pubsub.PubSub {
	t.Helper()
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatalf("pubsub: %v", err)
	}
	return ps
}

func waitFor(t *testing.T, ch <-chan received, want string) received {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case got := <-ch:
			if got.m.Text == want {
				return got
			}
		case <-deadline:
			t.Fatalf("message %q never arrived", want)
		}
	}
}

func TestTopicDeliversDecodedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h1 := newHost(t)
	h2 := newHost(t)
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	ps1 := newPubSub(t, ctx, h1)
	ps2 := newPubSub(t, ctx, h2)

	const name = "plural-star:test:notes"
	got1 := make(chan received, 8)
	t1, err := Join(ctx, ps1, h1.ID(), name, func(from peer.ID, m note) { got1 <- received{from, m} })
	if err != nil {
		t.Fatalf("join 1: %v", err)
	}
	got2 := make(chan received, 8)
	t2, err := Join(ctx, ps2, h2.ID(), name, func(from peer.ID, m note) { got2 <- received{from, m} })
	if err != nil {
		t.Fatalf("join 2: %v", err)
	}
	if t1.Name() != name || t2.Name() != name {
		t.Fatalf("topic name mismatch: %q %q", t1.Name(), t2.Name())
	}

	deadline := time.Now().Add(10 * time.Second)
	for len(ps1.ListPeers(name)) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		if err := t1.Publish(note{Text: "hello"}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		select {
		case r := <-got2:
			if r.from != h1.ID() || r.m.Text != "hello" {
				t.Fatalf("got from=%s text=%q", r.from, r.m.Text)
			}
			i = 5
		case <-time.After(time.Second):
		}
	}
	select {
	case r := <-got1:
		t.Fatalf("publisher received its own message: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if t2.Dropped() != 0 {
		t.Fatalf("dropped = %d, want 0", t2.Dropped())
	}
}

func TestTopicCountsMalformedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h1 := newHost(t)
	h2 := newHost(t)
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	ps1 := newPubSub(t, ctx, h1)
	ps2 := newPubSub(t, ctx, h2)

	const name = "plural-star:test:malformed"
	raw, err := ps1.Join(name)
	if err != nil {
		t.Fatalf("join raw: %v", err)
	}
	got := make(chan received, 8)
	t2, err := Join(ctx, ps2, h2.ID(), name, func(from peer.ID, m note) { got <- received{from, m} })
	if err != nil {
		t.Fatalf("join 2: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(ps1.ListPeers(name)) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		if err := raw.Publish(ctx, []byte("{not json")); err != nil {
			t.Fatalf("publish raw: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		if t2.Dropped() > 0 {
			break
		}
	}
	if t2.Dropped() == 0 {
		t.Fatal("malformed message was not counted as dropped")
	}
	if err := raw.Publish(ctx, []byte(`{"text":"ok"}`)); err != nil {
		t.Fatalf("publish good: %v", err)
	}
	r := waitFor(t, got, "ok")
	if r.from != h1.ID() {
		t.Fatalf("from = %s, want %s", r.from, h1.ID())
	}
}
