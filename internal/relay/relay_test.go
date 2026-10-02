package relay

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func TestPacketRoundTrip(t *testing.T) {
	id, err := NewPacketID()
	if err != nil {
		t.Fatalf("new packet id: %v", err)
	}
	orig := &Packet{
		ID:          id,
		SenderID:    []byte("sender"),
		RecipientID: []byte("recipient"),
		Payload:     []byte("opaque encrypted blob"),
		Timestamp:   time.Now().UnixMilli(),
	}
	b, err := orig.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalPacket(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != orig.ID || string(got.Payload) != string(orig.Payload) ||
		string(got.RecipientID) != string(orig.RecipientID) || got.Timestamp != orig.Timestamp {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, orig)
	}
}

func TestDedupSeenOrAdd(t *testing.T) {
	d := NewDedupCache(context.Background(), time.Second, time.Second)
	id, _ := NewPacketID()
	if d.SeenOrAdd(id) {
		t.Fatal("first insert should report not-seen (false)")
	}
	if !d.SeenOrAdd(id) {
		t.Fatal("second insert should report duplicate (true)")
	}
	other, _ := NewPacketID()
	if d.SeenOrAdd(other) {
		t.Fatal("distinct id should report not-seen (false)")
	}
}

func TestDedupEviction(t *testing.T) {
	d := NewDedupCache(context.Background(), 20*time.Millisecond, 10*time.Millisecond)
	id, _ := NewPacketID()
	d.SeenOrAdd(id)
	if d.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", d.Len())
	}
	time.Sleep(80 * time.Millisecond)
	if d.Len() != 0 {
		t.Fatalf("expected eviction to clear cache, got %d", d.Len())
	}
}

func TestRouterUpsertLookupExpire(t *testing.T) {
	r := NewRouter(context.Background(), time.Hour)
	target := test.RandPeerIDFatal(t)
	via := test.RandPeerIDFatal(t)

	if _, ok := r.Lookup(target); ok {
		t.Fatal("lookup on empty router should miss")
	}

	r.Upsert(target, via, 50*time.Millisecond)
	got, ok := r.Lookup(target)
	if !ok || got != via {
		t.Fatalf("lookup failed: ok=%v got=%s want=%s", ok, got, via)
	}
	if online := r.Online(); len(online) != 1 || online[0] != target {
		t.Fatalf("Online mismatch: %v", online)
	}

	time.Sleep(80 * time.Millisecond)
	if _, ok := r.Lookup(target); ok {
		t.Fatal("entry should have expired")
	}

	r.Upsert(target, via, time.Hour)
	r.Remove(target)
	if _, ok := r.Lookup(target); ok {
		t.Fatal("entry should be gone after Remove (tombstone)")
	}
}

var _ peer.ID

func TestQueueCaps(t *testing.T) {
	q := &Queue{items: make(map[peer.ID]map[string]queuedPacket)}
	big := &Packet{SenderID: []byte("s"), RecipientID: []byte("r"), Payload: make([]byte, QueueMaxBytes/2)}
	if !q.Put(peer.ID("a"), big) {
		t.Fatal("first packet should fit")
	}
	if !q.Put(peer.ID("a"), big) {
		t.Fatal("replacing the same sender's packet should fit")
	}
	if q.Put(peer.ID("b"), big) {
		t.Fatal("second recipient should exceed QueueMaxBytes")
	}
	small := &Packet{SenderID: []byte("s"), RecipientID: []byte("r"), Payload: []byte("x")}
	if !q.Put(peer.ID("b"), small) {
		t.Fatal("small packet should fit")
	}
	if got := q.Take(peer.ID("a")); len(got) != 1 {
		t.Fatalf("take returned %d packets, want 1", len(got))
	}
	if q.Bytes() != packetSize(small) {
		t.Fatalf("bytes after take = %d, want %d", q.Bytes(), packetSize(small))
	}
	q.sweep()
	if q.Bytes() != packetSize(small) {
		t.Fatalf("sweep changed bytes to %d", q.Bytes())
	}
}
