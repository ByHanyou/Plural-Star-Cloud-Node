// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func newTestInbox(t *testing.T, quotaMB int) *Inbox {
	t.Helper()
	return newInbox(t.TempDir(), config.CloudConfig{InboxTTLDays: 7, InboxQuotaMB: quotaMB})
}

func inboxPacket(t *testing.T, sender string, payload []byte) *relay.Packet {
	t.Helper()
	id, err := relay.NewPacketID()
	if err != nil {
		t.Fatal(err)
	}
	return &relay.Packet{ID: id, SenderID: []byte(sender), RecipientID: []byte("r"), Payload: payload, Timestamp: time.Now().UnixMilli()}
}

func storeSpaced(t *testing.T, ib *Inbox, rcpt peer.ID, p *relay.Packet) {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	if !ib.Store(rcpt, p) {
		t.Fatalf("store from %s refused", p.SenderID)
	}
}

func TestInboxStoreTakeAndSenderReplacement(t *testing.T) {
	ib := newTestInbox(t, 4)
	rcpt := test.RandPeerIDFatal(t)
	storeSpaced(t, ib, rcpt, inboxPacket(t, "alice", []byte("one")))
	storeSpaced(t, ib, rcpt, inboxPacket(t, "alice", []byte("two")))
	storeSpaced(t, ib, rcpt, inboxPacket(t, "bob", []byte("three")))
	if ib.count() != 2 {
		t.Fatalf("count = %d, want 2 (newest per sender)", ib.count())
	}
	got := ib.Take(rcpt)
	if len(got) != 2 || string(got[0].Payload) != "two" || string(got[1].Payload) != "three" {
		t.Fatalf("take returned %d packets in the wrong order or content", len(got))
	}
	if ib.count() != 0 || len(ib.Take(rcpt)) != 0 {
		t.Fatal("inbox not empty after take")
	}
	if _, err := os.Stat(ib.recipientDir(rcpt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("empty recipient directory not removed")
	}
}

func TestInboxQuotaEvictsOldest(t *testing.T) {
	ib := newTestInbox(t, 1)
	rcpt := test.RandPeerIDFatal(t)
	big := make([]byte, 400<<10)
	for _, s := range []string{"a", "b", "c"} {
		storeSpaced(t, ib, rcpt, inboxPacket(t, s, big))
	}
	got := ib.Take(rcpt)
	if len(got) != 2 || string(got[0].SenderID) != "b" || string(got[1].SenderID) != "c" {
		t.Fatalf("quota eviction kept the wrong packets: %d", len(got))
	}
	tooBig := inboxPacket(t, "d", make([]byte, (1<<20)+1))
	if ib.Store(rcpt, tooBig) {
		t.Fatal("a packet larger than the whole quota was stored")
	}
	ib.full = func() bool { return true }
	if ib.Store(rcpt, inboxPacket(t, "e", []byte("x"))) {
		t.Fatal("stored while the disk is at the watermark")
	}
}

func TestInboxReplayStopsOnFailure(t *testing.T) {
	ib := newTestInbox(t, 4)
	rcpt := test.RandPeerIDFatal(t)
	for _, s := range []string{"a", "b", "c"} {
		storeSpaced(t, ib, rcpt, inboxPacket(t, s, []byte(s)))
	}
	if n := ib.Replay(rcpt, func(*relay.Packet) bool { return false }); n != 0 || ib.count() != 3 {
		t.Fatalf("failed delivery removed packets: n=%d count=%d", n, ib.count())
	}
	calls := 0
	n := ib.Replay(rcpt, func(*relay.Packet) bool {
		calls++
		return calls == 1
	})
	if n != 1 || ib.count() != 2 {
		t.Fatalf("partial replay: n=%d count=%d", n, ib.count())
	}
	if got := ib.Take(rcpt); len(got) != 2 || string(got[0].SenderID) != "b" {
		t.Fatalf("remaining packets wrong: %d", len(got))
	}
}

func TestInboxSweepDropsExpired(t *testing.T) {
	ib := newTestInbox(t, 4)
	rcpt := test.RandPeerIDFatal(t)
	storeSpaced(t, ib, rcpt, inboxPacket(t, "fresh", []byte("fresh")))
	old := inboxPacket(t, "old", []byte("old"))
	raw, err := old.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
	name := strconv.FormatInt(at, 10) + "-" + hex.EncodeToString(old.ID[:]) + "-" + senderTag(old.SenderID) + ".pkt"
	if err := os.WriteFile(filepath.Join(ib.recipientDir(rcpt), name), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	ib.sweep()
	if ib.count() != 1 {
		t.Fatalf("count after sweep = %d, want 1", ib.count())
	}
	got := ib.Take(rcpt)
	if len(got) != 1 || string(got[0].Payload) != "fresh" {
		t.Fatalf("sweep kept the wrong packet: %d", len(got))
	}
}
