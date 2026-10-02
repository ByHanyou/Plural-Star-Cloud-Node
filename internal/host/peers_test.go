// SPDX-License-Identifier: AGPL-3.0-or-later

package host

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	testPeerA = "12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU"
	testPeerB = "12D3KooWL4A25M2sWt1HoFY3r8hWn2idbc4yVdsdBqpFitRTNyfz"
)

func seedMemory(t *testing.T, path string, list []rememberedPeer) {
	t.Helper()
	pm := &PeerMemory{path: path, peers: map[peer.ID]rememberedPeer{}, dirty: true}
	for _, rp := range list {
		id, err := peer.Decode(rp.ID)
		if err != nil {
			t.Fatalf("bad test peer id %q: %v", rp.ID, err)
		}
		pm.peers[id] = rp
	}
	pm.Save()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("save produced no file: %v", err)
	}
}

func TestPeerMemoryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "known_peers.json")
	now := time.Now().Unix()
	seedMemory(t, path, []rememberedPeer{
		{ID: testPeerA, Addrs: []string{"/ip4/10.0.0.5/tcp/4001"}, LastSeen: now},
	})

	pm := LoadPeerMemory(path)
	self, _ := peer.Decode(testPeerB)
	targets := pm.DialTargets(self)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].ID.String() != testPeerA {
		t.Fatalf("wrong peer id %s", targets[0].ID)
	}
	if len(targets[0].Addrs) != 1 || targets[0].Addrs[0].String() != "/ip4/10.0.0.5/tcp/4001" {
		t.Fatalf("addrs did not survive the round trip: %v", targets[0].Addrs)
	}
}

func TestPeerMemorySkipsSelfAndExpired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "known_peers.json")
	now := time.Now().Unix()
	seedMemory(t, path, []rememberedPeer{
		{ID: testPeerA, Addrs: []string{"/ip4/10.0.0.5/tcp/4001"}, LastSeen: now},
		{ID: testPeerB, Addrs: []string{"/ip4/10.0.0.6/tcp/4001"}, LastSeen: now},
	})

	pm := LoadPeerMemory(path)
	self, _ := peer.Decode(testPeerA)
	targets := pm.DialTargets(self)
	if len(targets) != 1 || targets[0].ID.String() != testPeerB {
		t.Fatalf("self should be excluded, got %v", targets)
	}

	// An entry older than the TTL never comes back from disk. Written by hand
	// so it is Load's cutoff being tested, not Save's trim.
	stale := time.Now().Add(-peerMemoryTTL - time.Hour).Unix()
	path2 := filepath.Join(t.TempDir(), "known_peers.json")
	raw := []byte(`[{"id":"` + testPeerA + `","addrs":["/ip4/10.0.0.5/tcp/4001"],"last_seen":` + strconv.FormatInt(stale, 10) + `}]`)
	if err := os.WriteFile(path2, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pm2 := LoadPeerMemory(path2)
	if got := pm2.DialTargets(self); len(got) != 0 {
		t.Fatalf("expired entry should be dropped, got %v", got)
	}
}

func TestPeerMemoryToleratesMissingAndCorruptFiles(t *testing.T) {
	dir := t.TempDir()
	if pm := LoadPeerMemory(filepath.Join(dir, "absent.json")); len(pm.peers) != 0 {
		t.Fatal("missing file should load empty")
	}
	bad := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if pm := LoadPeerMemory(bad); len(pm.peers) != 0 {
		t.Fatal("corrupt file should load empty, not error")
	}
}

func TestMergeTargetsDedupesWithBootstrapPriority(t *testing.T) {
	idA, _ := peer.Decode(testPeerA)
	idB, _ := peer.Decode(testPeerB)
	dns, _ := ma.NewMultiaddr("/dns4/example.dedyn.io/tcp/4001")
	nat, _ := ma.NewMultiaddr("/ip4/203.0.113.9/tcp/4001")
	other, _ := ma.NewMultiaddr("/ip4/198.51.100.2/tcp/4001")

	merged := MergeTargets(
		[]peer.AddrInfo{{ID: idA, Addrs: []ma.Multiaddr{dns}}},
		[]peer.AddrInfo{
			{ID: idA, Addrs: []ma.Multiaddr{nat}},
			{ID: idB, Addrs: []ma.Multiaddr{other}},
		},
	)
	if len(merged) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(merged))
	}
	if merged[0].ID != idA || merged[0].Addrs[0].String() != dns.String() {
		t.Fatalf("bootstrap entry should win the dedupe, got %v", merged[0])
	}
	if merged[1].ID != idB {
		t.Fatalf("remembered-only peer missing, got %v", merged[1])
	}
}
