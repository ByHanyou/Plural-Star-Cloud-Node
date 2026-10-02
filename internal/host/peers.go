// SPDX-License-Identifier: AGPL-3.0-or-later

package host

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// PeerMemory is a small on-disk record of node peers this node has actually
// held a libp2p connection to, kept so the reconnect loop has more to dial
// than the static bootstrap list. Without it the bootstrap node is a single
// point of failure twice over: a node that restarts while the bootstrap is
// down comes up with zero peers and stays stranded even though the rest of
// the mesh is alive, and the bootstrap node itself dials nobody after a
// reboot because its own list contains only its own address. With it, every
// node redials the mesh it last saw, so the network heals from whichever
// side comes back first and nobody has to restart anything.
//
// This is purely local dial-target state. Nothing here changes the wire
// protocol, adds a message, or shares new data with anyone.
const (
	peerMemoryMaxPeers     = 64
	peerMemoryMaxAddrs     = 8
	peerMemoryTTL          = 30 * 24 * time.Hour
	peerMemoryFilePerms    = 0o600
)

type rememberedPeer struct {
	ID       string   `json:"id"`
	Addrs    []string `json:"addrs"`
	LastSeen int64    `json:"last_seen"`
}

type PeerMemory struct {
	mu    sync.Mutex
	path  string
	peers map[peer.ID]rememberedPeer
	dirty bool
}

// LoadPeerMemory reads the remembered-peer file at path. A missing or
// unreadable file is a fresh start, never an error: this cache is an
// optimisation and must not be able to stop a node from booting.
func LoadPeerMemory(path string) *PeerMemory {
	pm := &PeerMemory{path: path, peers: map[peer.ID]rememberedPeer{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return pm
	}
	var list []rememberedPeer
	if err := json.Unmarshal(data, &list); err != nil {
		return pm
	}
	cutoff := time.Now().Add(-peerMemoryTTL).Unix()
	for _, rp := range list {
		id, dErr := peer.Decode(rp.ID)
		if dErr != nil || rp.LastSeen < cutoff || len(rp.Addrs) == 0 {
			continue
		}
		pm.peers[id] = rp
	}
	return pm
}

// Snapshot records every peer the host is connected to right now, with the
// addresses those connections actually used. Relay-circuit addresses are
// skipped: a path through another node is exactly what will not exist when
// that node is the one being redialled.
func (pm *PeerMemory) Snapshot(h host.Host) {
	now := time.Now().Unix()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, c := range h.Network().Conns() {
		id := c.RemotePeer()
		if id == h.ID() {
			continue
		}
		addr := c.RemoteMultiaddr()
		if addr == nil || strings.Contains(addr.String(), "p2p-circuit") {
			continue
		}
		s := addr.String()
		rp := pm.peers[id]
		rp.ID = id.String()
		rp.LastSeen = now
		merged := []string{s}
		for _, a := range rp.Addrs {
			if a != s && len(merged) < peerMemoryMaxAddrs {
				merged = append(merged, a)
			}
		}
		rp.Addrs = merged
		pm.peers[id] = rp
		pm.dirty = true
	}
}

// Save writes the memory to disk if anything changed since the last save,
// trimming expired entries and capping the file at the most recently seen
// peers. Failures are swallowed for the same reason load failures are.
func (pm *PeerMemory) Save() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.dirty {
		return
	}
	cutoff := time.Now().Add(-peerMemoryTTL).Unix()
	list := make([]rememberedPeer, 0, len(pm.peers))
	for id, rp := range pm.peers {
		if rp.LastSeen < cutoff {
			delete(pm.peers, id)
			continue
		}
		list = append(list, rp)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].LastSeen != list[j].LastSeen {
			return list[i].LastSeen > list[j].LastSeen
		}
		return list[i].ID < list[j].ID
	})
	if len(list) > peerMemoryMaxPeers {
		list = list[:peerMemoryMaxPeers]
	}
	out, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	if wErr := os.WriteFile(pm.path, out, peerMemoryFilePerms); wErr == nil {
		pm.dirty = false
	}
}

// DialTargets returns remembered peers as dialable AddrInfos, excluding self
// and anything expired. Order is most recently seen first so the cap in the
// caller lands on the freshest addresses.
func (pm *PeerMemory) DialTargets(self peer.ID) []peer.AddrInfo {
	cutoff := time.Now().Add(-peerMemoryTTL).Unix()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	list := make([]rememberedPeer, 0, len(pm.peers))
	for id, rp := range pm.peers {
		if id == self || rp.LastSeen < cutoff {
			continue
		}
		list = append(list, rp)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].LastSeen != list[j].LastSeen {
			return list[i].LastSeen > list[j].LastSeen
		}
		return list[i].ID < list[j].ID
	})
	out := make([]peer.AddrInfo, 0, len(list))
	for _, rp := range list {
		id, err := peer.Decode(rp.ID)
		if err != nil {
			continue
		}
		ai := peer.AddrInfo{ID: id}
		for _, s := range rp.Addrs {
			maddr, mErr := ma.NewMultiaddr(s)
			if mErr != nil {
				continue
			}
			ai.Addrs = append(ai.Addrs, maddr)
		}
		if len(ai.Addrs) > 0 {
			out = append(out, ai)
		}
	}
	return out
}

// MergeTargets folds remembered peers into a bootstrap-derived target list,
// deduplicating by peer ID. Bootstrap entries come first and win the dedupe:
// their addresses are operator-vouched DNS names, while remembered addresses
// may be stale NAT observations.
func MergeTargets(bootstrap []peer.AddrInfo, remembered []peer.AddrInfo) []peer.AddrInfo {
	seen := make(map[peer.ID]bool, len(bootstrap)+len(remembered))
	out := make([]peer.AddrInfo, 0, len(bootstrap)+len(remembered))
	for _, ai := range bootstrap {
		if seen[ai.ID] {
			continue
		}
		seen[ai.ID] = true
		out = append(out, ai)
	}
	for _, ai := range remembered {
		if seen[ai.ID] {
			continue
		}
		seen[ai.ID] = true
		out = append(out, ai)
	}
	return out
}
