// SPDX-License-Identifier: AGPL-3.0-or-later

package host

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const peerMemoryFilePerms = 0o600

type rememberedPeer struct {
	ID       string   `json:"id"`
	Addrs    []string `json:"addrs"`
	LastSeen int64    `json:"last_seen"`
}

type PeerMemory struct {
	maxPeers int
	maxAddrs int
	ttl      time.Duration
	mu       sync.Mutex
	path     string
	peers    map[peer.ID]rememberedPeer
	dirty    bool
}

func LoadPeerMemory(path string, maxPeers, maxAddrs int, ttl time.Duration) *PeerMemory {
	pm := &PeerMemory{path: path, peers: map[peer.ID]rememberedPeer{}, maxPeers: maxPeers, maxAddrs: maxAddrs, ttl: ttl}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("peers: reading remembered peers failed", "path", path, "err", err)
		}
		return pm
	}
	var list []rememberedPeer
	if err := json.Unmarshal(data, &list); err != nil {
		slog.Warn("peers: remembered peers unreadable", "path", path, "err", err)
		return pm
	}
	cutoff := time.Now().Add(-pm.ttl).Unix()
	for _, rp := range list {
		id, dErr := peer.Decode(rp.ID)
		if dErr != nil || rp.LastSeen < cutoff || len(rp.Addrs) == 0 {
			continue
		}
		pm.peers[id] = rp
	}
	return pm
}

func (pm *PeerMemory) Snapshot(h host.Host) {
	now := time.Now().Unix()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, c := range h.Network().Conns() {
		id := c.RemotePeer()
		if id == h.ID() {
			continue
		}
		if c.Stat().Direction != corenet.DirOutbound {
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
			if a != s && len(merged) < pm.maxAddrs {
				merged = append(merged, a)
			}
		}
		rp.Addrs = merged
		pm.peers[id] = rp
		pm.dirty = true
	}
}

func (pm *PeerMemory) Save() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.dirty {
		return
	}
	cutoff := time.Now().Add(-pm.ttl).Unix()
	list := make([]rememberedPeer, 0, len(pm.peers))
	for id, rp := range pm.peers {
		if rp.LastSeen < cutoff {
			delete(pm.peers, id)
			continue
		}
		list = append(list, rp)
	}
	sortByRecency(list)
	if len(list) > pm.maxPeers {
		list = list[:pm.maxPeers]
		kept := make(map[peer.ID]rememberedPeer, len(list))
		for _, rp := range list {
			if id, err := peer.Decode(rp.ID); err == nil {
				kept[id] = rp
			}
		}
		pm.peers = kept
	}
	out, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		slog.Warn("peers: encode failed", "path", pm.path, "err", err)
		return
	}
	tmp := pm.path + ".tmp"
	if wErr := os.WriteFile(tmp, out, peerMemoryFilePerms); wErr != nil {
		slog.Warn("peers: save failed", "path", pm.path, "err", wErr)
		return
	}
	if rErr := os.Rename(tmp, pm.path); rErr != nil {
		slog.Warn("peers: save failed", "path", pm.path, "err", rErr)
		return
	}
	pm.dirty = false
}

func (pm *PeerMemory) DialTargets(self peer.ID) []peer.AddrInfo {
	cutoff := time.Now().Add(-pm.ttl).Unix()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	list := make([]rememberedPeer, 0, len(pm.peers))
	for id, rp := range pm.peers {
		if id == self || rp.LastSeen < cutoff {
			continue
		}
		list = append(list, rp)
	}
	sortByRecency(list)
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

func sortByRecency(list []rememberedPeer) {
	sort.Slice(list, func(i, j int) bool {
		if list[i].LastSeen != list[j].LastSeen {
			return list[i].LastSeen > list[j].LastSeen
		}
		return list[i].ID < list[j].ID
	})
}

func MergeTargets(bootstrap []peer.AddrInfo, remembered []peer.AddrInfo) []peer.AddrInfo {
	seen := make(map[peer.ID]bool, len(bootstrap)+len(remembered))
	out := make([]peer.AddrInfo, 0, len(bootstrap)+len(remembered))
	for _, group := range [][]peer.AddrInfo{bootstrap, remembered} {
		for _, ai := range group {
			if seen[ai.ID] {
				continue
			}
			seen[ai.ID] = true
			out = append(out, ai)
		}
	}
	return out
}
