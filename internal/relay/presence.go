// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/gossip"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type presenceMsg struct {
	PeerID     string `json:"peer_id"`
	ViaNode    string `json:"via_node"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	Timestamp  int64  `json:"timestamp,omitempty"`
	Tombstone  bool   `json:"tombstone,omitempty"`
}

type PeerEvent func(peerID, viaNode peer.ID, online bool)

type Presence struct {
	self   peer.ID
	topic  *gossip.Topic[presenceMsg]
	router *Router
	ttl    time.Duration
	maxTTL time.Duration
	onPeer PeerEvent
}

func NewPresence(ctx context.Context, ps *pubsub.PubSub, self peer.ID, topicName string, router *Router, ttl, maxTTL time.Duration, onPeer PeerEvent) (*Presence, error) {
	p := &Presence{self: self, router: router, ttl: ttl, maxTTL: maxTTL, onPeer: onPeer}
	topic, err := gossip.Join(ctx, ps, self, topicName, p.receive)
	if err != nil {
		return nil, err
	}
	p.topic = topic
	return p, nil
}

func (p *Presence) Announce(appPeer peer.ID) error {
	return p.topic.Publish(presenceMsg{
		PeerID:     appPeer.String(),
		ViaNode:    p.self.String(),
		TTLSeconds: int(p.ttl.Seconds()),
		Timestamp:  time.Now().UnixMilli(),
	})
}

func (p *Presence) Tombstone(appPeer peer.ID) error {
	return p.topic.Publish(presenceMsg{
		PeerID:    appPeer.String(),
		ViaNode:   p.self.String(),
		Tombstone: true,
	})
}

func (p *Presence) receive(via peer.ID, m presenceMsg) {
	appPeer, err := peer.Decode(m.PeerID)
	if err != nil {
		return
	}
	if m.ViaNode != "" && m.ViaNode != via.String() {
		return
	}
	if m.Tombstone {
		if cur, ok := p.router.Lookup(appPeer); !ok || cur != via {
			return
		}
		p.router.Remove(appPeer)
		p.fire(appPeer, via, false)
		return
	}
	ttl := time.Duration(m.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = p.ttl
	}
	if ttl > p.maxTTL {
		ttl = p.maxTTL
	}
	p.router.Upsert(appPeer, via, ttl)
	p.fire(appPeer, via, true)
}

func (p *Presence) fire(appPeer, via peer.ID, online bool) {
	if p.onPeer != nil {
		p.onPeer(appPeer, via, online)
	}
}
