// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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
	ctx    context.Context
	self   peer.ID
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
	router *Router
	ttl    time.Duration
	onPeer PeerEvent
}

func NewPresence(ctx context.Context, ps *pubsub.PubSub, self peer.ID, topicName string, router *Router, ttl time.Duration, onPeer PeerEvent) (*Presence, error) {
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("join presence topic %q: %w", topicName, err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("subscribe presence topic: %w", err)
	}
	p := &Presence{
		ctx:    ctx,
		self:   self,
		topic:  topic,
		sub:    sub,
		router: router,
		ttl:    ttl,
		onPeer: onPeer,
	}
	go p.readLoop()
	return p, nil
}

func (p *Presence) Announce(appPeer peer.ID) error {
	m := presenceMsg{
		PeerID:     appPeer.String(),
		ViaNode:    p.self.String(),
		TTLSeconds: int(p.ttl.Seconds()),
		Timestamp:  time.Now().UnixMilli(),
	}
	return p.publish(m)
}

func (p *Presence) Tombstone(appPeer peer.ID) error {
	m := presenceMsg{
		PeerID:    appPeer.String(),
		ViaNode:   p.self.String(),
		Tombstone: true,
	}
	return p.publish(m)
}

func (p *Presence) publish(m presenceMsg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return p.topic.Publish(p.ctx, b)
}

func (p *Presence) readLoop() {
	for {
		msg, err := p.sub.Next(p.ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == p.self {
			continue
		}
		var m presenceMsg
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			continue
		}
		appPeer, err := peer.Decode(m.PeerID)
		if err != nil {
			continue
		}
		via, err := peer.Decode(m.ViaNode)
		if err != nil {
			continue
		}
		if m.Tombstone {
			p.router.Remove(appPeer)
			p.fire(appPeer, via, false)
			continue
		}
		ttl := time.Duration(m.TTLSeconds) * time.Second
		if ttl <= 0 {
			ttl = p.ttl
		}
		p.router.Upsert(appPeer, via, ttl)
		p.fire(appPeer, via, true)
	}
}

func (p *Presence) fire(appPeer, via peer.ID, online bool) {
	if p.onPeer != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("presence: onPeer callback panicked: %v", r)
				}
			}()
			p.onPeer(appPeer, via, online)
		}()
	}
}
