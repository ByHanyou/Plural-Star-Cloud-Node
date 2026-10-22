// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/gossip"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type rendezvousMsg struct {
	Namespace string `json:"namespace"`
	Record    string `json:"record"`
	ExpiresAt int64  `json:"expires_at"`
}

type RecordEvent func(namespace, record string, ttl time.Duration)

type RendezvousGossip struct {
	maxNamespaceLen int
	maxRecordBytes  int
	maxTTL          time.Duration
	topic           *gossip.Topic[rendezvousMsg]
	onRecord        RecordEvent
}

func NewRendezvousGossip(ctx context.Context, ps *pubsub.PubSub, self peer.ID, topicName string, maxNamespaceLen, maxRecordBytes int, maxTTL time.Duration, onRecord RecordEvent) (*RendezvousGossip, error) {
	g := &RendezvousGossip{maxNamespaceLen: maxNamespaceLen, maxRecordBytes: maxRecordBytes, maxTTL: maxTTL, onRecord: onRecord}
	topic, err := gossip.Join(ctx, ps, self, topicName, g.receive)
	if err != nil {
		return nil, err
	}
	g.topic = topic
	return g, nil
}

func (g *RendezvousGossip) Announce(namespace, record string, expiresAt time.Time) error {
	return g.topic.Publish(rendezvousMsg{
		Namespace: namespace,
		Record:    record,
		ExpiresAt: expiresAt.UnixMilli(),
	})
}

func (g *RendezvousGossip) receive(_ peer.ID, m rendezvousMsg) {
	if m.Namespace == "" || len(m.Namespace) > g.maxNamespaceLen {
		return
	}
	if m.Record == "" || len(m.Record) > g.maxRecordBytes {
		return
	}
	ttl := time.Until(time.UnixMilli(m.ExpiresAt))
	if ttl <= 0 {
		return
	}
	if ttl > g.maxTTL {
		ttl = g.maxTTL
	}
	if g.onRecord != nil {
		g.onRecord(m.Namespace, m.Record, ttl)
	}
}
