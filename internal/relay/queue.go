// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// QueueEntryTTL is how long an undeliverable packet is held before it is dropped.
	QueueEntryTTL = 7 * 24 * time.Hour
	// QueueMaxSenders caps how many distinct senders may hold a slot for one recipient.
	QueueMaxSenders = 500
	// QueueSweepInterval is how often expired entries are pruned.
	QueueSweepInterval = 30 * time.Minute
)

type queuedPacket struct {
	pkt *Packet
	at  time.Time
}

// Queue holds the NEWEST undeliverable packet per (recipient, sender) pair.
// A newer packet from the same sender replaces the older one — recipients get
// current state on reconnect, never a replay of a backlog.
//
// Payloads are sealed end-to-end by the apps, so the node stores ciphertext it
// cannot read.
type Queue struct {
	mu    sync.Mutex
	items map[peer.ID]map[string]queuedPacket
}

func NewQueue(ctx context.Context) *Queue {
	q := &Queue{items: make(map[peer.ID]map[string]queuedPacket)}
	go q.sweepLoop(ctx)
	return q
}

// Put stores p for recipient, replacing any earlier packet from the same sender.
func (q *Queue) Put(recipient peer.ID, p *Packet) {
	if p == nil {
		return
	}
	sender := string(p.SenderID)
	q.mu.Lock()
	defer q.mu.Unlock()
	bucket, ok := q.items[recipient]
	if !ok {
		bucket = make(map[string]queuedPacket)
		q.items[recipient] = bucket
	}
	if _, exists := bucket[sender]; !exists && len(bucket) >= QueueMaxSenders {
		return
	}
	bucket[sender] = queuedPacket{pkt: p, at: time.Now()}
}

// Take removes and returns everything queued for recipient.
func (q *Queue) Take(recipient peer.ID) []*Packet {
	q.mu.Lock()
	defer q.mu.Unlock()
	bucket, ok := q.items[recipient]
	if !ok {
		return nil
	}
	delete(q.items, recipient)
	out := make([]*Packet, 0, len(bucket))
	cutoff := time.Now().Add(-QueueEntryTTL)
	for _, entry := range bucket {
		if entry.at.Before(cutoff) {
			continue
		}
		out = append(out, entry.pkt)
	}
	return out
}

// Len reports how many packets are held for recipient.
func (q *Queue) Len(recipient peer.ID) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items[recipient])
}

func (q *Queue) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(QueueSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.sweep()
		}
	}
}

func (q *Queue) sweep() {
	cutoff := time.Now().Add(-QueueEntryTTL)
	q.mu.Lock()
	defer q.mu.Unlock()
	for recipient, bucket := range q.items {
		for sender, entry := range bucket {
			if entry.at.Before(cutoff) {
				delete(bucket, sender)
			}
		}
		if len(bucket) == 0 {
			delete(q.items, recipient)
		}
	}
}
