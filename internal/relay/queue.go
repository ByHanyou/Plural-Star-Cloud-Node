// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	QueueEntryTTL      = 7 * 24 * time.Hour
	QueueMaxSenders    = 500
	QueueMaxRecipients = 10000
	QueueMaxBytes      = 64 << 20
	QueueSweepInterval = 30 * time.Minute
)

type queuedPacket struct {
	pkt  *Packet
	at   time.Time
	size int64
}

type Queue struct {
	mu    sync.Mutex
	items map[peer.ID]map[string]queuedPacket
	bytes int64
}

func NewQueue(ctx context.Context) *Queue {
	q := &Queue{items: make(map[peer.ID]map[string]queuedPacket)}
	go q.sweepLoop(ctx)
	return q
}

func packetSize(p *Packet) int64 {
	return int64(len(p.ID) + len(p.SenderID) + len(p.RecipientID) + len(p.Payload) + 8)
}

func (q *Queue) Put(recipient peer.ID, p *Packet) bool {
	if p == nil {
		return false
	}
	sender := string(p.SenderID)
	size := packetSize(p)
	q.mu.Lock()
	defer q.mu.Unlock()
	bucket, ok := q.items[recipient]
	if !ok {
		if len(q.items) >= QueueMaxRecipients {
			return false
		}
		bucket = make(map[string]queuedPacket)
	}
	old, exists := bucket[sender]
	if !exists && len(bucket) >= QueueMaxSenders {
		return false
	}
	var freed int64
	if exists {
		freed = old.size
	}
	if q.bytes-freed+size > QueueMaxBytes {
		return false
	}
	if !ok {
		q.items[recipient] = bucket
	}
	q.bytes += size - freed
	bucket[sender] = queuedPacket{pkt: p, at: time.Now(), size: size}
	return true
}

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
		q.bytes -= entry.size
		if entry.at.Before(cutoff) {
			continue
		}
		out = append(out, entry.pkt)
	}
	return out
}

func (q *Queue) Len(recipient peer.ID) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items[recipient])
}

func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
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
				q.bytes -= entry.size
				delete(bucket, sender)
			}
		}
		if len(bucket) == 0 {
			delete(q.items, recipient)
		}
	}
}
