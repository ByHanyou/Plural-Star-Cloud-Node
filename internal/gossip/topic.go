// SPDX-License-Identifier: AGPL-3.0-or-later

package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const dropLogInterval = time.Hour

type Handler[T any] func(from peer.ID, m T)

type Topic[T any] struct {
	ctx    context.Context
	self   peer.ID
	name   string
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
	handle Handler[T]

	dropped atomic.Int64

	logMu     sync.Mutex
	lastLog   time.Time
	sinceLast int64
}

func Join[T any](ctx context.Context, ps *pubsub.PubSub, self peer.ID, name string, handle Handler[T]) (*Topic[T], error) {
	topic, err := ps.Join(name)
	if err != nil {
		return nil, fmt.Errorf("join topic %q: %w", name, err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("subscribe topic %q: %w", name, err)
	}
	t := &Topic[T]{ctx: ctx, self: self, name: name, topic: topic, sub: sub, handle: handle}
	go t.readLoop()
	return t, nil
}

func (t *Topic[T]) Name() string { return t.name }

func (t *Topic[T]) Publish(m T) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return t.topic.Publish(t.ctx, b)
}

func (t *Topic[T]) Dropped() int64 { return t.dropped.Load() }

func (t *Topic[T]) readLoop() {
	for {
		msg, err := t.sub.Next(t.ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == t.self {
			continue
		}
		from := msg.GetFrom()
		if from == "" {
			t.drop(fmt.Errorf("message without a signer"))
			continue
		}
		var m T
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			t.drop(err)
			continue
		}
		t.dispatch(from, m)
	}
}

func (t *Topic[T]) dispatch(from peer.ID, m T) {
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("gossip: handler panicked", "topic", t.name, "panic", r)
		}
	}()
	t.handle(from, m)
}

func (t *Topic[T]) drop(err error) {
	t.dropped.Add(1)
	t.logMu.Lock()
	defer t.logMu.Unlock()
	t.sinceLast++
	if !t.lastLog.IsZero() && time.Since(t.lastLog) < dropLogInterval {
		return
	}
	slog.Warn("gossip: dropped malformed messages", "topic", t.name, "count", t.sinceLast, "latest", err)
	t.lastLog = time.Now()
	t.sinceLast = 0
}
