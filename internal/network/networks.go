// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const DefaultDirectoryURL = ""

type NetworkDiscovery struct {
	ctx   context.Context
	self  peer.ID
	store *Store
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

func NewNetworkDiscovery(ctx context.Context, ps *pubsub.PubSub, self peer.ID, store *Store) (*NetworkDiscovery, error) {
	topic, err := ps.Join(DiscoveryTopic)
	if err != nil {
		return nil, fmt.Errorf("join discovery topic: %w", err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("subscribe discovery topic: %w", err)
	}
	nd := &NetworkDiscovery{ctx: ctx, self: self, store: store, topic: topic, sub: sub}
	go nd.readLoop()
	return nd, nil
}

func (nd *NetworkDiscovery) Announce(card NetworkCard) error {
	if err := VerifyNetworkCard(&card); err != nil {
		return err
	}
	if err := nd.store.Put(card); err != nil {
		return err
	}
	return nd.publish(card)
}

func (nd *NetworkDiscovery) publish(card NetworkCard) error {
	b, err := json.Marshal(card)
	if err != nil {
		return err
	}
	return nd.topic.Publish(nd.ctx, b)
}

func (nd *NetworkDiscovery) readLoop() {
	for {
		msg, err := nd.sub.Next(nd.ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == nd.self {
			continue
		}
		var card NetworkCard
		if err := json.Unmarshal(msg.Data, &card); err != nil {
			continue
		}
		if err := VerifyNetworkCard(&card); err != nil {
			continue
		}

		existing, found, err := nd.store.Get(card.ID)
		if err != nil {
			continue
		}
		if found && card.CreatedAt <= existing.CreatedAt {
			continue
		}
		if err := nd.store.Put(card); err != nil {
			continue
		}
		_ = nd.publish(card)
	}
}

func FetchDirectory(ctx context.Context, url string, store *Store) (int, error) {
	if url == "" {
		return 0, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("directory returned status %d", resp.StatusCode)
	}

	var cards []NetworkCard
	if err := json.NewDecoder(resp.Body).Decode(&cards); err != nil {
		return 0, fmt.Errorf("decode directory: %w", err)
	}
	stored := 0
	for _, card := range cards {
		if err := VerifyNetworkCard(&card); err != nil {
			log.Printf("directory: skipping invalid card %q: %v", card.ID, err)
			continue
		}
		if err := store.Put(card); err == nil {
			stored++
		}
	}
	return stored, nil
}
