// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/gossip"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type NetworkDiscovery struct {
	store *Store
	topic *gossip.Topic[NetworkCard]
}

func NewNetworkDiscovery(ctx context.Context, ps *pubsub.PubSub, self peer.ID, store *Store) (*NetworkDiscovery, error) {
	nd := &NetworkDiscovery{store: store}
	topic, err := gossip.Join(ctx, ps, self, DiscoveryTopic, nd.receive)
	if err != nil {
		return nil, err
	}
	nd.topic = topic
	return nd, nil
}

func (nd *NetworkDiscovery) publish(card NetworkCard) error {
	return nd.topic.Publish(card)
}

func (nd *NetworkDiscovery) receive(_ peer.ID, card NetworkCard) {
	if err := VerifyNetworkCard(&card); err != nil {
		slog.Debug("network: ignoring invalid card", "id", card.ID, "err", err)
		return
	}
	existing, found, err := nd.store.Get(card.ID)
	if err != nil {
		slog.Warn("network: card lookup failed", "id", card.ID, "err", err)
		return
	}
	if found && card.CreatedAt <= existing.CreatedAt {
		return
	}
	if found && card.CreatedBy != existing.CreatedBy {
		return
	}
	if err := nd.store.Put(card); err != nil {
		slog.Warn("network: storing card failed", "id", card.ID, "err", err)
		return
	}
	if err := nd.publish(card); err != nil {
		slog.Debug("network: republishing card failed", "id", card.ID, "err", err)
	}
}

func FetchDirectory(ctx context.Context, url string, store *Store, timeout time.Duration) (int, error) {
	if url == "" {
		return 0, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{Timeout: timeout}
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
			slog.Warn("directory: skipping invalid card", "id", card.ID, "err", err)
			continue
		}
		if err := store.Put(card); err != nil {
			slog.Warn("directory: storing card failed", "id", card.ID, "err", err)
			continue
		}
		stored++
	}
	return stored, nil
}
