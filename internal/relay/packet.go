// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"crypto/rand"
	"fmt"
	"time"
)

const (
	RelayProtocol = "/plural-star/relay/1.0.0"

	// Two refresh intervals: with the TTL only 15 s past the refresh, one delayed
	// gossip message expired the route and friends flapped offline/online.
	PresenceTTL = 90 * time.Second
	// MaxPresenceTTL caps the lifetime a remote node may request for a route.
	MaxPresenceTTL          = 10 * time.Minute
	PresenceRefreshInterval = 45 * time.Second
	DedupCacheTTL           = 10 * time.Second
	DedupCacheEvictInterval = 5 * time.Second
	RoutingTablePruneTicker = 30 * time.Second

	// MaxPacketBytes bounds one relay frame between nodes. /send accepts a 1 MiB
	// JSON body, so a legitimate packet is well under this; without it msgio
	// would allocate up to its 8 MiB default for any peer that asks.
	MaxPacketBytes = 2 << 20
)

type Packet struct {
	ID          [16]byte `msgpack:"id"`
	SenderID    []byte   `msgpack:"sender"`
	RecipientID []byte   `msgpack:"recipient"`
	Payload     []byte   `msgpack:"payload"`
	Timestamp   int64    `msgpack:"ts"`
}

func NewPacketID() ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, fmt.Errorf("generate packet id: %w", err)
	}
	return id, nil
}
