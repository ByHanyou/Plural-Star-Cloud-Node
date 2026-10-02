// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"crypto/rand"
	"fmt"
	"time"
)

const (
	RelayProtocol = "/plural-star/relay/1.0.0"

	PresenceTTL             = 60 * time.Second
	PresenceRefreshInterval = 45 * time.Second
	DedupCacheTTL           = 10 * time.Second
	DedupCacheEvictInterval = 5 * time.Second
	RoutingTablePruneTicker = 30 * time.Second
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
