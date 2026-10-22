// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"crypto/rand"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/vmihailenco/msgpack/v5"
)

const RelayProtocol = "/plural-star/relay/1.0.0"

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

func (p *Packet) Marshal() ([]byte, error) {
	return msgpack.Marshal(p)
}

func UnmarshalPacket(b []byte) (*Packet, error) {
	var p Packet
	if err := msgpack.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Packet) SenderPeerID() string {
	sid, err := peer.IDFromBytes(p.SenderID)
	if err != nil {
		return ""
	}
	return sid.String()
}
