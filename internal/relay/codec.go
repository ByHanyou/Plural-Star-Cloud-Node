// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import "github.com/vmihailenco/msgpack/v5"

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
