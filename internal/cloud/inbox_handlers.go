// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
)

func (s *Service) handleInboxGet(w http.ResponseWriter, r *http.Request) {
	h, ok := s.requireIdentity(w, r, func(h identityHeaders) string {
		return fmt.Sprintf("pscloud-inbox|%s|%d", h.PeerID, h.TS)
	})
	if !ok {
		return
	}
	packets := s.inbox.Take(h.ID)
	out := make([]map[string]any, 0, len(packets))
	for _, p := range packets {
		out = append(out, map[string]any{
			"packet_id":      hex.EncodeToString(p.ID[:]),
			"sender_peer_id": p.SenderPeerID(),
			"payload":        base64.StdEncoding.EncodeToString(p.Payload),
			"timestamp":      p.Timestamp,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"packets": out})
}
