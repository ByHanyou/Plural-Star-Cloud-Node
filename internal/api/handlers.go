// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	networkID := s.cfg.NetworkID
	if networkID == "" && s.cfg.NetworkMode == config.ModePublic {
		networkID = "plural-star-global"
	}
	s.mu.RLock()
	apps := len(s.clients)
	s.mu.RUnlock()

	roles := s.cfg.Roles()
	pushPub := ""
	if s.cloud != nil {
		pushPub = s.cloud.PushBoxPublicKey()
	}
	out := map[string]any{
		"status":          "ok",
		"peer_id":         s.h.ID().String(),
		"network_mode":    s.cfg.NetworkMode,
		"network_id":      networkID,
		"connected_nodes": s.ping.Count(),
		"connected_apps":  apps,
		"uptime_seconds":  int64(time.Since(s.startedAt).Seconds()),
		"roles":           roles,
		"agent":           cloud.AgentString(roles, pushPub),
		"pressure":        cloud.Pressure(),
	}
	if s.cloud != nil {
		out["cloud"] = s.cloud.HealthFields()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.ping.Nodes())
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	seen := make(map[string]struct{})
	out := make([]map[string]string, 0)

	self := s.h.ID().String()
	s.mu.RLock()
	for c := range s.clients {
		id := c.appPeer.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, map[string]string{
			"peer_id":  id,
			"via_node": self,
		})
	}
	s.mu.RUnlock()

	for _, pr := range s.relay.Router().Snapshot() {
		id := pr.Peer.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, map[string]string{
			"peer_id":  id,
			"via_node": pr.Via.String(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNetworks(w http.ResponseWriter, r *http.Request) {
	if s.networks == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	cards, err := s.networks.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read networks: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cards)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		SenderID  string `json:"sender_peer_id,omitempty"`
		Recipient string `json:"recipient_peer_id"`
		Payload   string `json:"payload"`
		PacketID string `json:"packet_id,omitempty"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	recipient, err := peer.Decode(req.Recipient)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid recipient_peer_id")
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, "payload must be base64")
		return
	}

	var id [16]byte
	if req.PacketID != "" {
		raw, derr := hex.DecodeString(req.PacketID)
		if derr != nil || len(raw) != 16 {
			writeError(w, http.StatusBadRequest, "packet_id must be 32 hex characters (16 bytes)")
			return
		}
		copy(id[:], raw)
	} else {
		gid, gerr := relay.NewPacketID()
		if gerr != nil {
			writeError(w, http.StatusInternalServerError, "could not generate packet id")
			return
		}
		id = gid
	}
	var sender peer.ID
	if req.SenderID != "" {
		parsed, pErr := peer.Decode(req.SenderID)
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid sender_peer_id")
			return
		}
		sender = parsed
	} else {
		sender = s.currentAppPeer()
	}
	pkt := &relay.Packet{
		ID:          id,
		SenderID:    []byte(sender),
		RecipientID: []byte(recipient),
		Payload:     payload,
		Timestamp:   time.Now().UnixMilli(),
	}

	if rErr := s.relay.Route(pkt); rErr != nil {
		if !errors.Is(rErr, relay.ErrNoRoute) {
			log.Printf("api: send %s -> %s failed: %v", sender, recipient, rErr)
		}
		if sender != "" {
			msg := "Recipient not found in routing table"
			if !errors.Is(rErr, relay.ErrNoRoute) {
				msg = "Could not forward to recipient's node"
			}
			s.sendToApp(sender, errorEvent{
				Type:    "error",
				Code:    "SEND_FAILED",
				Message: msg,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "queued",
		"packet_id": hex.EncodeToString(id[:]),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	redacted := *s.cfg
	redacted.APIToken = "***redacted***"
	writeJSON(w, http.StatusOK, redacted)
}

func packetEvent(p *relay.Packet) packetReceivedEvent {
	sender := ""
	if sid, e := peer.IDFromBytes(p.SenderID); e == nil {
		sender = sid.String()
	}
	return packetReceivedEvent{
		Type:         "packet_received",
		SenderPeerID: sender,
		Payload:      base64.StdEncoding.EncodeToString(p.Payload),
		Timestamp:    p.Timestamp,
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	appPeer, err := peer.Decode(r.URL.Query().Get("peer_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing or invalid peer_id query parameter")
		return
	}
	s.mu.RLock()
	atCapacity := s.cfg.MaxAppConnections > 0 && len(s.clients) >= s.cfg.MaxAppConnections
	s.mu.RUnlock()
	if atCapacity {
		writeError(w, http.StatusServiceUnavailable, "max_app_connections reached")
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := newWSClient(conn, appPeer)
	s.addClient(client)

	s.broadcast(peerOnlineEvent{Type: "peer_online", PeerID: appPeer.String(), ViaNode: s.h.ID().String()})

	// Deliver to EVERY client connected under this peer ID, not just the socket
	// that happened to register last. Linked devices share one identity, so one
	// address legitimately has several live connections and all of them must
	// receive. This also survives the registering socket closing while siblings
	// remain, since the relay slot no longer captures a single client.
	s.relay.AppConnected(appPeer, func(p *relay.Packet) {
		s.sendToApp(appPeer, packetEvent(p))
	})

	// Store-and-forward replay. Runs after AppConnected so a packet arriving
	// mid-replay is delivered live rather than parked until the next connect,
	// and BEFORE the write pump starts so this is the socket's only writer.
	// Each packet is written directly with a deadline: the buffered send path
	// drops silently once its 64 slots fill, and Replay deletes on success, so
	// routing stored packets through it would destroy them. A failed write
	// stops the replay and whatever remains stays on disk.
	if s.cloud != nil {
		replayed := s.cloud.Inbox().Replay(appPeer, func(p *relay.Packet) bool {
			b, err := json.Marshal(packetEvent(p))
			if err != nil {
				return true
			}
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			return conn.WriteMessage(websocket.TextMessage, b) == nil
		})
		if replayed > 0 {
			log.Printf("cloud: replayed %d inbox packet(s) to %s", replayed, appPeer)
		}
	}

	go client.writePump()
	client.readPump(func() {
		s.removeClient(client)
		client.close()
		if !s.hasClientFor(appPeer) {
			s.relay.AppDisconnected(appPeer)
			s.broadcast(peerOfflineEvent{Type: "peer_offline", PeerID: appPeer.String()})
		}
	})
}

func (s *Server) handleInviteGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if s.cfg.NetworkMode != config.ModePrivate {
		writeError(w, http.StatusBadRequest, "invites are only available in private network mode")
		return
	}
	psk, err := network.LoadPSK(s.cfg.PSKPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load psk: "+err.Error())
		return
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{ID: s.h.ID(), Addrs: s.h.Addrs()})
	if err != nil || len(p2pAddrs) == 0 {
		writeError(w, http.StatusInternalServerError, "node has no advertisable addresses")
		return
	}
	invite, err := network.EncodeInvite(network.Invite{
		Multiaddrs: p2pAddrs,
		PSK:        psk,
		Label:      s.cfg.NetworkID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"invite": invite})
}

func (s *Server) handleInviteAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Invite string `json:"invite"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	inv, err := network.DecodeInvite(req.Invite)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid invite: "+err.Error())
		return
	}
	if err := network.ApplyInvite(s.cfg, inv, s.cfg.PSKPath); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := config.Save(s.cfg, s.configPath); err != nil {
		writeError(w, http.StatusInternalServerError, "could not persist config: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "accepted",
		"restart_required": true,
	})
}
