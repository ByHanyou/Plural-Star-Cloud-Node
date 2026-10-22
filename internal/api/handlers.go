// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/version"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	cfg := s.config()
	networkID := cfg.NetworkID
	if networkID == "" && cfg.NetworkMode == config.ModePublic {
		networkID = "plural-star-global"
	}

	roles := cfg.Roles()
	pushPub := ""
	if s.cloud != nil {
		pushPub = s.cloud.PushBoxPublicKey()
	}
	out := map[string]any{
		"status":          "ok",
		"peer_id":         s.h.ID().String(),
		"network_mode":    cfg.NetworkMode,
		"network_id":      networkID,
		"connected_nodes": s.ping.Count(),
		"connected_apps":  s.hub.count(),
		"uptime_seconds":  int64(time.Since(s.startedAt).Seconds()),
		"roles":           roles,
		"agent":           version.AgentString(roles, pushPub),
		"pressure":        pressure(),
		"version":         version.Version,
	}
	if s.cloud != nil {
		out["cloud"] = s.cloud.HealthFields()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.ping.Nodes())
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	seen := make(map[string]struct{})
	out := make([]map[string]string, 0)

	self := s.h.ID().String()
	for _, p := range s.hub.appPeers() {
		id := p.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, map[string]string{
			"peer_id":  id,
			"via_node": self,
		})
	}

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
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
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
		PacketID  string `json:"packet_id,omitempty"`
	}
	if !decodeJSON(w, r, s.config().Limits.MaxSendBodyBytes, &req) {
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
	if req.SenderID == "" {
		writeError(w, http.StatusBadRequest, "sender_peer_id is required")
		return
	}
	sender, err := peer.Decode(req.SenderID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid sender_peer_id")
		return
	}
	pkt := &relay.Packet{
		ID:          id,
		SenderID:    []byte(sender),
		RecipientID: []byte(recipient),
		Payload:     payload,
		Timestamp:   time.Now().UnixMilli(),
	}

	outcome, err := s.relay.Route(pkt)
	if err != nil {
		slog.Warn("api: send failed", "from", sender, "to", recipient, "err", err)
		writeError(w, http.StatusInternalServerError, "could not route packet")
		return
	}
	status := http.StatusOK
	if outcome == relay.OutcomeForwarded || outcome == relay.OutcomeHeld {
		status = http.StatusAccepted
	}
	writeJSON(w, status, map[string]string{
		"status":    string(outcome),
		"packet_id": hex.EncodeToString(id[:]),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.operatorOnly(w, r, false) {
		return
	}
	redacted := *s.config()
	redact := func(v *string) {
		if *v != "" {
			*v = "***redacted***"
		}
	}
	redact(&redacted.APIToken)
	redact(&redacted.Cloud.RemoteBlobSecret)
	redact(&redacted.Cloud.BlobServeSecret)
	redact(&redacted.Cloud.S3AccessKey)
	redact(&redacted.Cloud.S3SecretKey)
	redact(&redacted.Cloud.PushForwardToken)
	writeJSON(w, http.StatusOK, redacted)
}

func packetEvent(p *relay.Packet) packetReceivedEvent {
	return packetReceivedEvent{
		Type:         "packet_received",
		SenderPeerID: p.SenderPeerID(),
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
	cfg := s.config()
	if !s.hub.reserve(cfg.MaxAppConnections) {
		writeError(w, http.StatusServiceUnavailable, "max_app_connections reached")
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.hub.release()
		slog.Warn("api: websocket upgrade failed", "peer", appPeer, "err", err)
		return
	}

	client := newWSClient(conn, appPeer, cfg.Limits)
	s.hub.add(client)

	s.hub.OnAppPeerEvent(appPeer, s.h.ID(), true)

	s.relay.AppConnected(appPeer, func(p *relay.Packet) {
		s.hub.sendToApp(appPeer, packetEvent(p))
	})

	if s.cloud != nil {
		replayed := s.cloud.Inbox().Replay(appPeer, func(p *relay.Packet) bool {
			b, err := json.Marshal(packetEvent(p))
			if err != nil {
				return true
			}
			_ = conn.SetWriteDeadline(time.Now().Add(cfg.Limits.WSWriteWait.Std()))
			return conn.WriteMessage(websocket.TextMessage, b) == nil
		})
		if replayed > 0 {
			slog.Info("cloud: replayed inbox packets", "count", replayed, "peer", appPeer)
		}
	}

	go client.writePump()
	client.readPump(func() {
		s.hub.remove(client)
		client.close()
		if !s.hub.hasClientFor(appPeer) {
			s.relay.AppDisconnected(appPeer)
			s.hub.OnAppPeerEvent(appPeer, s.h.ID(), false)
		}
	})
}

func (s *Server) handleInviteGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if !s.operatorOnly(w, r, false) {
		return
	}
	cfg := s.config()
	if cfg.NetworkMode != config.ModePrivate {
		writeError(w, http.StatusBadRequest, "invites are only available in private network mode")
		return
	}
	psk, err := network.LoadPSK(cfg.PSKPath)
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
		Label:      cfg.NetworkID,
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
	if !s.operatorOnly(w, r, true) {
		return
	}
	var req struct {
		Invite string `json:"invite"`
	}
	if !decodeJSON(w, r, s.config().Limits.MaxInviteBodyBytes, &req) {
		return
	}
	inv, err := network.DecodeInvite(req.Invite)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid invite: "+err.Error())
		return
	}
	s.inviteMu.Lock()
	defer s.inviteMu.Unlock()
	cur := s.config()
	next, err := network.ApplyInvite(*cur, inv, cur.PSKPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := config.Save(&next, s.configPath); err != nil {
		writeError(w, http.StatusInternalServerError, "could not persist config: "+err.Error())
		return
	}
	s.setConfig(&next)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "accepted",
		"restart_required": true,
	})
}

func (s *Server) handleRendezvousRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
		Record    string `json:"record"`
		TTLSecs   int    `json:"ttl_seconds"`
	}
	lim := s.config().Limits
	if !decodeJSON(w, r, lim.MaxRendezvousBodyBytes, &req) {
		return
	}
	if req.Namespace == "" || req.Record == "" {
		writeError(w, http.StatusBadRequest, "namespace and record required")
		return
	}
	if len(req.Namespace) > lim.RendezvousMaxNamespaceLen {
		writeError(w, http.StatusBadRequest, "namespace too long")
		return
	}
	if len(req.Record) > lim.RendezvousMaxRecordBytes {
		writeError(w, http.StatusBadRequest, "record too large")
		return
	}
	maxTTL := int(lim.RendezvousMaxTTL.Std().Seconds())
	ttl := req.TTLSecs
	if ttl <= 0 || ttl > maxTTL {
		ttl = maxTTL
	}
	if !s.rv.put(req.Namespace, req.Record, time.Duration(ttl)*time.Second) {
		writeError(w, http.StatusServiceUnavailable, "rendezvous full")
		return
	}
	if g := s.gossip; g != nil {
		if err := g.Announce(req.Namespace, req.Record, time.Now().Add(time.Duration(ttl)*time.Second)); err != nil {
			slog.Warn("rendezvous: gossip announce failed", "err", err)
		}
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleRendezvousLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		writeError(w, http.StatusBadRequest, "namespace required")
		return
	}
	rec, ok := s.rv.get(ns)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"record": rec})
}
