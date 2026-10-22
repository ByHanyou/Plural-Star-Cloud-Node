// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxFrontersLen = 400
	maxNameLen     = 64
	maxReaders     = 500
)

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max-1]) + "…"
}

type frontRequest struct {
	PeerID      string   `json:"peer_id"`
	EdPub       string   `json:"ed_pub"`
	Sig         string   `json:"sig"`
	TS          int64    `json:"ts"`
	Fronters    string   `json:"fronters"`
	StartTime   int64    `json:"start_time"`
	Name        string   `json:"name"`
	Primary     string   `json:"primary"`
	CoFront     string   `json:"co_front"`
	CoConscious string   `json:"co_conscious"`
	Readers     []string `json:"readers"`
}

func (f *frontRequest) verify() bool {
	signedRead := fmt.Sprintf("psgw-front|%s|%d|%s|%d|%s|%s|%s|%s|%s",
		f.PeerID, f.TS, f.Fronters, f.StartTime, f.Name,
		f.Primary, f.CoFront, f.CoConscious, strings.Join(f.Readers, ","))
	signedNew := fmt.Sprintf("psgw-front|%s|%d|%s|%d|%s", f.PeerID, f.TS, f.Fronters, f.StartTime, f.Name)
	signedOld := fmt.Sprintf("psgw-front|%s|%d|%s|%d", f.PeerID, f.TS, f.Fronters, f.StartTime)
	return verifySigned(f.PeerID, f.EdPub, f.Sig, signedRead) ||
		verifySigned(f.PeerID, f.EdPub, f.Sig, signedNew) ||
		verifySigned(f.PeerID, f.EdPub, f.Sig, signedOld)
}

func requirePathPeer(w http.ResponseWriter, r *http.Request) (string, bool) {
	target := r.PathValue("peer_id")
	if _, err := peer.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid peer_id")
		return target, false
	}
	return target, true
}

func (s *Service) handleFrontPut(w http.ResponseWriter, r *http.Request) {
	target, ok := requirePathPeer(w, r)
	if !ok {
		return
	}
	var req frontRequest
	if !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &req) {
		return
	}
	if req.PeerID != target {
		writeError(w, http.StatusBadRequest, "peer_id does not match path")
		return
	}
	if s.signatureRefused(w, req.TS, req.verify) {
		return
	}
	if s.watermarked(w) {
		return
	}
	if len(req.Readers) > maxReaders {
		req.Readers = req.Readers[:maxReaders]
	}
	entry := FrontEntry{
		Fronters:    truncateRunes(req.Fronters, maxFrontersLen),
		StartTime:   req.StartTime,
		Name:        truncateRunes(req.Name, maxNameLen),
		At:          time.Now().UnixMilli(),
		AuthoredAt:  req.TS,
		Primary:     truncateRunes(req.Primary, maxFrontersLen),
		CoFront:     truncateRunes(req.CoFront, maxFrontersLen),
		CoConscious: truncateRunes(req.CoConscious, maxFrontersLen),
		Readers:     req.Readers,
	}
	if err := s.fronts.Put(req.PeerID, entry); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Service) handleFrontGet(w http.ResponseWriter, r *http.Request) {
	target, ok := requirePathPeer(w, r)
	if !ok {
		return
	}
	h, ok := s.requireIdentity(w, r, func(h identityHeaders) string {
		return fmt.Sprintf("pscloud-front-get|%s|%d|%s", h.PeerID, h.TS, target)
	})
	if !ok {
		return
	}
	entry, found := s.fronts.Get(target)
	if !found || h.PeerID == target || !slices.Contains(entry.Readers, h.PeerID) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fronters":     entry.Fronters,
		"primary":      entry.Primary,
		"co_front":     entry.CoFront,
		"co_conscious": entry.CoConscious,
		"start_time":   entry.StartTime,
		"name":         entry.Name,
		"at":           entry.At,
		"authored_at":  entry.AuthoredAt,
	})
}
